const express = require("express");
const cors = require("cors");
const amqp = require("amqplib");
const axios = require("axios");
const jwt = require("jsonwebtoken");
const { Sequelize, DataTypes } = require("sequelize");
const { createServer } = require("http");
const { Server } = require("socket.io");
const logger = require("./shared/logger");
const correlationMiddleware = require("./shared/correlationMiddleware");
const {
  client,
  register,
  tripsRequestedTotal,
  tripsCompletedTotal
} = require("./shared/metrics");

const app = express();
const server = createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.use(cors());
app.use(express.json());
app.use(correlationMiddleware);

const eventPublishFailuresTotal = new client.Counter({
  name: 'ride_event_publish_failures_total',
  help: 'Total number of event publish failures',
  registers: [register]
});

const completedTripsInDb = new client.Gauge({
  name: 'ride_completed_trips_in_db',
  help: 'Number of completed trips in database',
  registers: [register]
});

const JWT_SECRET = process.env.JWT_SECRET || "your-secret-key-change-in-production";

// Middleware to verify JWT
const verifyToken = (req, res, next) => {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).send({ error: "Access token required" });
  }
  const token = authHeader.substring(7);
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    res.status(401).send({ error: "Invalid token" });
  }
};

const db = new Sequelize({
  dialect: "sqlite",
  storage: process.env.DB_PATH || "rides.db"
});

const Trip = db.define("Trip", {
  rider_id: DataTypes.INTEGER,
  driver_id: DataTypes.INTEGER,
  pickup_location: DataTypes.STRING,
  drop_location: DataTypes.STRING,
  city: DataTypes.STRING,
  distance_km: DataTypes.FLOAT,
  surge_multiplier: { type: DataTypes.FLOAT, defaultValue: 1.0 },
  base_fare: { type: DataTypes.FLOAT, defaultValue: 50.0 },
  fare_amount: { type: DataTypes.FLOAT, defaultValue: 0.0 },
  status: { type: DataTypes.STRING, defaultValue: "REQUESTED" },
  trip_status: { type: DataTypes.STRING, defaultValue: "REQUESTED" },
  payment_status: { type: DataTypes.STRING, defaultValue: "PENDING" },
  requested_at: DataTypes.STRING,
  accepted_at: DataTypes.STRING,
  completed_at: DataTypes.STRING,
  cancelled_at: DataTypes.STRING
});

db.sync();

const DRIVER_SERVICE_URL = process.env.DRIVER_SERVICE_URL || "http://driver:3000";
const PAYMENT_SERVICE_URL = process.env.PAYMENT_SERVICE_URL || "http://payment:3000";
const NOTIFICATION_SERVICE_URL = process.env.NOTIFICATION_SERVICE_URL || "http://notification:3000";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";
const EVENTS_EXCHANGE = process.env.EVENTS_EXCHANGE || "ride.events";
const CANCELLATION_FEE = Number(process.env.CANCELLATION_FEE || 30);
const ALLOWED_SURGE_MULTIPLIERS = new Set([1.0, 1.2, 1.5]);

let rabbitChannel;

async function getRabbitChannel() {
  if (rabbitChannel) return rabbitChannel;
  const connection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await connection.createChannel();
  await rabbitChannel.assertExchange(EVENTS_EXCHANGE, "topic", { durable: true });
  return rabbitChannel;
}

async function publishEvent(routingKey, payload) {
  try {
    const channel = await getRabbitChannel();
    channel.publish(EVENTS_EXCHANGE, routingKey, Buffer.from(JSON.stringify(payload)), { persistent: true });
  } catch (err) {
    eventPublishFailuresTotal.inc();
    logger.info({ event: "publish_failed", routingKey, error: err.message }, "event publish failed");
  }
}

app.use((req, res, next) => {
  const requestStart = Date.now();
  req.requestId = req.correlationId;
  req.traceId = req.correlationId;
  logger.info(
    {
      correlationId: req.correlationId,
      method: req.method,
      path: req.path
    },
    "request started"
  );
  res.on("finish", () => {
    logger.info(
      {
        correlationId: req.correlationId,
        method: req.method,
        path: req.path,
        statusCode: res.statusCode,
        durationMs: Date.now() - requestStart
      },
      "request completed"
    );
  });
  next();
});

function calculateFare(distance, surge, baseFare) {
  const ratePerKm = 12;
  return Math.round((baseFare + distance * ratePerKm * surge) * 100) / 100;
}

function getTripStatus(trip) {
  return trip.status || trip.trip_status || "REQUESTED";
}

function setTripStatus(trip, nextStatus) {
  trip.status = nextStatus;
  trip.trip_status = nextStatus;
}

function isValidTransition(from, to) {
  const validTransitions = {
    REQUESTED: new Set(["ACCEPTED", "CANCELLED"]),
    ACCEPTED: new Set(["ONGOING", "CANCELLED"]),
    ONGOING: new Set(["COMPLETED", "CANCELLED"]),
    COMPLETED: new Set([]),
    CANCELLED: new Set([]),
    PAYMENT_FAILED: new Set([])
  };
  return validTransitions[from]?.has(to) || false;
}

function ensureTransitionOrThrow(res, trip, to) {
  const from = getTripStatus(trip);
  if (!isValidTransition(from, to)) {
    res.status(400).send({ error: `Invalid state transition from ${from} to ${to}` });
    return false;
  }
  return true;
}

const v1Router = express.Router();

v1Router.post("/trips", verifyToken, async (req, res) => {
  try {
    const { rider_id, pickup_location, drop_location, city, distance_km } = req.body;
    if (!rider_id || !pickup_location || !drop_location || !city || typeof distance_km !== "number") {
      return res.status(400).send({ error: "rider_id, pickup_location, drop_location, city, and distance_km are required" });
    }
    const surgeOptions = [1.0, 1.2, 1.5];
    const randomSurge = surgeOptions[Math.floor(Math.random() * surgeOptions.length)];
    const baseFare = 20;
    const trip = await Trip.create({
      rider_id,
      pickup_location,
      drop_location,
      city,
      distance_km,
      surge_multiplier: randomSurge,
      base_fare: baseFare,
      status: "REQUESTED",
      trip_status: "REQUESTED",
      requested_at: new Date().toISOString()
    });
    tripsRequestedTotal.inc();
    res.status(201).send(trip);
  } catch (err) {
    res.status(500).send({ error: "Failed to create trip" });
  }
});

v1Router.get("/trips", async (req, res) => {
  const trips = await Trip.findAll();
  res.send(trips);
});

v1Router.get("/trips/:id", async (req, res) => {
  const trip = await Trip.findByPk(req.params.id);
  if (!trip) {
    return res.status(404).send({ error: "Trip not found" });
  }
  res.send(trip);
});

v1Router.post("/trips/:id/accept", async (req, res) => {
  try {
    const { driver_id } = req.body;
    if (!driver_id) {
      return res.status(400).send({ error: "driver_id is required" });
    }
    const trip = await Trip.findByPk(req.params.id);
    if (!trip) {
      return res.status(404).send({ error: "Trip not found" });
    }
    if (!ensureTransitionOrThrow(res, trip, "ACCEPTED")) {
      return;
    }

    const response = await axios.get(`${DRIVER_SERVICE_URL}/v1/drivers/${driver_id}`, {
      headers: {
        "X-Request-ID": req.requestId,
        "X-Trace-ID": req.traceId,
        "x-correlation-id": req.correlationId
      }
    });
    const driver = response.data;
    if (!driver) {
      return res.status(404).send({ error: "Driver not found" });
    }
    if (driver.is_active === false) {
      return res.status(422).send({ error: "Driver is not active" });
    }

    trip.driver_id = driver.id;
    setTripStatus(trip, "ACCEPTED");
    trip.accepted_at = new Date().toISOString();
    await trip.save();

    res.send(trip);
  } catch (err) {
    res.status(502).send({ error: "Failed to assign driver", details: err.message });
  }
});

v1Router.post("/trips/:id/complete", async (req, res) => {
  let trip;
  try {
    trip = await Trip.findByPk(req.params.id);
    if (!trip) {
      return res.status(404).send({ error: "Trip not found" });
    }
    const currentStatus = getTripStatus(trip);
    if (currentStatus === "ACCEPTED") {
      if (!ensureTransitionOrThrow(res, trip, "ONGOING")) {
        return;
      }
      setTripStatus(trip, "ONGOING");
      await trip.save();
    } else if (currentStatus !== "ONGOING") {
      return res.status(400).send({ error: `Invalid state transition from ${currentStatus} to COMPLETED` });
    }

    if (!ensureTransitionOrThrow(res, trip, "COMPLETED")) {
      return;
    }

    const fare = calculateFare(trip.distance_km || 0, trip.surge_multiplier || 1.0, 20);
    trip.fare_amount = fare;
    trip.completed_at = new Date().toISOString();
    trip.base_fare = 20;
    setTripStatus(trip, "COMPLETED");
    await trip.save();

    const paymentResponse = await axios.post(`${PAYMENT_SERVICE_URL}/v1/payments/charge`, {
      trip_id: trip.id,
      amount: fare,
      rider_id: trip.rider_id,
      idempotency_key: String(trip.id)
    }, {
      headers: {
        "X-Request-ID": req.requestId,
        "X-Trace-ID": req.traceId,
        "Idempotency-Key": String(trip.id),
        "x-correlation-id": req.correlationId
      }
    });

    trip.payment_status = paymentResponse.data.status || "COMPLETED";
    await trip.save();

    await axios.post(`${NOTIFICATION_SERVICE_URL}/v1/notifications`, {
      trip_id: trip.id,
      rider_id: trip.rider_id,
      driver_id: trip.driver_id,
      amount: trip.fare_amount,
      status: trip.trip_status,
      timestamp: new Date().toISOString()
    }, {
      headers: {
        "X-Request-ID": req.requestId,
        "X-Trace-ID": req.traceId,
        "x-correlation-id": req.correlationId
      }
    }).catch((notificationErr) => {
      logger.info(
        {
          correlationId: req.correlationId,
          method: req.method,
          path: req.path,
          error: notificationErr.message
        },
        "notification call failed"
      );
    });

    tripsCompletedTotal.inc();
    res.send({ trip, payment: paymentResponse.data });
  } catch (err) {
    if (trip) {
      setTripStatus(trip, "PAYMENT_FAILED");
      trip.payment_status = "FAILED";
      await trip.save();
    }
    res.status(402).send({ error: "Payment failed", trip_id: Number(req.params.id) });
  }
});

v1Router.post("/trips/:id/cancel", async (req, res) => {
  try {
    const trip = await Trip.findByPk(req.params.id);
    if (!trip) {
      return res.status(404).send({ error: "Trip not found" });
    }
    const currentStatus = getTripStatus(trip);
    if (!["REQUESTED", "ACCEPTED", "ONGOING"].includes(currentStatus)) {
      return res.status(400).send({ error: `Invalid state transition from ${currentStatus} to CANCELLED` });
    }
    if (!ensureTransitionOrThrow(res, trip, "CANCELLED")) {
      return;
    }
    const cancellationFee = ["ACCEPTED", "ONGOING"].includes(currentStatus) ? CANCELLATION_FEE : 0;
    if (cancellationFee > 0) {
      await axios.post(`${PAYMENT_SERVICE_URL}/v1/payments/charge`, {
        trip_id: trip.id,
        amount: cancellationFee,
        rider_id: trip.rider_id,
        idempotency_key: `cancel-${trip.id}`
      }, {
        headers: {
          "X-Request-ID": req.requestId,
          "X-Trace-ID": req.traceId,
          "Idempotency-Key": `cancel-${trip.id}`,
          "x-correlation-id": req.correlationId
        }
      });
    }
    setTripStatus(trip, "CANCELLED");
    trip.cancelled_at = new Date().toISOString();
    await trip.save();
    await publishEvent("trip.cancelled", {
      event: "trip.cancelled",
      trace_id: req.traceId,
      request_id: req.requestId,
      trip_id: trip.id,
      rider_id: trip.rider_id,
      driver_id: trip.driver_id,
      cancellation_fee: cancellationFee,
      occurred_at: trip.cancelled_at
    });

    res.send({ trip, cancellation_fee: cancellationFee });
  } catch (err) {
    res.status(500).send({ error: "Failed to cancel trip", details: err.message });
  }
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "trip" });
});

v1Router.patch("/trips/:id/payment-status", async (req, res) => {
  const { status } = req.body;
  if (!status) {
    return res.status(400).send({ error: "status is required" });
  }
  const trip = await Trip.findByPk(req.params.id);
  if (!trip) {
    return res.status(404).send({ error: "Trip not found" });
  }
  trip.payment_status = status;
  await trip.save();
  res.send(trip);
});

app.use("/v1", v1Router);

app.get("/metrics", async (req, res) => {
  const completedRatings = await Trip.count({ where: { trip_status: "COMPLETED" } });
  completedTripsInDb.set(completedRatings);
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// WebSocket for live tracking
io.on('connection', (socket) => {
  logger.info({ socketId: socket.id }, "socket client connected");

  socket.on('join-trip', (tripId) => {
    socket.join(`trip-${tripId}`);
    logger.info({ socketId: socket.id, tripId }, "socket joined trip room");
  });

  socket.on('update-location', (data) => {
    // Broadcast location update to clients tracking this trip
    io.to(`trip-${data.tripId}`).emit('location-update', data);
  });

  socket.on('disconnect', () => {
    logger.info({ socketId: socket.id }, "socket client disconnected");
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  logger.info({ service: "ride", port: PORT }, "service started");
});
