const express = require("express");
const cors = require("cors");
const amqp = require("amqplib");
const axios = require("axios");
const { Sequelize, DataTypes } = require("sequelize");

const app = express();
app.use(cors());
app.use(express.json());

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

let tripsRequestedTotal = 0;
let tripsCompletedTotal = 0;
let eventPublishFailuresTotal = 0;
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
    eventPublishFailuresTotal += 1;
    console.error(JSON.stringify({ level: "error", event: "publish_failed", routingKey, error: err.message }));
  }
}

app.use((req, res, next) => {
  const requestId = req.get("X-Request-ID") || `req-${Date.now()}`;
  const traceId = req.get("X-Trace-ID") || requestId;
  req.requestId = requestId;
  req.traceId = traceId;
  console.log(JSON.stringify({ requestId, traceId, method: req.method, path: req.path, body: req.body }));
  next();
});

function calculateFare(distance, surge) {
  const ratePerKm = 10;
  const baseFare = 50;
  return Math.round((baseFare + distance * ratePerKm) * surge * 100) / 100;
}

app.post("/v1/trips", async (req, res) => {
  try {
    const { rider_id, pickup_location, drop_location, city, distance_km, surge_multiplier = 1.0, base_fare = 50.0 } = req.body;
    if (!rider_id || !pickup_location || !drop_location || !city || typeof distance_km !== "number") {
      return res.status(400).send({ error: "rider_id, pickup_location, drop_location, city, and distance_km are required" });
    }
    const trip = await Trip.create({
      rider_id,
      pickup_location,
      drop_location,
      city,
      distance_km,
      surge_multiplier,
      base_fare,
      trip_status: "REQUESTED",
      requested_at: new Date().toISOString()
    });
    tripsRequestedTotal += 1;
    res.status(201).send(trip);
  } catch (err) {
    res.status(500).send({ error: "Failed to create trip" });
  }
});

app.get("/v1/trips", async (req, res) => {
  const trips = await Trip.findAll();
  res.send(trips);
});

app.get("/v1/trips/:id", async (req, res) => {
  const trip = await Trip.findByPk(req.params.id);
  if (!trip) {
    return res.status(404).send({ error: "Trip not found" });
  }
  res.send(trip);
});

app.post("/v1/trips/:id/accept", async (req, res) => {
  try {
    const trip = await Trip.findByPk(req.params.id);
    if (!trip) {
      return res.status(404).send({ error: "Trip not found" });
    }
    if (trip.trip_status !== "REQUESTED") {
      return res.status(400).send({ error: "Trip must be in REQUESTED state to accept" });
    }

    const response = await axios.get(`${DRIVER_SERVICE_URL}/v1/drivers?active=true`, {
      headers: { "X-Request-ID": req.requestId, "X-Trace-ID": req.traceId }
    });
    const availableDrivers = response.data || [];
    if (!availableDrivers.length) {
      return res.status(503).send({ error: "No active drivers available" });
    }

    const driver = availableDrivers[0];
    trip.driver_id = driver.id;
    trip.trip_status = "ACCEPTED";
    trip.accepted_at = new Date().toISOString();
    await trip.save();

    res.send(trip);
  } catch (err) {
    res.status(502).send({ error: "Failed to assign driver", details: err.message });
  }
});

app.post("/v1/trips/:id/complete", async (req, res) => {
  let trip;
  try {
    trip = await Trip.findByPk(req.params.id);
    if (!trip) {
      return res.status(404).send({ error: "Trip not found" });
    }
    if (!["ACCEPTED", "ONGOING"].includes(trip.trip_status)) {
      return res.status(400).send({ error: "Trip must be ACCEPTED or ONGOING to complete" });
    }

    const fare = calculateFare(trip.distance_km || 0, trip.surge_multiplier || 1.0);
    trip.fare_amount = fare;
    trip.completed_at = new Date().toISOString();
    trip.trip_status = "COMPLETED";
    await trip.save();
    const idempotencyKey = `trip-${trip.id}`;
    const asyncMode = req.query.mode === "async";

    if (asyncMode) {
      trip.payment_status = "PROCESSING";
      await trip.save();
      await publishEvent("trip.completed", {
        event: "trip.completed",
        trace_id: req.traceId,
        request_id: req.requestId,
        trip_id: trip.id,
        rider_id: trip.rider_id,
        driver_id: trip.driver_id,
        amount: fare,
        idempotency_key: idempotencyKey,
        occurred_at: new Date().toISOString()
      });
      tripsCompletedTotal += 1;
      return res.status(202).send({ trip, payment: { status: "PROCESSING", mode: "async" } });
    }

    const paymentResponse = await axios.post(`${PAYMENT_SERVICE_URL}/v1/payments/charge`, {
      trip_id: trip.id,
      amount: fare,
      idempotency_key: idempotencyKey
    }, {
      headers: { "X-Request-ID": req.requestId, "X-Trace-ID": req.traceId }
    });

    trip.payment_status = paymentResponse.data.status || "PAID";
    await trip.save();

    await axios.post(`${NOTIFICATION_SERVICE_URL}/v1/notifications`, {
      trip_id: trip.id,
      rider_id: trip.rider_id,
      driver_id: trip.driver_id,
      amount: trip.fare_amount,
      status: trip.trip_status,
      timestamp: new Date().toISOString()
    }, {
      headers: { "X-Request-ID": req.requestId, "X-Trace-ID": req.traceId }
    }).catch((notificationErr) => {
      console.error("Notification failed", notificationErr.message);
    });

    tripsCompletedTotal += 1;
    res.send({ trip, payment: paymentResponse.data });
  } catch (err) {
    if (trip) {
      trip.payment_status = "FAILED";
      await trip.save();
    }
    if (err.response && err.response.data) {
      res.status(err.response.status).send(err.response.data);
    } else {
      res.status(502).send({ error: "Payment processing failed", details: err.message });
    }
  }
});

app.post("/v1/trips/:id/cancel", async (req, res) => {
  try {
    const trip = await Trip.findByPk(req.params.id);
    if (!trip) {
      return res.status(404).send({ error: "Trip not found" });
    }
    if (!["REQUESTED", "ACCEPTED", "ONGOING"].includes(trip.trip_status)) {
      return res.status(400).send({ error: "Only in-progress trips can be cancelled" });
    }
    const cancellationFee = ["ACCEPTED", "ONGOING"].includes(trip.trip_status) ? CANCELLATION_FEE : 0;
    trip.trip_status = "CANCELLED";
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

app.get("/rides", async (req, res) => {
  const trips = await Trip.findAll();
  res.send(trips);
});

app.post("/rides", async (req, res) => {
  const trip = await Trip.create({
    rider_id: req.body.rider_id || 1,
    pickup_location: req.body.pickup_location || "Unknown pickup",
    drop_location: req.body.drop_location || "Unknown drop",
    city: req.body.city || "Unknown",
    distance_km: req.body.distance_km || 5,
    surge_multiplier: req.body.surge_multiplier || 1.0,
    base_fare: req.body.base_fare || 50,
    trip_status: "REQUESTED",
    requested_at: new Date().toISOString()
  });
  res.status(201).send(trip);
});

app.get("/health", (req, res) => {
  res.send("OK");
});

app.patch("/v1/trips/:id/payment-status", async (req, res) => {
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

app.get("/metrics", async (req, res) => {
  const completedRatings = await Trip.count({ where: { trip_status: "COMPLETED" } });
  res.send({
    trips_requested_total: tripsRequestedTotal,
    trips_completed_total: tripsCompletedTotal,
    completed_trips_in_db: completedRatings,
    event_publish_failures_total: eventPublishFailuresTotal
  });
});

app.listen(3000, () => {
  console.log("Ride service running on port 3000");
});
