const client = require("prom-client");

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const tripsRequestedTotal = new client.Counter({
  name: "trips_requested_total",
  help: "Total number of trips requested",
  registers: [register]
});

const tripsCompletedTotal = new client.Counter({
  name: "trips_completed_total",
  help: "Total number of trips completed",
  registers: [register]
});

const paymentsFailedTotal = new client.Counter({
  name: "payments_failed_total",
  help: "Total number of failed payment operations",
  registers: [register]
});

const avgDriverRating = new client.Gauge({
  name: "avg_driver_rating",
  help: "Rolling average of driver ratings",
  registers: [register]
});

module.exports = {
  client,
  register,
  tripsRequestedTotal,
  tripsCompletedTotal,
  paymentsFailedTotal,
  avgDriverRating
};
