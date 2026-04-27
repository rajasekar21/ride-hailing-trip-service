const path = require("path");
const fs = require("fs");
const csv = require("csv-parser");
const { Sequelize, DataTypes } = require("sequelize");

const db = new Sequelize({
  dialect: "sqlite",
  storage: process.env.DB_PATH || "rides.db"
});

const Trip = db.define("Trip", {
  id: { type: DataTypes.INTEGER, primaryKey: true },
  rider_id: DataTypes.INTEGER,
  driver_id: DataTypes.INTEGER,
  pickup_location: DataTypes.STRING,
  drop_location: DataTypes.STRING,
  city: DataTypes.STRING,
  distance_km: DataTypes.FLOAT,
  surge_multiplier: DataTypes.FLOAT,
  base_fare: DataTypes.FLOAT,
  fare_amount: DataTypes.FLOAT,
  trip_status: DataTypes.STRING,
  payment_status: DataTypes.STRING,
  requested_at: DataTypes.STRING,
  accepted_at: DataTypes.STRING,
  completed_at: DataTypes.STRING,
  cancelled_at: DataTypes.STRING
});

async function seed() {
  await db.sync({ force: true });

  const results = [];
  const filePath = process.env.DATASET_FILE || path.join(__dirname, "trips.csv");

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (data) => {
      results.push({
        id: parseInt(data.trip_id, 10),
        rider_id: parseInt(data.rider_id, 10) || null,
        driver_id: data.driver_id ? parseInt(data.driver_id, 10) : null,
        pickup_location: data.pickup_location,
        drop_location: data.drop_location,
        city: data.city,
        distance_km: parseFloat(data.distance_km) || 0,
        surge_multiplier: parseFloat(data.surge_multiplier) || 1.0,
        base_fare: parseFloat(data.base_fare) || 50.0,
        fare_amount: parseFloat(data.fare_amount) || 0.0,
        trip_status: data.trip_status,
        payment_status: data.trip_status === "COMPLETED" ? "PAID" : "PENDING",
        requested_at: data.requested_at,
        accepted_at: data.accepted_at || null,
        completed_at: data.completed_at || null,
        cancelled_at: data.cancelled_at || null
      });
    })
    .on("end", async () => {
      await Trip.bulkCreate(results, { ignoreDuplicates: true });
      console.log(`✅ Seeded ${results.length} trips`);
    });
}

seed();
