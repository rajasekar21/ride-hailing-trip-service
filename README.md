# Trip Service

Handles trip lifecycle: request, accept, complete, and cancel.

## API
- `POST /v1/trips`
- `GET /v1/trips`
- `GET /v1/trips/:id`
- `POST /v1/trips/:id/accept`
- `POST /v1/trips/:id/complete`
- `POST /v1/trips/:id/cancel`
- `PATCH /v1/trips/:id/payment-status`
- `GET /metrics`
- `GET /health`

## Environment Variables
- `DB_PATH` (default: `rides.db`)
- `DRIVER_SERVICE_URL` (default: `http://driver:3000`)
- `PAYMENT_SERVICE_URL` (default: `http://payment:3000`)
- `NOTIFICATION_SERVICE_URL` (default: `http://notification:3000`)
- `RABBITMQ_URL` (default: `amqp://rabbitmq:5672`)
- `EVENTS_EXCHANGE` (default: `ride.events`)
- `CANCELLATION_FEE` (default: `30`)

## Run Locally
```bash
npm install
node app.js
```

## Docker
```bash
docker build -t ride-hailing-trip-service .
docker run -p 3000:3000 ride-hailing-trip-service
```
