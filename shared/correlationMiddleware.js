const { v4: uuidv4 } = require("uuid");

module.exports = function correlationMiddleware(req, res, next) {
  const incoming = req.get("x-correlation-id");
  const correlationId = incoming || uuidv4();
  req.correlationId = correlationId;
  res.set("x-correlation-id", correlationId);
  next();
};
