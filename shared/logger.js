const pino = require("pino");

const isDevelopment = process.env.NODE_ENV !== "production";

const transport = isDevelopment
  ? {
      target: "pino-pretty",
      options: { colorize: true, translateTime: "SYS:standard" }
    }
  : undefined;

module.exports = pino({
  level: process.env.LOG_LEVEL || "info",
  ...(transport ? { transport } : {})
});
