/**
 * Winston logger configuration for the NMOS registry.
 *
 * This module sets up a centralized logging instance with configurable log levels,
 * formatted output, and console transport. Log level is controlled via the LOG_LEVEL environment variable.
 */
import winston from "winston";

/**
 * Combined log format including timestamp, error stack traces, and JSON output.
 */
const logFormat = winston.format.combine(
  winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
  winston.format.errors({ stack: true }),
  winston.format.splat(),
  winston.format.json(),
);

/**
 * Main Winston logger instance.
 * Configured with environment-based log level and console transport.
 */
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || "info",
  format: logFormat,
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          let msg = `${timestamp} [${level}]: ${message}`;
          if (Object.keys(meta).length > 0) {
            msg += ` ${JSON.stringify(meta)}`;
          }
          return msg;
        }),
      ),
    }),
  ],
});

export default logger;
