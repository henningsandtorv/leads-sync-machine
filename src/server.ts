import "dotenv/config";

import Fastify from "fastify";
import rateLimit from "@fastify/rate-limit";
import importRoutes from "./routes/import";
import ingestRoutes from "./routes/ingest";
import cronRoutes from "./routes/cron";

const PORT = Number(process.env.PORT || 3000);

async function buildServer() {
  const app = Fastify({
    logger: true,
    bodyLimit: 2 * 1024 * 1024, // 2MB
  });

  await app.register(rateLimit, {
    max: 120,
    timeWindow: "1 minute",
  });

  await app.register(importRoutes, { prefix: "/import" });
  await app.register(ingestRoutes, { prefix: "/ingest" });
  await app.register(cronRoutes, { prefix: "/cron" });

  app.setErrorHandler((err, req, reply) => {
    req.log.error({ err }, "Request failed");
    const status = err.statusCode ?? 500;
    reply.status(status).send({
      error: err.name || "Error",
      message: err.message,
    });
  });

  app.get("/health", async () => ({ status: "ok" }));

  return app;
}

async function start() {
  const app = await buildServer();
  try {
    await app.listen({ port: PORT, host: "0.0.0.0" });
    app.log.info(`Server listening on port ${PORT}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();

export type App = Awaited<ReturnType<typeof buildServer>>;
