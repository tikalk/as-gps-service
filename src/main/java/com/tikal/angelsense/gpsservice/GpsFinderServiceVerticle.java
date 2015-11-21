package com.tikal.angelsense.gpsservice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public class GpsFinderServiceVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsFinderServiceVerticle.class);
	private RedisClient redis;
	private RedisOptions config;

	@Override
	public void start() {
		config = new RedisOptions().setHost(config().getString("redis-host"));
		redis = RedisClient.create(vertx, config);

		final Router router = Router.router(vertx);
		router.route(HttpMethod.GET, "/gps/angel/:angelId").handler(this::handleQuery);
		vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http-port"));
		
		logger.info("Started the HTTP server...");

	}

	private void handleQuery(final RoutingContext routingContext) {
		final String angelId = routingContext.request().getParam("angelId");
		if (angelId == null)
			routingContext.response().setStatusCode(400).setStatusMessage("angelId is missing").end();
		else {
			final String start = routingContext.request().params().get("start");
			final String stop = routingContext.request().params().get("stop");
			redis.zrange("gps.angel."+angelId, Long.valueOf(start), Long.valueOf(stop), ar -> handleRedisQuery(ar, routingContext));
		}
	}

	private void handleRedisQuery(final AsyncResult<JsonArray> ar, final RoutingContext routingContext) {
		if (ar.succeeded()) {
			routingContext.response().end(Buffer.factory.buffer(ar.result().toString()));
		} else {
			logger.error("Failed to run Redis query: ", ar.cause());
			routingContext.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
		}
	}

}
