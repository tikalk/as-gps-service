package com.tikal.angelsense.gpsservice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public class GpsPersistVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsPersistVerticle.class);
	private RedisClient redis;
	private RedisOptions config;

	@Override
	public void start() {
		vertx.deployVerticle(new GpsFinderServiceVerticle(),new DeploymentOptions().setConfig(config()));
		
		config = new RedisOptions().setHost(config().getString("redis-host"));
		vertx.eventBus().consumer("gps.all", this::persistGps);
		logger.info("Started listening to EventBus for GPS");
	}

	private void persistGps(final Message<JsonObject> m) {
		logger.debug("Got GPS message {}",m.body());
		if(redis==null)
			redis = RedisClient.create(vertx, config);
		final JsonObject gps = m.body();
		redis.zadd("gps.angel."+gps.getInteger("angelId"), gps.getLong("readingTime").doubleValue(), gps.toString(), ar->handleAddGps(gps,ar));
	}

	private void handleAddGps(final JsonObject gps, final AsyncResult<Long> ar) {
		if (ar.succeeded())
			logger.debug("Added GPS to Redis. GPS is {}",gps);
		else
			logger.error("Problem on adding GPS {}: ",gps,ar.cause());
	}

	

}
