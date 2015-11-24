package com.tikal.angelsense.gpsservice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public class GpsServiceVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsServiceVerticle.class);

	@Override
	public void start() {
		vertx.deployVerticle(new GpsEnrichmentVerticle(),new DeploymentOptions().setConfig(config()));
		vertx.deployVerticle(new GpsFinderServiceVerticle(),new DeploymentOptions().setConfig(config()));
		vertx.deployVerticle(new GpsPersistVerticle(),new DeploymentOptions().setConfig(config()));
		
		
		logger.info("Started listening to EventBus for GPS");
	}
	

}
