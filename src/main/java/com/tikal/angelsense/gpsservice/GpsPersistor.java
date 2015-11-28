package com.tikal.angelsense.gpsservice;

import com.cyngn.kafka.MessageProducer;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public class GpsPersistor  {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsPersistor.class);
	private RedisClient redis;
	private final RedisOptions config;
	private final Vertx vertx;

	public GpsPersistor(final Vertx vertx,final JsonObject appConfig) {
		this.vertx=vertx;
		vertx.deployVerticle(MessageProducer.class.getName(),new DeploymentOptions().setConfig(appConfig));
		config = new RedisOptions().setHost(appConfig.getString("redis-host"));
		logger.info("Started listening to EventBus for GPS");
	}
	
	
	
	public void persistGps(final JsonObject gps) {
		logger.debug("Got GPS message {}",gps);
		if(redis==null)
			redis = RedisClient.create(vertx, config);
		redis.zadd("gps.angel."+gps.getInteger("angelId"), gps.getLong("readingTime").doubleValue(), gps.toString(), ar->handleAddGps(gps.toString(),ar));
	}

	private void handleAddGps(final String gps, final AsyncResult<Long> ar) {
		if (ar.succeeded()){
			logger.debug("Added GPS to Redis. GPS is {}",gps);
			vertx.eventBus().send(MessageProducer.EVENTBUS_DEFAULT_ADDRESS, gps);
			vertx.eventBus().publish("gps-feed", gps);
		}
		else
			logger.error("Problem on adding GPS {}: ",gps,ar.cause());
	}
	
	

	

}
