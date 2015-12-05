package com.tikal.angelsense.gpsservice;

import java.util.Arrays;

import com.cyngn.kafka.MessageProducer;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class GpsMongoPersistor  {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsMongoPersistor.class);

	private final Vertx vertx;
	private final MongoClient mongoClient;
	
	private final String collectionName;	
	
	

	public GpsMongoPersistor(final Vertx vertx,final JsonObject appConfig) {
		this.vertx=vertx;
		collectionName = appConfig.getJsonObject("mongoConfig").getString("gps_col_name");
		mongoClient = MongoClient.createShared(vertx, appConfig.getJsonObject("mongoConfig"));
		if(appConfig.getJsonObject("mongoConfig").getBoolean("recreate"))
			recreateDB();
		
		vertx.deployVerticle(MessageProducer.class.getName(),new DeploymentOptions().setConfig(appConfig));
		logger.info("Started listening to EventBus for GPS");
	}



	private void recreateDB() {
		mongoClient.runCommand("dropDatabase", new JsonObject().put("dropDatabase", 1), this::handleCommand);
		
		final JsonArray indexes = new JsonArray(Arrays.asList(
						new JsonObject().put("key", new JsonObject().put("readingTime", -1)).put("name", "gps_readingTime_idx"),
						new JsonObject().put("key", new JsonObject().put("angelId", 1).put("readingTime", -1)).put("name", "gps_angelId_readingTime_idx")
				));
		final JsonObject createIndexesCommand = new JsonObject().put("createIndexes", collectionName).put("indexes", indexes);
		mongoClient.runCommand("createIndexes", createIndexesCommand, this::handleCommand);
	}
	
	
	
	private void handleCommand(final AsyncResult<JsonObject> res) {
		if (res.succeeded())
			logger.debug(res.result().toString());
		else
			logger.error("Failed t run Command", res.cause());
	}



	public void persistGps(final JsonObject gps) {
		logger.debug("Got GPS message {}",gps);
		final Integer angelId = gps.getInteger("angelId");
		mongoClient.insert(collectionName, gps, ar->handleAddGps(gps.toString(),angelId,ar));
	}

	private void handleAddGps(final String gps, final Integer angelId, final AsyncResult<String> ar) {
		if (ar.succeeded()){
			logger.debug("Added GPS to Mongo. GPS is {}",gps);
			vertx.eventBus().send(MessageProducer.EVENTBUS_DEFAULT_ADDRESS, gps);
			vertx.eventBus().publish("gps-feed-all", gps);
			vertx.eventBus().publish("gps-feed-"+angelId, gps);
		}
		else
			logger.error("Problem on adding GPS {}: ",gps,ar.cause());
	}
	
	

	

}
