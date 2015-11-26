package com.tikal.angelsense.gpsservice;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import com.cyngn.kafka.MessageConsumer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class GpsEnrichmentVerticle extends AbstractVerticle {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsEnrichmentVerticle.class);

	private final SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmmss");

	{
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	private final Map<String, Integer> imeiToAngelCache = new HashMap<>();

	@Override
	public void start() {
//		vertx.eventBus().consumer("gps.all", this::enrichGps);
		vertx.deployVerticle(MessageConsumer.class.getName(),new DeploymentOptions().setConfig(config()),this::handleKafkaDeploy);
		logger.info("Deployed GpsEnrichmentVerticle successfully");
	}

	private void enrichGps(final Message<String> message) {
		final String gpsPayload = message.body();
		logger.debug("I have received a message: {}", gpsPayload);
		final JsonObject gps = toJson(gpsPayload);
		final Integer angelId = getAngelId(gps.getString("emei"));
		gps.put("angelId", angelId);
		logger.debug("Reply with the following enrichment gps: {}", gps);
		vertx.eventBus().send("enriched.gps",gps);
	}

	public JsonObject toJson(final String gpsPayload) {
		final JsonObject gps = new JsonObject();
		gps.put("id", UUID.randomUUID().toString());
		final String[] csvValues = gpsPayload.split(",");
		gps.put("imei", csvValues[1]);
		gps.put("lat", Double.valueOf(csvValues[4]));
		gps.put("lon", Double.valueOf(csvValues[5]));
		gps.put("receptionTime", Long.valueOf(df.format(new Date())));
		gps.put("readingTime", Long.valueOf(csvValues[6]));
		return gps;
	}

	private Integer getAngelId(final String imei) {
		final Integer angelId = imeiToAngelCache.get(imei);
		if(angelId != null)
			return angelId;
		
		imeiToAngelCache.put(imei, 2);
		return 2;
		
	}
	
	private void handleKafkaDeploy(final AsyncResult<String> ar) {
		if (ar.succeeded()){
			logger.info("Connected to succfully to Kafka");
			vertx.eventBus().consumer(MessageConsumer.EVENTBUS_DEFAULT_ADDRESS, this::enrichGps);
		}
		else
			logger.error("Problem connect to Kafka: ",ar.cause());
	}
}
