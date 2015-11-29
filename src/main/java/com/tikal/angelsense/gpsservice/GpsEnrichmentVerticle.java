package com.tikal.angelsense.gpsservice;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import com.cyngn.kafka.MessageConsumer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;

public class GpsEnrichmentVerticle extends AbstractVerticle {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsEnrichmentVerticle.class);
	

	private final SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmmss");
	
	private GpsPersistor gpsPersistor;

	

	@Override
	public void start() {
//		extractAngelIdAndSave("013949008057328",null);
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		vertx.deployVerticle(MessageConsumer.class.getName(),new DeploymentOptions().setConfig(config()),this::handleKafkaDeploy);
		gpsPersistor = new GpsPersistor(vertx,config());
		logger.info("Deployed GpsEnrichmentVerticle successfully");
	}

	private void enrichGps(final Message<String> message) {
		final String gpsPayload = message.body();
		logger.debug("I have received a message: {}", gpsPayload);
		final JsonObject gps = toJson(gpsPayload);
		extractAngelIdAndSave(gps.getString("imei"),gps);
		
	}

	public JsonObject toJson(final String gpsPayload) {
		final JsonObject gps = new JsonObject();
		final String[] csvValues = gpsPayload.split(",");
		gps.put("id", csvValues[csvValues.length-1]);
		gps.put("receptionTime", Long.valueOf(csvValues[csvValues.length-2]));
		gps.put("imei", csvValues[1]);
		gps.put("lat", Double.valueOf(csvValues[4]));
		gps.put("lon", Double.valueOf(csvValues[5]));
		gps.put("readingTime", Long.valueOf(csvValues[6]));
		return gps;
	}

	private void extractAngelIdAndSave(final String imei, final JsonObject gps) {
		final HttpClient managementHttpClient = vertx.createHttpClient(
				new HttpClientOptions().setDefaultHost(config().getString("management.http.server.address"))
						.setDefaultPort(config().getInteger("management.http.server.port")));
		managementHttpClient.get(
				"/api/v1/devices/"+imei+"/angels", 
				response->handleResponse(response,gps)).putHeader("content-type", "text/json").end();		
	}
	
	
	
	private void handleResponse(final HttpClientResponse response, final JsonObject gps) {
		if(response.statusCode() != 200){
			logger.error("Could not find angel: {}"+response.statusMessage());
			return;
		}
			
		response.bodyHandler(body -> {			
			if(body==null || body.toString().isEmpty())
				logger.trace("Could not find angel for gps: {}",gps);
			else{
				gps.put("angelId", Integer.valueOf(body.toString()));
				gpsPersistor.persistGps(gps);
			}
		});
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
