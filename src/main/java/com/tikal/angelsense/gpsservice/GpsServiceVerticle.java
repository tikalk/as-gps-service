package com.tikal.angelsense.gpsservice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;

public class GpsServiceVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GpsServiceVerticle.class);

	@Override
	public void start() {
		vertx.deployVerticle(new GpsEnrichmentVerticle(),new DeploymentOptions().setConfig(config()));
		vertx.deployVerticle(new GpsFinderServiceVerticle(),new DeploymentOptions().setConfig(config()));
		
		
		logger.info("Started listening to EventBus for GPS");
	}
	

}
