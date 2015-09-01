package com.mapr.distiller.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan("com.mapr.distiller.server")
public class Application implements CommandLineRunner {

	@Autowired
	private Coordinator coordinator;

	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		// LOG.info("Main: Shutting down.");
		System.setProperty("app.workdir", "/tmp/distiller/");
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("Starting the server");
		String configLocation = "/opt/mapr/conf/distiller.conf";
		if (args.length == 0) {
			LOG.debug("Main: Using default configuration file location: " + configLocation);
		} else {
			configLocation = args[0];
			LOG.debug("Main: Using custom configuration file location: " + configLocation);
		}
		coordinator.init(configLocation);
		Thread thread = new Thread(coordinator, "Coordinator");
		thread.start();
	}
}
