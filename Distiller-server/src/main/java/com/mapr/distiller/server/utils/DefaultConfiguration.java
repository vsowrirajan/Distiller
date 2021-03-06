package com.mapr.distiller.server.utils;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConfiguration {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(DefaultConfiguration.class);
	
	public static Properties getConfiguration(Properties otherProperties, boolean applyOtherPropertiesFirst) {
		if(applyOtherPropertiesFirst) {
			return setDefaultProperties(otherProperties);
		}
		Properties mergedProperties = new Properties();
		mergedProperties = setDefaultProperties(mergedProperties);
		mergedProperties.putAll(otherProperties);
		return mergedProperties;
	}
	
	public static Properties getConfiguration() {
		Properties properties = new Properties();
		properties = setDefaultProperties(properties);
		return properties;
	}
	
	public static Properties setDefaultProperties(Properties configuration) {
		configuration.setProperty("monitor.iostat", "true");
		configuration.setProperty("monitor.system.memory.usage", "true");
		configuration.setProperty("monitor.network.interface.counters", "true");
		configuration.setProperty("monitor.system.cpu.usage", "true");
		configuration.setProperty("monitor.processes.cpu", "true");
		configuration.setProperty("monitor.network.connections.queuesizes", "true");
		configuration.setProperty("monitor.mfs.guts",  "true");
		configuration.setProperty("distill.system.cpu.usage", "true");
		configuration.setProperty("distill.network.connections.queuesizes", "true");
		configuration.setProperty("distill.disk.usage", "true");
		configuration.setProperty("distill.network.interface.counters", "true");
		configuration.setProperty("distill.mfs.guts", "true");
		configuration.setProperty("distill.system.memory.usage", "true");
		configuration.setProperty("distill.processes.cpu", "true");
		configuration.setProperty("rpc.server.port", "23721");
		return configuration;
	}
	
	public static Properties applyConfigurationFile(Properties configuration, String configurationFilePath) {
		FileInputStream confFile = null;
		try {
			confFile = new FileInputStream(configurationFilePath);
		} catch (FileNotFoundException e) {
			LOG.error("FATAL: Configuration file not found: " + configurationFilePath);
			System.exit(1);
		}
		try {
			configuration.load(confFile);
		} catch (IOException e) {
			LOG.error("FATAL: Failed to read configuration from file: " + configurationFilePath);
			System.exit(1);
		}
		LOG.debug("DEBUG: Applied configuration from file: " + configurationFilePath);
		return configuration;
	}
}
