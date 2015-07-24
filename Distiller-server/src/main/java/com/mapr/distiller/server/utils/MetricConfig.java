package com.mapr.distiller.server.utils;

import java.util.Map;

public class MetricConfig {

	private final String id;
	private final String inputQueue;
	private final String outputQueue;
	private final String recordType;
	private final String aggregationType;
	private final Map<String, String> aggregationMap;
	private final boolean shouldPersist;

	private MetricConfig(MetricConfigBuilder metricConfigBuilder) {
		// This can be randomly generated using java uuid()
		this.id = metricConfigBuilder.id;
		this.inputQueue = metricConfigBuilder.inputQueue;
		this.outputQueue = metricConfigBuilder.outputQueue;
		this.recordType = metricConfigBuilder.recordType;
		this.aggregationType = metricConfigBuilder.aggregationType;
		this.aggregationMap = metricConfigBuilder.aggregationMap;
		this.shouldPersist = metricConfigBuilder.shouldPersist;
	}

	public String getId() {
		return id;
	}

	public String getInputQueue() {
		return inputQueue;
	}

	public String getOutputQueue() {
		return outputQueue;
	}

	public String getRecordType() {
		return recordType;
	}

	public String getAggregationType() {
		return aggregationType;
	}

	public Map<String, String> getAggregationMap() {
		return aggregationMap;
	}

	public boolean isShouldPersist() {
		return shouldPersist;
	}

	// Builder pattern
	public static class MetricConfigBuilder {
		private final String id;
		private final String inputQueue;
		private final String outputQueue;
		private final String recordType;
		private String aggregationType;
		private Map<String, String> aggregationMap;
		private boolean shouldPersist;

		public MetricConfigBuilder(String id, String inputQueue,
				String ouputQueue, String recordType) {
			if (inputQueue == null || ouputQueue == null || recordType == null) {
				throw new IllegalArgumentException(
						"id or inputqueue or outputQueue or recordType cannot be null");
			}

			this.id = id;
			this.inputQueue = inputQueue;
			this.outputQueue = ouputQueue;
			this.recordType = recordType;
		}

		public MetricConfigBuilder aggregation(String aggregationType,
				Map<String, String> aggregationMap) {
			this.aggregationType = aggregationType;
			this.aggregationMap = aggregationMap;
			return this;
		}

		public MetricConfigBuilder persist(boolean shouldPersist) {
			this.shouldPersist = shouldPersist;
			return this;
		}

		public MetricConfig build() {
			return new MetricConfig(this);
		}
	}

}
