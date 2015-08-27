package com.mapr.distiller.server.status;

public class RecordProducerStatus {
	private boolean isMfsGutsRecordProducerRunning;
	private boolean isMfsGutsRecordProducerMetricsEnabled;
	
	private boolean isProcRecordProducerRunning;
	private boolean isProcRecordProducerMetricsEnabled;
	
	private String[] procRecordProducerMetricsList;

	public RecordProducerStatus(boolean isMfsGutsRecordProducerRunning,
			boolean isMfsGutsRecordProducerMetricsEnabled,
			boolean isProcRecordProducerRunning,
			boolean isProcRecordProducerMetricsEnabled,
			String[] procRecordProducerMetricsList) {
		this.isMfsGutsRecordProducerRunning = isMfsGutsRecordProducerRunning;
		this.isMfsGutsRecordProducerMetricsEnabled = isMfsGutsRecordProducerMetricsEnabled;
		this.isProcRecordProducerRunning = isProcRecordProducerRunning;
		this.isProcRecordProducerMetricsEnabled = isProcRecordProducerMetricsEnabled;
		this.procRecordProducerMetricsList = procRecordProducerMetricsList;
	}

	public boolean isMfsGutsRecordProducerRunning() {
		return isMfsGutsRecordProducerRunning;
	}

	public boolean isMfsGutsRecordProducerMetricsEnabled() {
		return isMfsGutsRecordProducerMetricsEnabled;
	}

	public boolean isProcRecordProducerRunning() {
		return isProcRecordProducerRunning;
	}

	public boolean isProcRecordProducerMetricsEnabled() {
		return isProcRecordProducerMetricsEnabled;
	}

	public String[] getProcRecordProducerMetricsList() {
		return procRecordProducerMetricsList;
	}
	
}
