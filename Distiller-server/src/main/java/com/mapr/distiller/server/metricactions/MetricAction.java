package com.mapr.distiller.server.metricactions;

public abstract class MetricAction implements Runnable {
	private String id;

	public MetricAction(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public abstract void selectSequentialRecords();

	public abstract void selectCumulativeRecords();

	public abstract void selectTimeSeparatedRecords();

}
