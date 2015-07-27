package com.mapr.distiller.server.metricactions;

public abstract class MetricAction implements Runnable {
	protected String id;
	protected volatile boolean isGathericMetric;
	
	public abstract void suspend() throws InterruptedException;
	
	public abstract void resume();
	
	public abstract void kill();
	
	public boolean isGathericMetric() {
		return isGathericMetric;
	}

	public void setGathericMetric(boolean isGathericMetric) {
		this.isGathericMetric = isGathericMetric;
	}

	public MetricAction(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

}
