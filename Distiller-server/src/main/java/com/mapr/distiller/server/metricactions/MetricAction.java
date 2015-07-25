package com.mapr.distiller.server.metricactions;

public abstract class MetricAction implements Runnable {
	protected String id;
	protected volatile boolean isGathericMetric;
	
	//Object object = new Object();
	
	/*public void suspend() throws InterruptedException {
		System.out.println("Stopping metric with " + id);
		isGathericMetric = false;
		synchronized (object) {
			object.wait();
		}
	}

	public void resume() {
		System.out.println("Resuming metric with " + id);
		isGathericMetric = true;
		synchronized (object) {
			object.notifyAll();
		}
	}*/
	
	public abstract void suspend() throws InterruptedException;
	
	public abstract void resume();

	public abstract void selectSequentialRecords();

	public abstract void selectCumulativeRecords();

	public abstract void selectTimeSeparatedRecords();

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
