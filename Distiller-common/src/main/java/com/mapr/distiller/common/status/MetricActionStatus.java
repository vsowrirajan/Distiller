package com.mapr.distiller.common.status;

public class MetricActionStatus {
	private String id;
	private boolean isEnabled;
	private boolean isRunning;
	private boolean isScheduled;
	private String nextSchedule;

	public MetricActionStatus(String id, boolean isEnabled,
			boolean isRunning, boolean isScheduled, String nextSchedule) {
		this.id = id;
		this.isEnabled = isEnabled;
		this.isRunning = isRunning;
		this.isScheduled = isScheduled;
		this.nextSchedule = nextSchedule;
	}

	public String getId() {
		return id;
	}

	public boolean isEnabled() {
		return isEnabled;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public boolean isScheduled() {
		return isScheduled;
	}

	public String getNextSchedule() {
		return nextSchedule;
	}	
}
