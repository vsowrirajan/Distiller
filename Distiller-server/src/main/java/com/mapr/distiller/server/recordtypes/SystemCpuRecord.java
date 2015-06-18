package com.mapr.distiller.server.recordtypes;

public class SystemCpuRecord extends Record {
	private double user, system, idle;

	public SystemCpuRecord() {
		super();
	}
	
	public SystemCpuRecord(long timestamp, long previousTimestamp,
			long durationms) {
		super(timestamp, previousTimestamp, durationms);
	}

	public SystemCpuRecord(long timestamp, long previousTimestamp,
			long durationms, double user, double system, double idle) {
		super(timestamp, previousTimestamp, durationms);
		this.user = user;
		this.system = system;
		this.idle = idle;
	}

	public double getUser() {
		return user;
	}

	public void setUser(double user) {
		this.user = user;
	}

	public double getSystem() {
		return system;
	}

	public void setSystem(double system) {
		this.system = system;
	}

	public double getIdle() {
		return idle;
	}

	public void setIdle(double idle) {
		this.idle = idle;
	}

	@Override
	public String toString() {
		return super.toString() + " SystemCpu idle:" + idle + " user:" + user
				+ " system:" + system;
	}
}
