package com.mapr.distiller.server.recordtypes;

public class CpuUtilizationSample {
	private double user, system, idle;

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
}
