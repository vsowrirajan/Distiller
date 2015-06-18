package com.mapr.distiller.server.recordtypes;

public class SystemCpuRecord extends Record {
	private double user, system, idle;

	@Override
	public String toString() {
		return super.toString() + " SystemCpu idle:" + idle + " user:" + user
				+ " system:" + system;
	}
}
