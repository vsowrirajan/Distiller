package com.mapr.distiller.server.recordtypes;

public class Record {

	private long timestamp, previousTimestamp, durationms;

	@Override
	public String toString() {
		return "RecTime: " + previousTimestamp + " - " + timestamp + " ("
				+ durationms + "ms)";
	}
}
