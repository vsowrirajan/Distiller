package com.mapr.distiller.server.recordtypes;

public class Record {

	private long timestamp, previousTimestamp, durationms;
	
	public Record() {
		
	}
	
	public Record(long timestamp, long previousTimestamp, long durationms) {
		this.timestamp = timestamp;
		this.previousTimestamp = previousTimestamp;
		this.durationms = durationms;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getPreviousTimestamp() {
		return previousTimestamp;
	}

	public void setPreviousTimestamp(long previousTimestamp) {
		this.previousTimestamp = previousTimestamp;
	}

	public long getDurationms() {
		return durationms;
	}

	public void setDurationms(long durationms) {
		this.durationms = durationms;
	}

	@Override
	public String toString() {
		return "RecTime: " + previousTimestamp + " - " + timestamp + " ("
				+ durationms + "ms)";
	}
}
