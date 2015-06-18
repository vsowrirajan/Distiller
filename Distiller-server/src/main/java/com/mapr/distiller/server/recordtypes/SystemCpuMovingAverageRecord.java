package com.mapr.distiller.server.recordtypes;

public class SystemCpuMovingAverageRecord extends SystemCpuRecord {

	private int movingAverageSampleWidth;
	private long movingAverageDurationms, movingAverageStartTime;
	
	public SystemCpuMovingAverageRecord() {
		super();
	}
	
	public SystemCpuMovingAverageRecord(long timestamp, long previousTimestamp,
			long durationms) {
		super(timestamp, previousTimestamp, durationms);
	}

	public SystemCpuMovingAverageRecord(long timestamp, long previousTimestamp,
			long durationms, int movingAverageSampleWidth,
			long movingAverageDurationms, long movingAverageStartTime) {
		super(timestamp, previousTimestamp, durationms);
		this.movingAverageSampleWidth = movingAverageSampleWidth;
		this.movingAverageDurationms = movingAverageDurationms;
		this.movingAverageStartTime = movingAverageStartTime;
	}

	public SystemCpuMovingAverageRecord(long timestamp, long previousTimestamp,
			long durationms, double user, double system, double idle,
			int movingAverageSampleWidth, long movingAverageDurationms,
			long movingAverageStartTime) {
		super(timestamp, previousTimestamp, durationms, user, system, idle);
		this.movingAverageSampleWidth = movingAverageSampleWidth;
		this.movingAverageDurationms = movingAverageDurationms;
		this.movingAverageStartTime = movingAverageStartTime;
	}

	public int getMovingAverageSampleWidth() {
		return movingAverageSampleWidth;
	}

	public void setMovingAverageSampleWidth(int movingAverageSampleWidth) {
		this.movingAverageSampleWidth = movingAverageSampleWidth;
	}

	public long getMovingAverageDurationms() {
		return movingAverageDurationms;
	}

	public void setMovingAverageDurationms(long movingAverageDurationms) {
		this.movingAverageDurationms = movingAverageDurationms;
	}

	public long getMovingAverageStartTime() {
		return movingAverageStartTime;
	}

	public void setMovingAverageStartTime(long movingAverageStartTime) {
		this.movingAverageStartTime = movingAverageStartTime;
	}

	@Override
	public String toString() {
		return super.toString() + " MovingAverage sampleWidth:"
				+ movingAverageSampleWidth + " durationMA:"
				+ movingAverageDurationms + "ms startTime:"
				+ movingAverageStartTime;
	}
}
