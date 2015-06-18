package com.mapr.distiller.server.recordtypes;

public class SystemCpuMovingAverageRecord extends SystemCpuRecord {
	private int movingAverageSampleWidth;
	private long movingAverageDurationms, movingAverageStartTime;

	@Override
	public String toString() {
		return super.toString() + " MovingAverage sampleWidth:"
				+ movingAverageSampleWidth + " durationMA:"
				+ movingAverageDurationms + "ms startTime:"
				+ movingAverageStartTime;
	}
}
