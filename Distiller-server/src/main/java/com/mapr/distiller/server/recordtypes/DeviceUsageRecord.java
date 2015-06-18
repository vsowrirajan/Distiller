package com.mapr.distiller.server.recordtypes;

public class DeviceUsageRecord extends Record {
	private String name;
	private int numReadReqs, numWriteReqs, avgReadSizeBytes, avgWriteSizeBytes;
	private double readMB, writeMB, avgWaitms, utilizationPct;

	@Override
	public String toString() {
		return super.toString() + " DeviceUsage dev:" + name + " nrr:"
				+ numReadReqs + " nwr:" + numWriteReqs + " avgrs:"
				+ avgReadSizeBytes + "b avgws:" + avgWriteSizeBytes + "b rMB:"
				+ readMB + " wMB" + writeMB + " await:" + avgWaitms
				+ "ms util:" + utilizationPct + "%";
	}
}
