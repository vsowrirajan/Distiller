package com.mapr.distiller.server.recordtypes;

public class SystemCpuHighRecord extends SystemCpuRecord {
	private double systemCpuHighThreshold;
	private int sampleWidth;
	
	public String toString() {
		return super.toString() + " usage above threshold:" + systemCpuHighThreshold + " sampleWidth:" + sampleWidth;
	}
}
