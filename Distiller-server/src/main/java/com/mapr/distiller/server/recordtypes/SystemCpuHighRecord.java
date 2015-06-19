package com.mapr.distiller.server.recordtypes;

public class SystemCpuHighRecord extends SystemCpuRecord {
	private double systemCpuHighThreshold;
	private int sampleWidth;
	
	public double getSystemCpuHighThreshold() {
		return systemCpuHighThreshold;
	}

	public void setSystemCpuHighThreshold(double systemCpuHighThreshold) {
		this.systemCpuHighThreshold = systemCpuHighThreshold;
	}

	public int getSampleWidth() {
		return sampleWidth;
	}

	public void setSampleWidth(int sampleWidth) {
		this.sampleWidth = sampleWidth;
	}

	public String toString() {
		return super.toString() + " usage above threshold:" + systemCpuHighThreshold + " sampleWidth:" + sampleWidth;
	}
}
