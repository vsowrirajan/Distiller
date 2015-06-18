package com.mapr.distiller.server.recordtypes;

public class DeviceUtilizationSample {
	private String name;
	private int numReadReqs, numWriteReqs, avgReadSizeBytes, avgWriteSizeBytes;
	private double readMB, writeMB, avgWaitms, utilizationPct;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getNumReadReqs() {
		return numReadReqs;
	}
	public void setNumReadReqs(int numReadReqs) {
		this.numReadReqs = numReadReqs;
	}
	public int getNumWriteReqs() {
		return numWriteReqs;
	}
	public void setNumWriteReqs(int numWriteReqs) {
		this.numWriteReqs = numWriteReqs;
	}
	public int getAvgReadSizeBytes() {
		return avgReadSizeBytes;
	}
	public void setAvgReadSizeBytes(int avgReadSizeBytes) {
		this.avgReadSizeBytes = avgReadSizeBytes;
	}
	public int getAvgWriteSizeBytes() {
		return avgWriteSizeBytes;
	}
	public void setAvgWriteSizeBytes(int avgWriteSizeBytes) {
		this.avgWriteSizeBytes = avgWriteSizeBytes;
	}
	public double getReadMB() {
		return readMB;
	}
	public void setReadMB(double readMB) {
		this.readMB = readMB;
	}
	public double getWriteMB() {
		return writeMB;
	}
	public void setWriteMB(double writeMB) {
		this.writeMB = writeMB;
	}
	public double getAvgWaitms() {
		return avgWaitms;
	}
	public void setAvgWaitms(double avgWaitms) {
		this.avgWaitms = avgWaitms;
	}
	public double getUtilizationPct() {
		return utilizationPct;
	}
	public void setUtilizationPct(double utilizationPct) {
		this.utilizationPct = utilizationPct;
	}
}
