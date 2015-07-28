package com.mapr.distiller.server.processors;

public interface Thresholdable<T> {
	// Metric above than a threshold value, emit a record
	public boolean isAboveThreshold(T record, String metric,
			String thresholdValue) throws Exception;

	// Metric below than a threshold value, emit a record
	public boolean isBelowThreshold(T record, String metric,
			String thresholdValue) throws Exception;
	
	public boolean isEqual(T record, String metric,
			String thresholdValue) throws Exception;
}
