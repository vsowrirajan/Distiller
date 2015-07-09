package com.mapr.distiller.server.processors;

public interface Thresholdable<T> {
	public boolean isAboveThreshold(T record, String metric,
			String thresholdValue) throws Exception;

	public boolean isBelowThreshold(T record, String metric,
			String thresholdValue) throws Exception;
}
