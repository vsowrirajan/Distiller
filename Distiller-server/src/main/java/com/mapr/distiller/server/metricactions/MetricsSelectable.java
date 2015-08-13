package com.mapr.distiller.server.metricactions;

public interface MetricsSelectable {
	public void selectSequentialRecords() throws Exception;

	public void selectSequentialRecordsWithQualifier() throws Exception;

	public void selectCumulativeRecords() throws Exception;

	public void selectCumulativeRecordsWithQualifier() throws Exception;

	public void selectTimeSeparatedRecords() throws Exception;

}
