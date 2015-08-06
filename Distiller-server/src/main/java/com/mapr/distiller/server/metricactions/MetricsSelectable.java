package com.mapr.distiller.server.metricactions;

public interface MetricsSelectable {
	public void selectSequentialRecords();

	public void selectCumulativeRecords();

	public void selectTimeSeparatedRecords();

}
