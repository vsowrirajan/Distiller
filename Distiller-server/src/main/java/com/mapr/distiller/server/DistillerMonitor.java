package com.mapr.distiller.server;

public interface DistillerMonitor {
	public String getRecordProducerStatus();

	public String getMetricActions();

	public boolean metricDisable(String metricName);

	public boolean metricEnable(String metricName);

	public boolean metricDelete(String metricName);

	public boolean isScheduledMetricAction(String metricAction);

	public boolean isRunningMetricAction(String metricAction);

	public String getRecordQueues();
	
	public String getQueueStatus(String queueName);

	public String getRecords(String queueName, int count);

}
