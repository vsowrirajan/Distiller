package com.mapr.distiller.server;

import java.util.List;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.status.MetricActionStatus;
import com.mapr.distiller.server.status.RecordProducerStatus;
import com.mapr.distiller.server.status.RecordQueueStatus;

//Interface to expose Coordinator methods to outside world - In short for distiller-client
public interface DistillerMonitor {
	public RecordProducerStatus getRecordProducerStatus();

	public List<MetricActionStatus> getMetricActions();

	public boolean metricDisable(String metricName);

	public boolean metricEnable(String metricName);

	public boolean metricDelete(String metricName);

	public boolean isScheduledMetricAction(String metricAction);

	public boolean isRunningMetricAction(String metricAction);

	public List<RecordQueueStatus> getRecordQueues();
	
	public RecordQueueStatus getQueueStatus(String queueName);

	public Record[] getRecords(String queueName, int count);

}
