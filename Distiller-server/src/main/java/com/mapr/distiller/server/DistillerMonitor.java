package com.mapr.distiller.server;

import java.util.List;

import com.mapr.distiller.common.status.MetricActionStatus;
import com.mapr.distiller.common.status.RecordProducerStatus;
import com.mapr.distiller.common.status.RecordQueueStatus;
import com.mapr.distiller.server.recordtypes.Record;

//Interface to expose Coordinator methods to outside world - In short for distiller-client
public interface DistillerMonitor {
	public RecordProducerStatus getRecordProducerStatus();

	public List<MetricActionStatus> getMetricActions();

	public boolean metricDisable(String metricName) throws Exception;

	public boolean metricEnable(String metricName) throws Exception;

	public boolean metricDelete(String metricName) throws Exception;

	public boolean isScheduledMetricAction(String metricAction) throws Exception;

	public boolean isRunningMetricAction(String metricAction) throws Exception;

	public List<RecordQueueStatus> getRecordQueues();
	
	public RecordQueueStatus getQueueStatus(String queueName) throws Exception;

	public Record[] getRecords(String queueName, int count) throws Exception;

	public MetricActionStatus getMetricAction(String metricActionName) throws Exception;

}
