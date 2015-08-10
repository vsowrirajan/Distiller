package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.processors.ThreadResourceRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.MetricConfig;

public class ThreadResourceMetricAction extends MetricAction {

	private ThreadResourceMetricAction(String id, String recordType,
			String aggregationType, Map<String, String> aggregationMap,
			RecordQueue inputQueue, RecordQueue outputQueue,
			RecordProcessor<Record> recordProcessor) {
		super(id, recordType, aggregationType, aggregationMap, inputQueue,
				outputQueue, recordProcessor);
	}

	public static ThreadResourceMetricAction getInstance(
			MetricConfig metricConfig, RecordQueueManager queueManager) {
		RecordProcessor<Record> recordProcessor = new ThreadResourceRecordProcessor();
		return new ThreadResourceMetricAction(metricConfig.getId(),
				metricConfig.getRecordType(),
				metricConfig.getAggregationType(),
				metricConfig.getAggregationMap(),
				queueManager.getQueue(metricConfig.getInputQueue()),
				queueManager.getQueue(metricConfig.getOutputQueue()),
				recordProcessor);
	}
}
