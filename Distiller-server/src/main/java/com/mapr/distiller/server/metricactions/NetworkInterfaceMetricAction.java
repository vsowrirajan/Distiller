package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.NetworkInterfaceRecordProcessor;
import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.MetricConfig;

public class NetworkInterfaceMetricAction extends MetricAction {

	private NetworkInterfaceMetricAction(String id, String recordType,
			String aggregationType, Map<String, String> aggregationMap,
			RecordQueue inputQueue, RecordQueue outputQueue,
			RecordProcessor<Record> recordProcessor) {
		super(id, recordType, aggregationType, aggregationMap, inputQueue,
				outputQueue, recordProcessor);
	}

	public static NetworkInterfaceMetricAction getInstance(
			MetricConfig metricConfig, RecordQueueManager queueManager) {
		RecordProcessor<Record> recordProcessor = new NetworkInterfaceRecordProcessor();
		return new NetworkInterfaceMetricAction(metricConfig.getId(),
				metricConfig.getRecordType(),
				metricConfig.getAggregationType(),
				metricConfig.getAggregationMap(),
				queueManager.getQueue(metricConfig.getInputQueue()),
				queueManager.getQueue(metricConfig.getOutputQueue()),
				recordProcessor);
	}
}
