package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.MfsGutsRecordProcessor;
import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.MetricConfig;

public class MfsGutsMetricAction extends MetricAction {

	private MfsGutsMetricAction(String id, String recordType,
			String aggregationType, Map<String, String> aggregationMap,
			RecordQueue inputQueue, RecordQueue outputQueue,
			RecordProcessor<Record> recordProcessor) {
		super(id, recordType, aggregationType, aggregationMap, inputQueue,
				outputQueue, recordProcessor);
	}

	public static MfsGutsMetricAction getInstance(MetricConfig metricConfig,
			RecordQueueManager queueManager) {
		RecordProcessor<Record> recordProcessor = new MfsGutsRecordProcessor();
		return new MfsGutsMetricAction(metricConfig.getId(),
				metricConfig.getRecordType(),
				metricConfig.getAggregationType(),
				metricConfig.getAggregationMap(),
				queueManager.getQueue(metricConfig.getInputQueue()),
				queueManager.getQueue(metricConfig.getOutputQueue()),
				recordProcessor);
	}
}
