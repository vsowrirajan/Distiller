package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.processors.SystemCpuRecordProcessor;
import com.mapr.distiller.server.processors.SystemMemoryRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.utils.MetricConfig;

public class SystemMemoryMetricAction extends MetricAction {

	Object object = new Object();

	private RecordQueue inputQueue;
	private RecordQueue outputQueue;
	private String recordType;

	private SystemMemoryRecordProcessor recordProcessor;

	private String aggregationType;
	private Map<String, String> aggregationMap;

	public SystemMemoryMetricAction(String id, String recordType,
			String aggregationType, Map<String, String> aggregationMap,
			RecordQueue inputQueue, RecordQueue outputQueue,
			RecordProcessor<Record> recordProcessor) {
		super(id, recordType, aggregationType, aggregationMap, inputQueue,
				outputQueue, recordProcessor);
	}

	// We also need one more argument which has a RecordQueue map to their names
	public static SystemMemoryMetricAction getInstance(
			MetricConfig metricConfig, RecordQueueManager queueManager) {
		RecordProcessor<Record> recordProcessor = new SystemMemoryRecordProcessor();
		return new SystemMemoryMetricAction(metricConfig.getId(),
				metricConfig.getRecordType(),
				metricConfig.getAggregationType(),
				metricConfig.getAggregationMap(),
				queueManager.getQueue(metricConfig.getInputQueue()),
				queueManager.getQueue(metricConfig.getOutputQueue()),
				recordProcessor);
	}

	@Override
	public void run() {
		System.out.println("****" + this.recordType + "****");
		while (!Thread.interrupted()) {
			if (isGathericMetric()) {
				selectSequentialRecords();
				/*
				 * try { Thread.sleep(1000); } catch (InterruptedException e) {
				 * System.out.println("Thread got interrupted - Going down");
				 * break; }
				 */
			}

			else {
				try {
					synchronized (object) {
						object.wait();
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		System.out.println("Exiting Metric action due to kill action");

	}

	@Override
	public void suspend() throws InterruptedException {
		System.out.println("Stopping metric with id = " + id);
		if (gatherMetric) {
			gatherMetric = false;
		}

		else {
			System.out.println("Already suspended metric " + id);
		}
	}

	@Override
	public void resume() {
		synchronized (object) {
			if (!gatherMetric) {
				System.out.println("Resuming metric with id = " + id);
				gatherMetric = true;
				object.notifyAll();
			}

			else {
				System.out.println("Already running metric " + id);
			}
		}
	}

	@Override
	public void kill() {
		System.out.println("Kill metric with id = " + id);
		gatherMetric = false;
	}
}
