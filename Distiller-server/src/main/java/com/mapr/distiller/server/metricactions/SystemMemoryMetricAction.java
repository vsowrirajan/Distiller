package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.SystemMemoryRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.utils.MetricConfig;

public class SystemMemoryMetricAction extends MetricAction implements
		MetricsSelectable {

	Object object = new Object();

	private RecordQueue inputQueue;
	private RecordQueue outputQueue;
	private String recordType;

	private SystemMemoryRecordProcessor recordProcessor;

	private String aggregationType;
	private Map<String, String> aggregationMap;

	public SystemMemoryMetricAction(String id, String recordType,
			String aggregationType, Map<String, String> aggregationMap,
			RecordQueue inputQueue, RecordQueue outputQueue) {
		super(id);
		this.recordType = recordType;
		this.aggregationType = aggregationType;
		this.aggregationMap = aggregationMap;

		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;

		setGathericMetric(true);
	}

	// We also need one more argument which has a RecordQueue map to their names
	public static SystemMemoryMetricAction getInstance(
			MetricConfig metricConfig, RecordQueueManager queueManager) {
		return new SystemMemoryMetricAction(metricConfig.getId(),
				metricConfig.getRecordType(),
				metricConfig.getAggregationType(),
				metricConfig.getAggregationMap(),
				queueManager.getQueue(metricConfig.getInputQueue()),
				queueManager.getQueue(metricConfig.getOutputQueue()));
	}

	@Override
	public void run() {
		System.out.println("****" + this.recordType + "****");
		while (!Thread.interrupted()) {
			if (isGathericMetric()) {
				selectSequentialRecords();
				/*try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.println("Thread got interrupted - Going down");
					break;
				}*/
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
		if (isGathericMetric) {
			isGathericMetric = false;
		}

		else {
			System.out.println("Already suspended metric " + id);
		}
	}

	@Override
	public void resume() {
		synchronized (object) {
			if (!isGathericMetric) {
				System.out.println("Resuming metric with id = " + id);
				isGathericMetric = true;
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
		isGathericMetric = false;
	}

	@Override
	public void selectSequentialRecords() {
		System.out.println("Id " + id);
		inputQueue.registerConsumer(id);
		outputQueue.registerProducer(id);

		try {
			SystemMemoryRecord newRecord = null;
			SystemMemoryRecord oldRecord = (SystemMemoryRecord) inputQueue
					.get(id);
			System.out.println("Is Record null " + oldRecord != null);
			while (((newRecord = (SystemMemoryRecord) inputQueue.get(id)) != null)
					&& !isGathericMetric) {
				switch (aggregationType) {
				case Constants.MOVING_AVERAGE:
					Record processedRecord = recordProcessor.movingAverage(
							oldRecord, newRecord);
					outputQueue.put(id, processedRecord);
					break;

				default:
					throw new Exception("Not a valid processing type "
							+ aggregationType);
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

		finally {
			inputQueue.unregisterConsumer(id);
			outputQueue.unregisterProducer(id);
		}
		System.out.println("Processed Sequential records");
	}

	@Override
	public void selectCumulativeRecords() {
		// TODO Auto-generated method stub

	}

	@Override
	public void selectTimeSeparatedRecords() {
		// TODO Auto-generated method stub

	}

}
