package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

public abstract class MetricAction implements Runnable, MetricsSelectable {
	protected String id;
	protected volatile boolean gatherMetric;

	Object object = new Object();

	protected RecordQueue inputQueue;
	protected RecordQueue outputQueue;
	protected String recordType;

	protected RecordProcessor<Record> recordProcessor;

	protected String aggregationType;
	protected Map<String, String> aggregationMap;

	private boolean shouldPersist;

	public MetricAction(String id, String recordType, String aggregationType,
			Map<String, String> aggregationMap, RecordQueue inputQueue,
			RecordQueue outputQueue, RecordProcessor<Record> recordProcessor) {
		this.id = id;
		this.recordType = recordType;
		this.aggregationType = aggregationType;
		this.aggregationMap = aggregationMap;

		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;

		this.recordProcessor = recordProcessor;

		setGathericMetric(true);
	}

	public abstract void suspend() throws InterruptedException;

	public abstract void resume();

	public abstract void kill();

	@Override
	public void selectSequentialRecords() {
		System.out.println("Id " + id);
		inputQueue.registerConsumer(id);
		outputQueue.registerProducer(id);

		try {
			Record newRecord = null;
			Record oldRecord = inputQueue.get(id);
			System.out.println("Is Record null " + oldRecord != null);
			while (((newRecord = inputQueue.get(id)) != null) && !gatherMetric) {
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

	public boolean isGathericMetric() {
		return gatherMetric;
	}

	public void setGathericMetric(boolean isGathericMetric) {
		this.gatherMetric = isGathericMetric;
	}

	public MetricAction(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

}
