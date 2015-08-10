package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

public abstract class MetricAction implements Runnable, MetricsSelectable {
	Object object = new Object();
	
	protected String id;
	protected volatile boolean gatherMetric;
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
	
	@Override
	public void run() {
		System.out.println("****" + this.recordType + "****");
		while (!Thread.interrupted()) {
			if (isGathericMetric()) {
				System.out.println("Processing metric " + this.id);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.println("Thread got interrupted - Going down");
					break;
				}
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
	

	public void suspend() throws InterruptedException {
		System.out.println("Stopping metric with id = " + id);
		if (gatherMetric) {
			gatherMetric = false;
		}

		else {
			System.out.println("Already suspended metric " + id);
		}
	}

	public void resume() {
		synchronized (object) {
			if (!gatherMetric) {
				System.out.println("Resuming metric with id = " + id);
				gatherMetric = true;
				object.notifyAll();
			}

			else {
				System.out.println("Already running Metric " + id);
			}
		}
	}

	public void kill() {
		System.out.println("Kill metric with id = " + id);
		gatherMetric = false;
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
