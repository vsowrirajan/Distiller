package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.SystemMemoryRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.MetricConfig;

public class SystemMemoryMetricAction extends MetricAction implements MetricsSelectable {

	Object object = new Object();

	private RecordQueue inputQueue;
	private RecordQueue outputQueue;
	private String recordType;

	private SystemMemoryRecordProcessor recordProcessor;

	private String aggregationType;
	private Map<String, String> aggregationMap;

	public SystemMemoryMetricAction(String id, String recordType,
			String aggregationType, Map<String, String> aggregationMap) {
		super(id);
		this.recordType = recordType;
		this.aggregationType = aggregationType;
		this.aggregationMap = aggregationMap;
		setGathericMetric(true);
	}

	// We also need one more argument which has a RecordQueue map to their names
	public static SystemMemoryMetricAction getInstance(MetricConfig metricConfig) {
		return new SystemMemoryMetricAction(metricConfig.getId(),
				metricConfig.getRecordType(),
				metricConfig.getAggregationType(),
				metricConfig.getAggregationMap());
	}

	@Override
	public void run() {
		System.out.println("****" + this.recordType + "****");
		while (!Thread.interrupted()) {
			if (isGathericMetric()) {
				System.out.println(this.aggregationType);
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
	public void suspend() throws InterruptedException {
		System.out.println("Stopping metric with id = " + id);
		if (isGathericMetric) {
			isGathericMetric = false;
		}
		
		else {
			System.out.println("Already suspended metric "+id);
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
		// TODO Auto-generated method stub

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
