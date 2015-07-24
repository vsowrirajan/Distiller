package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.SystemCpuRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.MetricConfig;

public class SystemCpuMetricAction extends MetricAction {

	private RecordQueue inputQueue;
	private RecordQueue outputQueue;
	private String recordType;

	private SystemCpuRecordProcessor recordProcessor;

	private String aggregationType;
	private Map<String, String> aggregationMap;

	private boolean shouldPersist;

	private boolean isGathericMetric;

	public SystemCpuMetricAction(String id, String recordType,
			String aggregationType, Map<String, String> aggregationMap) {
		super(id);
		this.recordType = recordType;
		this.aggregationType = aggregationType;
		this.aggregationMap = aggregationMap;
	}

	// We also need one more argument which has a RecordQueue map to their names
	public static SystemCpuMetricAction getInstance(MetricConfig metricConfig) {
		return new SystemCpuMetricAction(metricConfig.getId(),
				metricConfig.getRecordType(),
				metricConfig.getAggregationType(),
				metricConfig.getAggregationMap());
	}

	@Override
	public void run() {
		System.out.println("****" + this.recordType + "****");
		while (true) {
			System.out.println(this.aggregationType);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
