package com.mapr.distiller.server.processors;

import java.util.List;

import com.mapr.distiller.server.recordtypes.SystemCpuRecord;

public class SystemCpuRecordProcessor implements
		Thresholdable<SystemCpuRecord>, MovingAverageable<SystemCpuRecord> {

	@Override
	public boolean isAboveThreshold(SystemCpuRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "idle":
			return record.getIdle() > Double.parseDouble(thresholdValue);

		case "system":
			return record.getSystem() > Double.parseDouble(thresholdValue);

		case "user":
			return record.getUser() > Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " does not have a value in SystemCpuRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(SystemCpuRecord record, String metric,
			String thresholdValue) throws Exception {
		switch (metric) {
		case "idle":
			return record.getIdle() < Double.parseDouble(thresholdValue);

		case "system":
			return record.getSystem() < Double.parseDouble(thresholdValue);

		case "user":
			return record.getUser() < Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " does not have a value in SystemCpuRecord");
		}
	}

	@Override
	public SystemCpuRecord movingAverage(List<SystemCpuRecord> records) {
		SystemCpuRecord systemCpuRecord = null;

		double idle = 0;
		double system = 0;
		double user = 0;

		int recordsSize = records.size();

		for (SystemCpuRecord record : records) {
			idle += record.getIdle();
			system += record.getSystem();
			user += record.getUser();
		}

		idle = idle / recordsSize;
		system = system / recordsSize;
		user = user / recordsSize;

		systemCpuRecord = new SystemCpuRecord(user, system, idle);

		return systemCpuRecord;
	}

}
