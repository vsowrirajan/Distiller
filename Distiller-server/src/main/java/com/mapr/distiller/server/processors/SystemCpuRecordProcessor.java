package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemCpuRecord;

public class SystemCpuRecordProcessor implements RecordProcessor<Record> {

	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		SystemCpuRecord currentRecord = (SystemCpuRecord) record;

		switch (metric) {
		case "%idle":
			if (currentRecord.getIdleCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to value");
			else
				return currentRecord.getIdleCpuUtilPct() == Double
						.parseDouble(thresholdValue);

		case "%iowait":
			if (currentRecord.getIowaitCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to value");
			else
				return currentRecord.getIowaitCpuUtilPct() == Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		SystemCpuRecord currentRecord = (SystemCpuRecord) record;

		switch (metric) {
		case "%idle":
			if (currentRecord.getIdleCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIdleCpuUtilPct() > Double
						.parseDouble(thresholdValue);

		case "%iowait":
			if (currentRecord.getIowaitCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIowaitCpuUtilPct() > Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		SystemCpuRecord currentRecord = (SystemCpuRecord) record;

		switch (metric) {
		case "%idle":
			if (currentRecord.getIdleCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIdleCpuUtilPct() < Double
						.parseDouble(thresholdValue);

		case "%iowait":
			if (currentRecord.getIowaitCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIowaitCpuUtilPct() < Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public SystemCpuRecord movingAverage(Record rec1, Record rec2)
			throws Exception {
		return new SystemCpuRecord((SystemCpuRecord) rec1,
				(SystemCpuRecord) rec2);
	}

}
