package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.SystemCpuRecord;

public class SystemCpuRecordProcessor implements
		Thresholdable<SystemCpuRecord>, MovingAverageable<SystemCpuRecord> {

	public boolean isNotEqual(SystemCpuRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(SystemCpuRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "%idle":
			if(record.getIdleCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to value");
			else
				return record.getIdleCpuUtilPct() == Double.parseDouble(thresholdValue);

		case "%iowait":
			if(record.getIowaitCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to value");
			else
				return record.getIowaitCpuUtilPct() == Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}
	
	@Override
	public boolean isAboveThreshold(SystemCpuRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "%idle":
			if(record.getIdleCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to threshold");
			else
				return record.getIdleCpuUtilPct() > Double.parseDouble(thresholdValue);

		case "%iowait":
			if(record.getIowaitCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to threshold");
			else
				return record.getIowaitCpuUtilPct() > Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(SystemCpuRecord record, String metric,
			String thresholdValue) throws Exception {
		switch (metric) {
		case "%idle":
			if(record.getIdleCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to threshold");
			else
				return record.getIdleCpuUtilPct() < Double.parseDouble(thresholdValue);

		case "%iowait":
			if(record.getIowaitCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to threshold");
			else
				return record.getIowaitCpuUtilPct() < Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public SystemCpuRecord movingAverage(SystemCpuRecord rec1,
			SystemCpuRecord rec2) throws Exception{
		return new SystemCpuRecord(rec1, rec2);
	}

}
