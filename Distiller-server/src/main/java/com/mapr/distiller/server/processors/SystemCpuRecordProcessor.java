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
			if(record.getIdleCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to threshold");
			else
				return record.getIdleCpuUtilPct() > Double.parseDouble(thresholdValue);

		case "iowait":
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
		case "idle":
			if(record.getIdleCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to threshold");
			else
				return record.getIdleCpuUtilPct() < Double.parseDouble(thresholdValue);

		case "iowait":
			if(record.getIowaitCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SystemCpuRecord to threshold");
			else
				return record.getIowaitCpuUtilPct() < Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	// Moving average of all the variables in a record
	@Override
	public SystemCpuRecord movingAverage(List<SystemCpuRecord> records) throws Exception {
		//This code presumes, but does not check that, the records in the list are sorted by timestamp.
		//If the list is not sorted then the results of this method are likely to be inaccurate.
		return new SystemCpuRecord(records.get(0), records.get(records.size() - 1));
	}

	@Override
	public SystemCpuRecord movingAverage(SystemCpuRecord rec1,
			SystemCpuRecord rec2) throws Exception{
		return new SystemCpuRecord(rec1, rec2);
	}

}
