package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;

public class SystemMemoryRecordProcessor implements
		Thresholdable<SystemMemoryRecord>, MovingAverageable<SystemMemoryRecord> {
	@Override
	public boolean isEqual(SystemMemoryRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "%free":
			if(record.getFreeMemPct() == -1d)
				throw new Exception("Can not compare raw SystemMemoryRecord to value");
			else
				return record.getFreeMemPct() == Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemMemoryRecord");
		}
	}
	
	@Override
	public boolean isAboveThreshold(SystemMemoryRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "%free":
			if(record.getFreeMemPct() == -1d)
				throw new Exception("Can not compare raw SystemMemoryRecord to threshold");
			else
				return record.getFreeMemPct() > Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemMemoryRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(SystemMemoryRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "%free":
			if(record.getFreeMemPct() == -1d)
				throw new Exception("Can not compare raw SystemMemoryRecord to threshold");
			else
				return record.getFreeMemPct() < Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemMemoryRecord");
		}
	}

	@Override
	public SystemMemoryRecord movingAverage(SystemMemoryRecord rec1,
			SystemMemoryRecord rec2) throws Exception{
		return new SystemMemoryRecord(rec1, rec2);
	}
}
