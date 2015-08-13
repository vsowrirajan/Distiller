package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;

public class SystemMemoryRecordProcessor implements RecordProcessor<Record> {

	public String getName(){
		return "SystemMemoryRecordProcessor";
	}
	
	public boolean isNotEqual(SystemMemoryRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {
		SystemMemoryRecord currentRecord = (SystemMemoryRecord) record;

		switch (metric) {
		case "%free":
			if (currentRecord.getFreeMemPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemMemoryRecord to value");
			else
				return currentRecord.getFreeMemPct() == Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemMemoryRecord");
		}
	}

	@Override
	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		SystemMemoryRecord currentRecord = (SystemMemoryRecord) record;

		switch (metric) {
		case "%free":
			if (currentRecord.getFreeMemPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemMemoryRecord to threshold");
			else
				return currentRecord.getFreeMemPct() > Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemMemoryRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		SystemMemoryRecord currentRecord = (SystemMemoryRecord) record;
		
		switch (metric) {
		case "%free":
			if (currentRecord.getFreeMemPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemMemoryRecord to threshold");
			else
				return currentRecord.getFreeMemPct() < Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemMemoryRecord");
		}
	}

	@Override
	public SystemMemoryRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new SystemMemoryRecord((SystemMemoryRecord) rec1,
				(SystemMemoryRecord) rec2);
	}

	@Override
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		throw new UnsupportedOperationException();
	}
}
