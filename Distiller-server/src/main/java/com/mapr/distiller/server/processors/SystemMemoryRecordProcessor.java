package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;
import com.mapr.distiller.server.utils.Constants;
import java.math.BigInteger;

public class SystemMemoryRecordProcessor implements RecordProcessor<Record> {

	public String getName(){
		return Constants.SYSTEM_MEMORY_RECORD_PROCESSOR;
	}

	@Override
	public boolean isNotEqual(Record record, String metric,
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

		case "pswpin":
			return currentRecord.get_pswpin().equals(new BigInteger(thresholdValue));
				
		case "pswpout":
			return currentRecord.get_pswpout().equals(new BigInteger(thresholdValue));
				
		case "allocstall":
			return currentRecord.get_allocstall().equals(new BigInteger(thresholdValue));
				
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
		
		case "pswpin":
			return currentRecord.get_pswpin().compareTo(new BigInteger(thresholdValue)) == 1;
				
		case "pswpout":
			return currentRecord.get_pswpout().compareTo(new BigInteger(thresholdValue)) == 1;
				
		case "allocstall":
			return currentRecord.get_allocstall().compareTo(new BigInteger(thresholdValue)) == 1;
				
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

		case "pswpin":
			return currentRecord.get_pswpin().compareTo(new BigInteger(thresholdValue)) == -1;
				
		case "pswpout":
			return currentRecord.get_pswpout().compareTo(new BigInteger(thresholdValue)) == -1;
				
		case "allocstall":
			return currentRecord.get_allocstall().compareTo(new BigInteger(thresholdValue)) == -1;
				
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

}
