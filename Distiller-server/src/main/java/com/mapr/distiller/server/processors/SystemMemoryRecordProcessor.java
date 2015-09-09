package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;
import com.mapr.distiller.server.utils.Constants;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemMemoryRecordProcessor implements RecordProcessor<Record> {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(SystemMemoryRecordProcessor.class);

	@Override
	public Record[] mergeChronologicallyConsecutive(Record oldRecord, Record newRecord) throws Exception{
		Record[] ret = new Record[2];
		if(oldRecord.getTimestamp() == newRecord.getPreviousTimestamp()){
			ret[0] = new SystemMemoryRecord((SystemMemoryRecord)oldRecord, (SystemMemoryRecord)newRecord);
			ret[1] = null;
		} else {
			ret[0] = newRecord;
			ret[1] = oldRecord;
		}
		return ret;
	}

	@Override
	public Record convert(Record record) throws Exception{
		throw new Exception("Not implemented");
	}
	
	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric) throws Exception {
		if( rec1.getPreviousTimestamp()==-1l ||
			rec2.getPreviousTimestamp()==-1l )
			throw new Exception("SystemMemoryRecords can only be diff'd from non-raw SystemMemoryRecords");
			
		SystemMemoryRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = (SystemMemoryRecord)rec1;
			newRecord = (SystemMemoryRecord)rec2;
		} else {
			oldRecord = (SystemMemoryRecord)rec2;
			newRecord = (SystemMemoryRecord)rec1;
		}
		
		if(oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception("Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");

		switch (metric) {
		case "pswpin":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_pswpin().subtract(oldRecord.get_pswpin()) );


		case "pswpout":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_pswpout().subtract(oldRecord.get_pswpout()) );


		case "allocstall":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_allocstall().subtract(oldRecord.get_allocstall()) );

		case "%free":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												oldRecord.getTimestamp(),
												newRecord.getPreviousTimestamp(),
												newRecord.getTimestamp(),
												getName(),
												metric,
												"double",
												newRecord.getFreeMemPct() - oldRecord.getFreeMemPct() );
	
		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable in SystemMemoryRecordProcessor");
		}
	}
	
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
