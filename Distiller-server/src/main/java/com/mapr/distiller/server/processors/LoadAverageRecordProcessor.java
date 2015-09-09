package com.mapr.distiller.server.processors;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;
import com.mapr.distiller.server.recordtypes.LoadAverageRecord;
import com.mapr.distiller.server.utils.Constants;

public class LoadAverageRecordProcessor implements RecordProcessor<Record> {
	
	//private static final Logger LOG = LoggerFactory
	//		.getLogger(LoadAverageRecordProcessor.class);

	@Override
	public Record[] mergeChronologicallyConsecutive(Record oldRecord, Record newRecord) throws Exception{
		Record[] ret = new Record[2];
		if(oldRecord.getTimestamp() == newRecord.getPreviousTimestamp()){
			ret[0] = new LoadAverageRecord((LoadAverageRecord)oldRecord, (LoadAverageRecord)newRecord);
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
	
	public String getName(){
		return Constants.LOAD_AVERAGE_RECORD_PROCESSOR;
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {
		LoadAverageRecord currentRecord = (LoadAverageRecord) record;

		switch (metric) {
		case "loadAverage1Min":
			return currentRecord.getLoadAverage1Min() == Double.parseDouble(thresholdValue);

		case "loadAverage5Min":
			return currentRecord.getLoadAverage5Min() == Double.parseDouble(thresholdValue);

		case "loadAverage15Min":
			return currentRecord.getLoadAverage15Min() == Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable");
		}
	}

	@Override
	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		LoadAverageRecord currentRecord = (LoadAverageRecord) record;

		switch (metric) {
		case "loadAverage1Min":
			return currentRecord.getLoadAverage1Min() > Double.parseDouble(thresholdValue);

		case "loadAverage5Min":
			return currentRecord.getLoadAverage5Min() > Double.parseDouble(thresholdValue);

		case "loadAverage15Min":
			return currentRecord.getLoadAverage15Min() > Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		LoadAverageRecord currentRecord = (LoadAverageRecord) record;

		switch (metric) {
		case "loadAverage1Min":
			return currentRecord.getLoadAverage1Min() < Double.parseDouble(thresholdValue);

		case "loadAverage5Min":
			return currentRecord.getLoadAverage5Min() < Double.parseDouble(thresholdValue);

		case "loadAverage15Min":
			return currentRecord.getLoadAverage15Min() < Double.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable");
		}
	}

	@Override
	public LoadAverageRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new LoadAverageRecord((LoadAverageRecord) rec1,
				(LoadAverageRecord) rec2);
	}

	
	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric) throws Exception {
		LoadAverageRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = (LoadAverageRecord)rec1;
			newRecord = (LoadAverageRecord)rec2;
		} else {
			oldRecord = (LoadAverageRecord)rec2;
			newRecord = (LoadAverageRecord)rec1;
		}
	
		switch (metric) {
		case "loadAverage1Min":
			return new DifferentialValueRecord( -1,
												oldRecord.getTimestamp(),
												-1,
												newRecord.getTimestamp(),
												Constants.LOAD_AVERAGE_RECORD,
												metric,
												"double",
												newRecord.getLoadAverage1Min() - oldRecord.getLoadAverage1Min() );
			
		case "loadAverage5Min":
			return new DifferentialValueRecord( -1,
												oldRecord.getTimestamp(),
												-1,
												newRecord.getTimestamp(),
												Constants.LOAD_AVERAGE_RECORD,
												metric,
												"double",
												newRecord.getLoadAverage5Min() - oldRecord.getLoadAverage5Min() );
			
		case "loadAverage15Min":
			return new DifferentialValueRecord( -1,
												oldRecord.getTimestamp(),
												-1,
												newRecord.getTimestamp(),
												Constants.LOAD_AVERAGE_RECORD,
												metric,
												"double",
												newRecord.getLoadAverage15Min() - oldRecord.getLoadAverage15Min() );
			
		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable");
		}
	}

}
