package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.ProcessEfficiencyRecord;
import com.mapr.distiller.server.recordtypes.ProcessResourceRecord;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

public class ProcessEfficiencyRecordProcessor implements RecordProcessor<Record> {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(ProcessEfficiencyRecordProcessor.class);

	@Override
	public Record[] mergeChronologicallyConsecutive(Record oldRecord, Record newRecord) throws Exception{
		throw new Exception("not implemented");
	}

	@Override
	public ProcessEfficiencyRecord convert(Record record) throws Exception{
		try {
			return new ProcessEfficiencyRecord((ProcessResourceRecord)record);
		} catch (Exception e){
			throw new Exception("Failed to convert input record to ProcessEfficiencyRecord", e);
		}
	}
	
	@Override
	public Record diff(Record rec1, Record rec2, String metric) throws Exception {
		throw new Exception("Not implemented");
	}	

	public String getName(){
		return Constants.PROCESS_EFFICIENCY_RECORD_PROCESSOR;
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		throw new Exception("Not implemented");
	}

	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {
		throw new Exception("Not implemented");
	}

	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public Record merge(Record rec1, Record rec2)
			throws Exception {
		throw new Exception("Not implemented");
	}

}
