package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SlimProcessResourceRecord;
import com.mapr.distiller.server.utils.Constants;

public class PassthroughRecordProcessor implements RecordProcessor<Record> {

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric) throws Exception {
		throw new Exception("Not implemented.");
	}	

	
	public String getName(){
		return Constants.PASSTHROUGH_RECORD_PROCESSOR;
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		throw new Exception("Not implemented.");
	}

	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {
		throw new Exception("Not implemented.");
	}

	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		throw new Exception("Not implemented.");
	}
	
	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		throw new Exception("Not implemented.");
	}

	@Override
	public SlimProcessResourceRecord merge(Record rec1, Record rec2)
			throws Exception {
		throw new Exception("Not implemented.");
	}
}
