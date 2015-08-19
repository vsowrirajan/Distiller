package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;

import java.math.BigInteger;

public class DifferentialValueRecordProcessor implements RecordProcessor<Record>{

	@Override
	public String getName(){
		return "DifferentialValueRecordProcessor";
	}

	@Override
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		DifferentialValueRecord currentRecord = (DifferentialValueRecord) record;
		if(!currentRecord.getValueName().equals(metric))
			throw new Exception("Requested metric " + metric + " does not match the metric used to generate this DifferentialValueRecord: " + currentRecord.getValueName());
		switch (currentRecord.getValueType()) {
		case "BigInteger":
			return ((BigInteger)currentRecord.getValue()).equals(new BigInteger(thresholdValue));
		case "double":
			return ((double)currentRecord.getValue()) == Double.parseDouble(thresholdValue);
		case "int":
			return ((long)currentRecord.getValue()) == Long.parseLong(thresholdValue);
		case "long":
			return ((int)currentRecord.getValue()) == Integer.parseInt(thresholdValue);
			
		default:
			throw new Exception("Value type " + currentRecord.getValueType()
					+ " is not Thresholdable in DifferentialValue");
		}
	}

	@Override
	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		
		DifferentialValueRecord currentRecord = (DifferentialValueRecord) record;
		if(!currentRecord.getValueName().equals(metric))
			throw new Exception("Requested metric " + metric + " does not match the metric used to generate this DifferentialValueRecord: " + currentRecord.getValueName());
		switch (currentRecord.getValueType()) {
		case "BigInteger":
			BigInteger bC = null;
			if( ((BigInteger)currentRecord.getValue()).compareTo(new BigInteger("0")) == -1 )
				bC = ((BigInteger)currentRecord.getValue()).divide(new BigInteger("-1"));
			else
				bC = (BigInteger)currentRecord.getValue();
			return bC.compareTo(new BigInteger(thresholdValue)) == 1;
		case "double":
			double dC;
			if( ((double)currentRecord.getValue()) < 0d )
				dC = ((double)currentRecord.getValue()) / -1d;
			else
				dC = (double)currentRecord.getValue();
			return dC > Double.parseDouble(thresholdValue);
		case "int":
			int iC;
			if( ((int)currentRecord.getValue()) < 0d )
				iC = ((int)currentRecord.getValue()) / -1;
			else
				iC = (int)currentRecord.getValue();
			return iC > Integer.parseInt(thresholdValue);
		case "long":
			long lC;
			if( ((long)currentRecord.getValue()) < 0d )
				lC = ((long)currentRecord.getValue()) / -1l;
			else
				lC = (long)currentRecord.getValue();
			return lC > Long.parseLong(thresholdValue);
			
		default:
			throw new Exception("Value type " + currentRecord.getValueType()
					+ " is not Thresholdable in DifferentialValue");
		}

	}
	
	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		DifferentialValueRecord currentRecord = (DifferentialValueRecord) record;
		if(!currentRecord.getValueName().equals(metric))
			throw new Exception("Requested metric " + metric + " does not match the metric used to generate this DifferentialValueRecord: " + currentRecord.getValueName());
		switch (currentRecord.getValueType()) {
		case "BigInteger":
			BigInteger bC = null;
			if( ((BigInteger)currentRecord.getValue()).compareTo(new BigInteger("0")) == -1 )
				bC = ((BigInteger)currentRecord.getValue()).divide(new BigInteger("-1"));
			else
				bC = (BigInteger)currentRecord.getValue();
			return bC.compareTo(new BigInteger(thresholdValue)) == -1;
		case "double":
			double dC;
			if( ((double)currentRecord.getValue()) < 0d )
				dC = ((double)currentRecord.getValue()) / -1d;
			else
				dC = (double)currentRecord.getValue();
			return dC < Double.parseDouble(thresholdValue);
		case "int":
			int iC;
			if( ((int)currentRecord.getValue()) < 0d )
				iC = ((int)currentRecord.getValue()) / -1;
			else
				iC = (int)currentRecord.getValue();
			return iC < Integer.parseInt(thresholdValue);
		case "long":
			long lC;
			if( ((long)currentRecord.getValue()) < 0d )
				lC = ((long)currentRecord.getValue()) / -1l;
			else
				lC = (long)currentRecord.getValue();
			return lC < Long.parseLong(thresholdValue);
			
		default:
			throw new Exception("Value type " + currentRecord.getValueType()
					+ " is not Thresholdable in DifferentialValue");
		}
	}

	@Override
	public DifferentialValueRecord merge(Record rec1, Record rec2)
			throws Exception {
		throw new Exception("merge is not implemented in DifferentialValueRecordProcessor");
	}
	
	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric) throws Exception {
		throw new Exception("diff is not implemented in DifferentialValueRecordProcessor");
	}
}
