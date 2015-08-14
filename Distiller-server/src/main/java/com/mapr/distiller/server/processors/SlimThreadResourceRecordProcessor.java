package com.mapr.distiller.server.processors;

import java.math.BigInteger;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SlimThreadResourceRecord;
import com.mapr.distiller.server.utils.Constants;

public class SlimThreadResourceRecordProcessor implements RecordProcessor<Record> {
	public String getName(){
		return Constants.SLIM_THREAD_RESOURCE_RECORD_PROCESSOR;
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {
		SlimThreadResourceRecord currentRecord = (SlimThreadResourceRecord) record;

		switch (metric) {
		case "commandName":
			return currentRecord.getCommandName().equals(thresholdValue);

		case "pid":
			return currentRecord.getPid() == Integer.parseInt(thresholdValue);
			
		case "ppid":
			return currentRecord.getPpid() == Integer.parseInt(thresholdValue);
			
		case "startTime":
			return currentRecord.getStartTime() == Long.parseLong(thresholdValue);

		case "ioCallRate":
			return currentRecord.getIoCallRate() == Double.parseDouble(thresholdValue);
			
		case "ioBytesRead":
			return currentRecord.getIoBytesRead().equals(new BigInteger(thresholdValue));
			
		case "ioBytesWritten":
			return currentRecord.getIoBytesWritten().equals(new BigInteger(thresholdValue));
			
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SlimThreadResourceRecord for " + Constants.INPUT_RECORD_PROCESSOR_METHOD + "=" + Constants.IS_EQUAL);
		}
	}

	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		SlimThreadResourceRecord currentRecord = (SlimThreadResourceRecord) record;

		switch (metric) {
		case "idlePct":
			if(currentRecord.getCpuUtilPct() == -1d || currentRecord.getIowaitUtilPct() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return 1d - currentRecord.getCpuUtilPct() - currentRecord.getIowaitUtilPct() > Double.parseDouble(thresholdValue);
				
		case "cpuUtilPct":
			if(currentRecord.getCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getCpuUtilPct() > Double.parseDouble(thresholdValue);
		
		case "iowaitUtilPct":
			if(currentRecord.getIowaitUtilPct() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getIowaitUtilPct() > Double.parseDouble(thresholdValue);
		
		case "ioCallRate":
			if(currentRecord.getIoCallRate() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getIoCallRate() > Double.parseDouble(thresholdValue);
		
		case "readIoByteRate":
			if(currentRecord.getReadIoByteRate() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getReadIoByteRate() > Double.parseDouble(thresholdValue);
		
		case "writeIoByteRate":
			if(currentRecord.getWriteIoByteRate() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getWriteIoByteRate() > Double.parseDouble(thresholdValue);
		
		case "iowaitTicks":
			return currentRecord.getIowaitTicks().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "cpuUsageTicks":
			return currentRecord.getCpuUsageTicks().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "ioCalls":
			return currentRecord.getIoCalls().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "ioBytesRead":
			return currentRecord.getIoBytesRead().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "ioBytesWritten":
			return currentRecord.getIoBytesWritten().compareTo(new BigInteger(thresholdValue)) == 1;
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SlimThreadResourceRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		SlimThreadResourceRecord currentRecord = (SlimThreadResourceRecord) record;

		switch (metric) {
		case "idlePct":
			if(currentRecord.getCpuUtilPct() == -1d || currentRecord.getIowaitUtilPct() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return 1d - currentRecord.getCpuUtilPct() - currentRecord.getIowaitUtilPct() < Double.parseDouble(thresholdValue);
				
		case "cpuUtilPct":
			if(currentRecord.getCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getCpuUtilPct() < Double.parseDouble(thresholdValue);
		
		case "iowaitUtilPct":
			if(currentRecord.getIowaitUtilPct() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getIowaitUtilPct() < Double.parseDouble(thresholdValue);
		
		case "ioCallRate":
			if(currentRecord.getIoCallRate() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getIoCallRate() < Double.parseDouble(thresholdValue);
		
		case "readIoByteRate":
			if(currentRecord.getReadIoByteRate() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getReadIoByteRate() < Double.parseDouble(thresholdValue);
		
		case "writeIoByteRate":
			if(currentRecord.getWriteIoByteRate() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return currentRecord.getWriteIoByteRate() < Double.parseDouble(thresholdValue);
		
		case "iowaitTicks":
			return currentRecord.getIowaitTicks().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "cpuUsageTicks":
			return currentRecord.getCpuUsageTicks().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "ioCalls":
			return currentRecord.getIoCalls().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "ioBytesRead":
			return currentRecord.getIoBytesRead().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "ioBytesWritten":
			return currentRecord.getIoBytesWritten().compareTo(new BigInteger(thresholdValue)) == -1;
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SlimThreadResourceRecord");
		}
	}

	@Override
	public SlimThreadResourceRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new SlimThreadResourceRecord((SlimThreadResourceRecord) rec1,
				(SlimThreadResourceRecord) rec2);
	}
}
