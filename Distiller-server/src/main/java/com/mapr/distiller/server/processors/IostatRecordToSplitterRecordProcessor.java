package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DeviceUsageRecord;
import com.mapr.distiller.server.recordtypes.IoStatRecord;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SplitterRecord;
import com.mapr.distiller.server.recordtypes.SystemCpuRecord;

public class IostatRecordToSplitterRecordProcessor implements RecordProcessor {

	public IostatRecordToSplitterRecordProcessor() {
	}

	public Record process(Record record) throws Exception {
		return process((IoStatRecord) record);
	}

	public SplitterRecord process(IoStatRecord inputRecord) throws Exception {
		if (inputRecord.getType().equals("cpu")) {
			SystemCpuRecord outputRecord = new SystemCpuRecord();
			outputRecord.setTimestamp(inputRecord.getTimestamp());
			outputRecord.setPreviousTimestamp(inputRecord
					.getPreviousTimestamp());
			outputRecord.setDurationms(inputRecord.getDurationms());
			outputRecord.setUser(inputRecord.getCpu().getUser());
			outputRecord.setSystem(inputRecord.getCpu().getSystem());
			outputRecord.setIdle(inputRecord.getCpu().getIdle());
			SplitterRecord splitterRecord = new SplitterRecord();
			splitterRecord.setType("systemCpu");
			splitterRecord.setRecord(outputRecord);
			return splitterRecord;
		} else if (inputRecord.getType().equals("device")) {
			DeviceUsageRecord outputRecord = new DeviceUsageRecord();
			outputRecord.setTimestamp(inputRecord.getTimestamp());
			outputRecord.setPreviousTimestamp(inputRecord
					.getPreviousTimestamp());
			outputRecord.setDurationms(inputRecord.getDurationms());
			outputRecord.setName(inputRecord.getDevice().getName());
			outputRecord.setNumReadReqs(inputRecord.getDevice()
					.getNumReadReqs());
			outputRecord.setNumWriteReqs(inputRecord.getDevice()
					.getNumWriteReqs());
			outputRecord.setAvgReadSizeBytes(inputRecord.getDevice()
					.getAvgReadSizeBytes());
			outputRecord.setAvgWriteSizeBytes(inputRecord.getDevice()
					.getAvgWriteSizeBytes());
			outputRecord.setReadMB(inputRecord.getDevice().getReadMB());
			outputRecord.setWriteMB(inputRecord.getDevice().getWriteMB());
			outputRecord.setAvgWaitms(inputRecord.getDevice().getAvgWaitms());
			outputRecord.setUtilizationPct(inputRecord.getDevice()
					.getUtilizationPct());
			SplitterRecord splitterRecord = new SplitterRecord();
			splitterRecord.setType("deviceUsage");
			splitterRecord.setRecord(outputRecord);
			return splitterRecord;
		} else {
			System.err.println("IostatRecord has unknown type: "
					+ inputRecord.getType() + ", returning null.");
		}
		return null;
	}
}
