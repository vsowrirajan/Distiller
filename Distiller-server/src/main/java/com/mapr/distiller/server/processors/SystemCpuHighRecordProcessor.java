package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemCpuHighRecord;
import com.mapr.distiller.server.recordtypes.SystemCpuRecord;

public class SystemCpuHighRecordProcessor implements RecordProcessor {

	private double systemCpuHighThreshold;
	private SystemCpuHighRecord systemCpuHighRecord, recordToReturn;
	private double idleTimeTotal = 0d, userTimeTotal = 0d,
			systemTimeTotal = 0d;
	private long durationmsTotal = 0l;

	public SystemCpuHighRecordProcessor(double systemCpuHighThreshold) {
		this.systemCpuHighThreshold = systemCpuHighThreshold;
		this.systemCpuHighRecord = null;
		this.recordToReturn = null;
	}

	public Record process(Record record) throws Exception {
		SystemCpuRecord systemCpuRecord = (SystemCpuRecord) record;
		if (systemCpuHighRecord == null
				&& (100d - systemCpuRecord.getIdle()) >= systemCpuHighThreshold) {
			systemCpuHighRecord = new SystemCpuHighRecord();
			systemCpuHighRecord.setSampleWidth(1);
			systemCpuHighRecord.setPreviousTimestamp(systemCpuRecord
					.getPreviousTimestamp());
			systemCpuHighRecord.setTimestamp(systemCpuRecord.getTimestamp());
			systemCpuHighRecord
					.setSystemCpuHighThreshold(systemCpuHighThreshold);
			idleTimeTotal = systemCpuRecord.getIdle()
					* (double) systemCpuRecord.getDurationms();
			userTimeTotal = systemCpuRecord.getUser()
					* (double) systemCpuRecord.getDurationms();
			systemTimeTotal = systemCpuRecord.getSystem()
					* (double) systemCpuRecord.getDurationms();
			durationmsTotal = systemCpuRecord.getDurationms();
			recordToReturn = null;
		} else if (systemCpuHighRecord == null
				&& (100d - systemCpuRecord.getIdle()) < systemCpuHighThreshold) {
			recordToReturn = null;
		} else if (systemCpuHighRecord != null
				&& (100d - systemCpuRecord.getIdle()) >= systemCpuHighThreshold) {
			systemCpuHighRecord.setSampleWidth(systemCpuHighRecord
					.getSampleWidth() + 1);
			systemCpuHighRecord.setTimestamp(systemCpuRecord.getTimestamp());
			idleTimeTotal += systemCpuRecord.getIdle()
					* (double) systemCpuRecord.getDurationms();
			userTimeTotal += systemCpuRecord.getUser()
					* (double) systemCpuRecord.getDurationms();
			systemTimeTotal += systemCpuRecord.getSystem()
					* (double) systemCpuRecord.getDurationms();
			durationmsTotal += systemCpuRecord.getDurationms();
			recordToReturn = null;
		} else if (systemCpuHighRecord != null
				&& (100d - systemCpuRecord.getIdle()) < systemCpuHighThreshold) {
			systemCpuHighRecord.setIdle(idleTimeTotal
					/ (double) durationmsTotal);
			systemCpuHighRecord.setUser(userTimeTotal
					/ (double) durationmsTotal);
			systemCpuHighRecord.setSystem(systemTimeTotal
					/ (double) durationmsTotal);
			systemCpuHighRecord.setDurationms(durationmsTotal);
			recordToReturn = systemCpuHighRecord;
			systemCpuHighRecord = null;
		}
		return recordToReturn;
	}
}
