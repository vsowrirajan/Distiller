package com.mapr.distiller.server.processors;

import java.util.ArrayList;
import java.util.List;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SystemCpuMovingAverageRecord;
import com.mapr.distiller.server.recordtypes.SystemCpuRecord;

public class SystemCpuMovingAverageRecordProcessor implements RecordProcessor {
	private int movingAverageWidth;
	private List<SystemCpuRecord> recordList;

	public SystemCpuMovingAverageRecordProcessor(int movingAverageWidth) {
		this.movingAverageWidth = movingAverageWidth;
		this.recordList = new ArrayList<SystemCpuRecord>();
	}

	public Record process(Record record) throws Exception {
		if (recordList.size() == movingAverageWidth) {
			recordList.remove(0);
		}

		recordList.add((SystemCpuRecord) record);

		if (recordList.size() == movingAverageWidth) {
			SystemCpuMovingAverageRecord systemCpuMovingAverageRecord = new SystemCpuMovingAverageRecord();
			systemCpuMovingAverageRecord
					.setMovingAverageSampleWidth(movingAverageWidth);
			systemCpuMovingAverageRecord.setMovingAverageStartTime(recordList
					.get(0).getPreviousTimestamp());
			systemCpuMovingAverageRecord.setPreviousTimestamp(record
					.getPreviousTimestamp());
			systemCpuMovingAverageRecord.setTimestamp(record.getTimestamp());
			systemCpuMovingAverageRecord.setDurationms(record.getDurationms());
			systemCpuMovingAverageRecord.setMovingAverageDurationms(record
					.getTimestamp()
					- systemCpuMovingAverageRecord.getMovingAverageStartTime());

			double idleTotal = 0d, userTotal = 0d, systemTotal = 0d;

			for (SystemCpuRecord systemCpuRecord : recordList) {
				double currentDurationms = systemCpuRecord.getDurationms();
				idleTotal += systemCpuRecord.getIdle() * currentDurationms;
				userTotal += systemCpuRecord.getUser() * currentDurationms;
				systemTotal += systemCpuRecord.getSystem() * currentDurationms;
			}

			double currentMovingAvgDurationms = systemCpuMovingAverageRecord
					.getMovingAverageDurationms();
			systemCpuMovingAverageRecord.setIdle(idleTotal
					/ currentMovingAvgDurationms);
			systemCpuMovingAverageRecord.setUser(userTotal
					/ currentMovingAvgDurationms);
			systemCpuMovingAverageRecord.setSystem(systemTotal
					/ currentMovingAvgDurationms);
			return systemCpuMovingAverageRecord;
		}
		return null;
	}

}
