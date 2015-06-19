package com.mapr.distiller.server.queues;

import java.util.HashMap;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.SplitterRecord;

public class SplitterRecordQueue extends SingleConsumerRecordQueue {
	private HashMap<String, RecordQueue> metricNameToRecordQueueMap;

	public SplitterRecordQueue(HashMap<String, RecordQueue> metricNameToRecordQueueMap) {
		this.metricNameToRecordQueueMap = metricNameToRecordQueueMap;
	}

	public boolean put(Record record) {
		return put((SplitterRecord) record);
	}

	public boolean put(SplitterRecord record) {
		try {
			RecordQueue outputQueue = metricNameToRecordQueueMap.get(record
					.getType());
			if (outputQueue != null) {
				if (!outputQueue.put(record.getRecord())) {
					return false;
				}
			} else {
				System.err
						.println("Could not find RecordQueue that maps to SplitterRecord type "
								+ record.getType());
				return false;
			}
		} catch (Exception e) {
			System.err
					.println("DEBUG: "
							+ id
							+ ": Caught an exception attempting to insert child record from SplitterRecord type "
							+ record.getType() + " to queue");
			// e.printStackTrace();
			return false;
		}
		return true;
	}

	public int queueSize() {
		return 0;
	}

	public Record get() {
		return null;
	}

	public Record get(String name) {
		return null;
	}

	public String printRecords() {
		return null;
	}

	public String printNewestRecords(int numRecords) {
		return null;
	}

	public Record[] dumpNewestRecords(int numRecords) {
		return null;
	}

	public Record[] dumpOldestRecords(int numRecords) {
		return null;
	}

	public Record[] dumpAllRecords() {
		return null;
	}

	public Record[] dumpRecordsFromTimeRange(long startTime, long endTime) {
		return null;
	}

}
