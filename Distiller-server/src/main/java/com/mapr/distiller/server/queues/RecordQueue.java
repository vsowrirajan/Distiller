package com.mapr.distiller.server.queues;

import com.mapr.distiller.server.recordtypes.Record;

public interface RecordQueue {
	public int queueSize();

	public boolean put(Record record);

	public Record get();

	public Record get(String name);

	public String printRecords();

	public String[] listProducers();

	public String[] listConsumers();

	public String printNewestRecords(int numRecords);

	public Record[] dumpNewestRecords(int numRecords);

	public Record[] dumpOldestRecords(int numRecords);

	public Record[] dumpAllRecords();

	public Record[] dumpRecordsFromTimeRange(long startTime,
			long endTime);
}