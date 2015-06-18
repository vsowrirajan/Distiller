package com.mapr.distiller.server.readers;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.recordtypes.Record;

public class PassthroughRecordReader implements RecordReader {

	private RecordQueue recordQueue;
	private boolean debugEnabled = false;

	public PassthroughRecordReader(RecordQueue queue) {
		this.recordQueue = queue;
	}

	public void setDebug(boolean debug) {
		debugEnabled = debug;
	}

	public Record getRecord() {
		if (debugEnabled) {
			System.err.println("Calling get for queue " + recordQueue);
		}

		Record record = recordQueue.get();

		if (debugEnabled) {
			System.err.println("Get for queue " + recordQueue
					+ " returned record " + record);
		}
		
		return record;
	}

	public Record getRecord(String name) {
		return recordQueue.get(name);
	}
}
