package com.mapr.distiller.server.coordinators;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.readers.RecordReader;
import com.mapr.distiller.server.recordtypes.Record;

public class RecordProducer extends Thread {
	private RecordReader inputRecords;
	private RecordProcessor recordProcessor;
	private RecordQueue outputRecordQueue;
	private  String id, getName;
	private long recordsRead = 0l, recordsWritten = 0l;
	private boolean shouldStop = false, useGetWithName = false, debugEnabled = false;

	RecordProducer(RecordReader inputRecords, RecordProcessor recordProcessor,
			RecordQueue outputRecordQueue, String id) {
		this.inputRecords = inputRecords;
		this.recordProcessor = recordProcessor;
		this.outputRecordQueue = outputRecordQueue;
		this.id = id + ":RecProd";
		this.useGetWithName = false;
	}

	RecordProducer(RecordReader inputRecords, RecordProcessor recordProcessor,
			RecordQueue outputRecordQueue, String id, String getName) {
		this.inputRecords = inputRecords;
		this.recordProcessor = recordProcessor;
		this.outputRecordQueue = outputRecordQueue;
		this.id = id + ":RecProd";
		this.getName = getName;
		this.useGetWithName = true;
	}

	public void setDebug(boolean state) {
		debugEnabled = state;
	}

	// RecordProducer reads Record's from a RecordReader, asks RecordProcessor
	// to process and generate an output Record
	// If output Record is not null, places the output Record into the output
	// RecordQueue
	public void run() {
		Record outputRecord;
		Record inputRecord;
		if (useGetWithName) {
			try {
				if (debugEnabled)
					System.err.println("calling getRecord for " + getName
							+ " with reader " + inputRecords);
				while ((inputRecord = inputRecords.getRecord(getName)) != null
						&& !shouldStop) {
					if (debugEnabled)
						System.err.println("getRecord returned record "
								+ inputRecord);
					recordsRead++;
					try {
						outputRecord = recordProcessor.process(inputRecord);
					} catch (Exception e) {
						System.err
								.println(id
										+ ": RecordProcessor threw exception while processing inputRecord");
						e.printStackTrace();
						throw e;
					}
					if (outputRecord != null) {
						if (!outputRecordQueue.put(id, outputRecord)) {
							System.err.println(id
									+ ": Failed to put outputRecord "
									+ outputRecord + " into RecordQueue "
									+ outputRecordQueue
									+ ". Dropping outputRecord.");
							outputRecord = null;
						} else {
							recordsWritten++;
						}
					}
				}
				if (debugEnabled)
					System.err.println("terminating with inputRecord "
							+ inputRecord + " shouldStop " + shouldStop);
			} catch (Exception e) {
				System.err.println(id
						+ ": Failed to get a Record from RecordReader.");
				e.printStackTrace();
			}
		} else {
			try {
				if (debugEnabled)
					System.err.println("calling getRecord with reader "
							+ inputRecords);
				while ((inputRecord = inputRecords.getRecord()) != null
						&& !shouldStop) {
					if (debugEnabled)
						System.err.println("getRecord returned record "
								+ inputRecord);

					recordsRead++;
					try {
						outputRecord = recordProcessor.process(inputRecord);
					} catch (Exception e) {
						System.err
								.println(id
										+ ": RecordProcessor threw exception while processing inputRecord");
						e.printStackTrace();
						throw e;
					}
					if (outputRecord != null) {
						if (!outputRecordQueue.put(id, outputRecord)) {
							System.err.println(id
									+ ": Failed to put outputRecord "
									+ outputRecord + " into RecordQueue "
									+ outputRecordQueue
									+ ". Dropping outputRecord.");
							outputRecord = null;
						} else {
							recordsWritten++;
						}
					}
				}
				if (debugEnabled)
					System.err.println("terminating with inputRecord "
							+ inputRecord + " shouldStop " + shouldStop);
			} catch (Exception e) {
				System.err.println(id
						+ ": Failed to get a Record from RecordReader.");
			}
		}
	}

	public void requestStop() {
		shouldStop = true;
	}
}
