package com.mapr.distiller.server.queues;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.Iterator;

import com.mapr.distiller.server.recordtypes.Record;

public class SingleConsumerRecordQueue implements RecordQueue {
	protected String id;
	private int maxQueueLength;

	private static Object lock = new Object();
	LinkedBlockingQueue<Record> consumerRecordQueue = null;
	boolean debugEnabled = false;
	String consumer = null, producer = null;
	
	public SingleConsumerRecordQueue () {
		
	}

	public SingleConsumerRecordQueue(String id, int maxQueueLength) {
		this.id = id + ":RecQ";
		this.consumerRecordQueue = new LinkedBlockingQueue<Record>(
				maxQueueLength);
		this.maxQueueLength = maxQueueLength;
	}

	public String[] listProducers() {
		return new String[] { producer };
	}

	public String[] listConsumers() {
		return new String[] { consumer };
	}

	public boolean subscribe(String subscriber) {
		if (consumer != null || subscriber.equals("")) {
			return false;
		}
		consumer = subscriber;
		return true;
	}

	public void unsubscribe(String unsubscriber) {
		if (consumer != null && consumer.equals(unsubscriber)) {
			consumer = null;
		}
	}

	public boolean registerProducer(String registrant) {
		if (producer != null || registrant.equals("")) {
			return false;
		}
		producer = registrant;
		return true;
	}

	public void unregisterProducer(String unregistrant) {
		if (producer != null && producer.equals(unregistrant)) {
			producer = null;
		}
	}

	public void setDebug(boolean debug) {
		debugEnabled = debug;
	}

	public int queueSize() {
		synchronized (lock) {
			return consumerRecordQueue.size();
		}
	}

	public String printRecords() {
		String records = "";
		synchronized (lock) {
			if (consumerRecordQueue.size() > 100) {
				System.err
						.println("Request to print all records will limit results to 100 as queue size is "
								+ consumerRecordQueue.size());
				return printNewestRecords(100);
			}
			Iterator<Record> i = consumerRecordQueue.iterator();
			while (i.hasNext()) {
				records = records + i.next().toString() + "\n";
			}
		}
		return records;
	}

	public String printNewestRecords(int numRecords) {
		String records = "";
		synchronized (lock) {
			if (numRecords > consumerRecordQueue.size()) {
				numRecords = consumerRecordQueue.size();
			}
			Iterator<Record> i = consumerRecordQueue.iterator();
			for (int x = 0; x < (consumerRecordQueue.size() - numRecords); x++) {
				i.next();
			}
			for (int x = 0; x < numRecords; x++) {
				records = records + i.next().toString() + "\n";
			}
		}
		return records;
	}

	public Record[] dumpAllRecords() {
		synchronized (lock) {
			Record[] outputRecords = new Record[consumerRecordQueue.size()];
			Iterator<Record> i = consumerRecordQueue.iterator();
			int x = 0;
			while (i.hasNext()) {
				outputRecords[x] = i.next();
				x++;
			}
			return outputRecords;
		}
	}

	public Record[] dumpOldestRecords(int numRecords) {
		synchronized (lock) {
			if (numRecords > consumerRecordQueue.size()) {
				numRecords = consumerRecordQueue.size();
			}
			Record[] outputRecords = new Record[numRecords];
			Iterator<Record> i = consumerRecordQueue.iterator();
			for (int x = 0; x < numRecords; x++) {
				outputRecords[x] = i.next();
			}
			return outputRecords;
		}
	}

	public Record[] dumpNewestRecords(int numRecords) {
		synchronized (lock) {
			if (numRecords > consumerRecordQueue.size()) {
				numRecords = consumerRecordQueue.size();
			}
			Record[] outputRecords = new Record[numRecords];
			Iterator<Record> i = consumerRecordQueue.iterator();
			for (int x = 0; x < (consumerRecordQueue.size() - numRecords); x++) {
				i.next();
			}
			for (int x = 0; x < numRecords; x++) {
				outputRecords[x] = i.next();
			}
			return outputRecords;
		}
	}

	public Record[] dumpRecordsFromTimeRange(long startTime, long endTime) {
		synchronized (lock) {
			Iterator<Record> iterator = consumerRecordQueue.iterator();
			boolean foundStart = true, foundEnd = true;
			int startPos = -1, endPos = -1;
			for (int i = 0; i < consumerRecordQueue.size(); i++) {
				Record record = iterator.next();
				long currentTimeStamp = record.getTimestamp();
				if (!foundStart && currentTimeStamp > startTime
						&& currentTimeStamp <= endTime) {
					foundStart = true;
					startPos = i;
				} else if (foundStart && currentTimeStamp > endTime) {
					foundEnd = true;
					endPos = i - 1;
					break;
				}
			}
			if (foundStart && !foundEnd) {
				endPos = consumerRecordQueue.size() - 1;
			}
			if (foundStart) {
				Record[] outputRecords = new Record[endPos - startPos + 1];
				iterator = consumerRecordQueue.iterator();
				for (int x = 0; x < startPos; x++) {
					iterator.next();
				}
				for (int x = 0; x < (endPos - startPos + 1); x++) {
					outputRecords[x] = iterator.next();
				}
				return outputRecords;
			}
		}
		return null;
	}

	public boolean put(Record record) {
		synchronized (lock) {
			try {
				if (!consumerRecordQueue.offer(record)) {
					return false;
				}
			} catch (Exception e) {
				System.err
						.println("DEBUG: "
								+ id
								+ ": Caught an exception attempting to insert record to queue");
				e.printStackTrace();
				return false;
			}
			return true;
		}

	}

	public Record get() {
		boolean getComplete = false;
		int sleepTime = 10;
		while (!getComplete) {
			synchronized (lock) {
				if (consumerRecordQueue.size() > 0) {
					try {
						return (Record) consumerRecordQueue.take();
					} catch (Exception e) {
						System.err.println("DEBUG: " + id
								+ ": Failed to take a record from queue");
						// e.printStackTrace();
					}
					System.err
							.println("Failed to take record from queue "
									+ consumerRecordQueue + " with size "
									+ consumerRecordQueue.size()
									+ " so returning null");
					return null;
				}
			}
			if (!getComplete) {
				try {
					Thread.sleep(sleepTime);
				} catch (Exception e) {
				}
				;
				if (sleepTime < 1000)
					sleepTime = sleepTime * 10;
			}
		}
		return null;
	}

	public Record get(String name) {
		return get();
	}

}
