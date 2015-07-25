package com.mapr.distiller.server.queues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.List;

import com.mapr.distiller.server.recordtypes.Record;

public class SubscriptionRecordQueue implements RecordQueue {
	protected String id;
	private int maxQueueLength;

	private List<Record> subscriptionRecordQueue;
	private ConcurrentHashMap<String, Integer> consumers, producers;
	private static Object lock = new Object();
	private static Object valueAdded = new Object();

	public SubscriptionRecordQueue(String id, int maxQueueLength) {
		this.consumers = new ConcurrentHashMap<String, Integer>(10, 0.75f, 4);
		this.producers = new ConcurrentHashMap<String, Integer>(10, 0.75f, 4);
		this.subscriptionRecordQueue = Collections
				.synchronizedList(new ArrayList<Record>(maxQueueLength));
		this.maxQueueLength = maxQueueLength;
		this.id = id;
	}
	
	public int maxQueueSize(){
		return maxQueueLength;
	}
	public String getQueueName(){
		return id;
	}
	
	public boolean hasConsumer(String name){
		return consumers.containsKey(name);
	}
	
	public boolean hasProducer(String name){
		return producers.containsKey(name);
	}

	public boolean registerConsumer(String consumer) {
		if (consumer == null || consumer.equals("")) {
			System.err
					.println("ERROR: Subscription request received for null subscriber name.");
			return false;
		}
		synchronized (lock) {
			if (!consumers.containsKey(consumer)) {
				consumers.put(consumer, new Integer(0));
				return true;
			} else {
				System.err
						.println("ERROR: Duplicate subscription request received for "
								+ consumer);
			}
		}
		return false;
	}

	public boolean registerProducer(String producer) {
		if (producer == null || producer.equals("")) {
			System.err
					.println("ERROR: Producer registration request received for null producer name");
			return false;
		}
		synchronized (lock) {
			if (!producers.containsKey(producer)) {
				producers.put(producer, new Integer(0));
				return true;
			} else {
				System.err
						.println("ERROR: Duplicate producer registration request received for "
								+ producer);
			}
		}
		return false;
	}

	public boolean unregisterProducer(String producer) {
		synchronized (lock) {
			if(!producers.containsKey(producer))
				return false;
			producers.remove(producer);
			return true;
		}
	}

	public boolean unregisterConsumer(String consumer) {
		synchronized (lock) {
			if(!consumers.containsKey(consumer))
				return false;
			consumers.remove(consumer);
			return true;
		}
	}

	public int queueSize() {
		return subscriptionRecordQueue.size();
	}

	public String[] listProducers() {
		synchronized (lock) {
			String[] ret = new String[producers.size()];
			Iterator<Map.Entry<String, Integer>> i = producers.entrySet()
					.iterator();
			for (int x = 0; x < producers.size(); x++) {
				Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) i
						.next();
				ret[x] = pair.getKey();

			}
			return ret;
		}
	}

	public String[] listConsumers() {
		synchronized (lock) {
			String[] ret = new String[consumers.size()];
			Iterator<Map.Entry<String, Integer>> i = consumers.entrySet()
					.iterator();
			for (int x = 0; x < consumers.size(); x++) {
				Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) i
						.next();
				ret[x] = pair.getKey();

			}
			return ret;
		}
	}

	public String printRecords(String consumerName) {
		String records = "";
		synchronized (lock) {
			if (subscriptionRecordQueue.size() > 100) {
				System.err
						.println("Request to print all records will limit results to 100 as queue size is "
								+ subscriptionRecordQueue.size());
				return printNewestRecords(consumerName, 100);
			}
			ListIterator<Record> i = subscriptionRecordQueue.listIterator();
			while (i.hasNext()) {
				records = records + i.next().toString() + "\n";
			}
		}
		return records;
	}

	public String printNewestRecords(String consumerName, int numRecords) {
		String records = "";
		synchronized (lock) {
			if (numRecords > subscriptionRecordQueue.size()) {
				numRecords = subscriptionRecordQueue.size();
			}
			int consumerPosition = consumers.get(consumerName).intValue();
			if(subscriptionRecordQueue.size() - consumerPosition > numRecords)
				numRecords = subscriptionRecordQueue.size() - consumerPosition;
			ListIterator<Record> i = subscriptionRecordQueue
					.listIterator(subscriptionRecordQueue.size() - numRecords);
			for (int x = 0; x < numRecords; x++) {
				records = records + i.next().toString() + "\n";
			}
		}
		return records;
	}

	public Record[] dumpNewestRecords(int numRecords) {
		synchronized (lock) {
			if (subscriptionRecordQueue.size() < numRecords) {
				numRecords = subscriptionRecordQueue.size();
			}
			Record[] outputRecords = new Record[numRecords];
			ListIterator<Record> i = subscriptionRecordQueue
					.listIterator(subscriptionRecordQueue.size() - numRecords);
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
			if (subscriptionRecordQueue.size() < numRecords) {
				numRecords = subscriptionRecordQueue.size();
			}
			Record[] outputRecords = new Record[numRecords];
			ListIterator<Record> i = subscriptionRecordQueue.listIterator();
			for (int x = 0; x < numRecords; x++) {
				outputRecords[x] = i.next();
			}
			return outputRecords;
		}
	}

	public Record[] dumpAllRecords() {
		synchronized (lock) {
			Record[] outputRecords = new Record[subscriptionRecordQueue.size()];
			ListIterator<Record> i = subscriptionRecordQueue.listIterator();
			for (int x = 0; x < subscriptionRecordQueue.size(); x++) {
				outputRecords[x] = i.next();
			}
			return outputRecords;
		}
	}

	public Record[] dumpRecordsFromTimeRange(long startTime, long endTime) {
		synchronized (lock) {
			ListIterator<Record> queueFirstListItr = subscriptionRecordQueue
					.listIterator();
			ListIterator<Record> queueSecondListItr = subscriptionRecordQueue
					.listIterator(subscriptionRecordQueue.size());
			boolean foundStart = false, foundEnd = false;
			int startPos = 0, endPos = subscriptionRecordQueue.size() - 1;
			Record firstRecord = null;
			while (queueFirstListItr.hasNext()) {
				firstRecord = queueFirstListItr.next();
				long currentTimeStamp = firstRecord.getTimestamp();
				if (currentTimeStamp > startTime && currentTimeStamp <= endTime) {
					foundStart = true;
					break;
				}
				startPos++;
			}
			if (foundStart) {
				while (queueSecondListItr.hasPrevious()) {
					Record record = queueFirstListItr.previous();
					long timeStamp = record.getTimestamp();
					if (timeStamp > startTime && timeStamp <= endTime) {
						foundEnd = true;
						break;
					}
					endPos--;
				}
			}
			if (foundEnd) {
				Record[] outputRecords = new Record[endPos - startPos + 1];
				outputRecords[0] = firstRecord;
				for (int x = 1; x < (endPos - startPos + 1); x++) {
					outputRecords[x] = queueFirstListItr.next();
				}
				return outputRecords;
			}
		}
		return null;
	}

	public boolean put(String producerName, Record record) {
		if(!producers.containsKey(producerName))
			return false;
		synchronized (lock) {
			if (subscriptionRecordQueue.size() == maxQueueLength) {
				int positionToRemove = (maxQueueLength / 2);
				System.err
						.println(System.currentTimeMillis()
								+ " DEBUG: "
								+ id
								+ " Request received to add element to full queue, dropping record from the middle of the queue.");
				Iterator<Map.Entry<String, Integer>> iterator = consumers
						.entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) iterator
							.next();
					if (((Integer) pair.getValue()).intValue() > positionToRemove) {
						Integer newPosition = new Integer(
								((Integer) pair.getValue()).intValue() - 1);
						consumers.put((String) pair.getKey(), newPosition);
					} else {
						System.err
								.println(System.currentTimeMillis()
										+ " DEBUG: "
										+ id
										+ " Subscriber "
										+ (String) pair.getKey()
										+ " missed a Record in this queue that was dropped when the queue became full and a subsequent put was performed");
					}
				}
				subscriptionRecordQueue.remove(positionToRemove);
				subscriptionRecordQueue.add(record);
			} else {
				subscriptionRecordQueue.add(record);
			}

		}
		synchronized (valueAdded) {
			valueAdded.notifyAll();
		}
		return true;
	}

	public Record get() {
		int waitTime=10;
		while(true){
			synchronized(lock) {
				if(subscriptionRecordQueue.size()>0)
					return subscriptionRecordQueue.get(0);	
			}
			try {
				synchronized (valueAdded) {
					valueAdded.wait(waitTime);
					if (waitTime < 1000) {
						waitTime *= 10;
					}
				}
			} catch (Exception e) {}
		}
	}

	public Record get(String subscriberName) {
		boolean getComplete = false;
		boolean needToWaitForRecord = false;
		int waitTime = 10;
		Record record = null;
		if(subscriberName==null || subscriberName.equals("")){
			return get();
		}
		while (!getComplete) {
			// Synchronize on lock for reading/writing SubscriberQueue contents.
			synchronized (lock) {
				int positionToRead = consumers.get(subscriberName).intValue();
				// Check if we can read a value based on queue size and
				// subscriber position.
				if (positionToRead == maxQueueLength
						|| positionToRead == subscriptionRecordQueue.size()) {
					needToWaitForRecord = true;
					// If we have a value we can read, then read it and adjust
					// the positions.
				} else {
					record = subscriptionRecordQueue.get(positionToRead);
					positionToRead++;
					consumers
							.put(subscriberName, new Integer(positionToRead));
					// Check if we can delete the element at the front of the
					// queue.
					if (positionToRead == 1) {
						boolean canDrop = true;
						Iterator<Map.Entry<String, Integer>> i = consumers
								.entrySet().iterator();
						while (i.hasNext()) {
							Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) i
									.next();
							if (((Integer) pair.getValue()).intValue() == 0) {
								canDrop = false;
								break;
							}
						}
						if (canDrop) {
							subscriptionRecordQueue.remove(0);
							i = consumers.entrySet().iterator();
							while (i.hasNext()) {
								Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) i
										.next();
								int newPosition = ((Integer) pair.getValue())
										.intValue();
								newPosition--;
								consumers.put((String) pair.getKey(),
										new Integer(newPosition));
							}
						}
					}
					getComplete = true;
				}
			}
			if (needToWaitForRecord) {
				try {
					synchronized (valueAdded) {
						valueAdded.wait(waitTime);
						if (waitTime < 1000) {
							waitTime *= 10;
						}
					}
				} catch (Exception e) {}
			}
		}
		return record;
	}
}
