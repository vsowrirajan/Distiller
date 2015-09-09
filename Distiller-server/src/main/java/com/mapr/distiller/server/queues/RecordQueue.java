package com.mapr.distiller.server.queues;

import com.mapr.distiller.server.recordtypes.Record;

public interface RecordQueue {
	//Return the type of the RecordQueue
	public String getQueueType();
	
	//Return the qualifier key used by the RecordQueue
	public String getQueueQualifierKey();
	
	//Return the name of the RecordQueue
	public String getQueueName();
	
	//Return the queue capacity in number of Records
	public int getQueueRecordCapacity();
		
	//Return the queue capacity in number of seconds
	public int getQueueTimeCapacity();
		
	//Return the number of elements in the queue.
	public int queueSize();

	//Return the number of elements in the queue available for the specific subscriber to get
	public int queueSize(String subscriber);
	
	//Add a Record onto the end of the queue
	public boolean put(String producer, Record record);

	//Perform a blocking get for the next sequential Record for the specific subscriber
	public Record get(String subscriber) throws Exception;

	//Perform a get for the next sequential Record for the specific consumer, either blocking or non blocking as specified, return null for non-blocking requests where no records are available
	public Record get(String subscriber, boolean blocking) throws Exception;
	
	//Perform a peek at the next sequential Record for the specific consumer, either blocking or non blocking as specified, return null for non-blocking requests where no records are available
	public Record peek(String subscriber, boolean blocking) throws Exception;
	
	//Return a String array where each element represents the name of a registered Producer
	public String[] listProducers();

	//Return a String array where each element represents the name of a registered Consumer
	public String[] listConsumers();
	
	//Returns true if the given name is registered as a producer
	public boolean hasProducer(String name);
	
	//Returns a true if the given name is registered as a consumer
	public boolean hasConsumer(String name);
	
	//Returns true if the producer with the given name is successfully registered as a producer
	public boolean registerProducer(String name);
	
	//Returns true if the consumer with the given name is successfully registered as a consumer
	public boolean registerConsumer(String name);
	
	//Returns true if the producer with the given name was successfully removed as a producer (false if no change)
	public boolean unregisterProducer(String name);
	
	//Returns true if the consumer with the given name was successfully removed as a consumer (false if no change)
	public boolean unregisterConsumer(String name);

	//Returns the value of getTimestamp from the oldest record in the queue or now if the queue is empty
	public long getOldestRecordTimestamp();
	
	//Iterate through the records in the queue, calling Record.toString() for each Record that the specified consumer can consume
	//What happens if this gets too big???
	public String printRecords(String subscriber);
	
	//Iterate through the records in the queue starting from the most recently added Record, calling Record.toString() for each Record that the specified consumer can consume
	public String printNewestRecords(String consumer, int numRecords);
		
	/**
		Will implement these later as needed...
	

	

	public Record[] dumpNewestRecords(String consumer, int numRecords);

	public Record[] dumpOldestRecords(String consumer, int numRecords);

	public Record[] dumpAllRecords(String consumer);

	public Record[] dumpRecordsFromTimeRange(String consumer, long startTime,
			long endTime);
	**/
}