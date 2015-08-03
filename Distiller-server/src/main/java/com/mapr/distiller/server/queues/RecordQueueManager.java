package com.mapr.distiller.server.queues;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;

public class RecordQueueManager {
	private static ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap;
	private static ConcurrentHashMap<String, Integer> nameToMaxProducerMap;
	
	public RecordQueueManager(){
		nameToRecordQueueMap = new ConcurrentHashMap<String, RecordQueue>(1000);
		nameToMaxProducerMap = new ConcurrentHashMap<String, Integer>(1000);
	}
	
	public boolean createQueue(String queueName, int capacity, int maxProducers){
		if(!queueExists(queueName) && maxProducers>=0){
			nameToRecordQueueMap.put(queueName, new SubscriptionRecordQueue(queueName, capacity));
			nameToMaxProducerMap.put(queueName,  maxProducers);
			return true;
		}
		return false;
	}
	
	public boolean deleteQueue(String queueName){
		if(queueExists(queueName) && getQueue(queueName).listConsumers().length == 0 && getQueue(queueName).listProducers().length ==0){
			nameToRecordQueueMap.remove(queueName);
			nameToMaxProducerMap.remove(queueName);
		} else if (!queueExists(queueName))
			return true;
		return false;
	}
	
	public RecordQueue getQueue(String name) {
		return nameToRecordQueueMap.get(name);
	}
	
	public RecordQueue[] getQueues() {
		RecordQueue[] ret = new RecordQueue[nameToRecordQueueMap.size()];
		Iterator<Map.Entry<String, RecordQueue>> it = nameToRecordQueueMap.entrySet().iterator();
		int pos=0;
		while (it.hasNext()) {
			Map.Entry<String, RecordQueue> pair = (Map.Entry<String, RecordQueue>) it
					.next();
			ret[pos++] = (RecordQueue) pair.getValue();
		}
		return ret;
	}
	
	public boolean queueExists(String name) {
		return nameToRecordQueueMap.containsKey(name);
	}
	
	public int getQueueCapacity(String name) {
		return nameToRecordQueueMap.get(name).maxQueueSize();
	}
	
	public String[] getQueueProducers(String name) {
		return nameToRecordQueueMap.get(name).listProducers();
	}
	
	public String[] getQueueConsumers(String name) {
		return nameToRecordQueueMap.get(name).listConsumers();
	}
	
	public int getMaxQueueProducers(String name) {
		return nameToMaxProducerMap.get(name);
	}
	
	public boolean checkForQueueProducer(String queueName, String producerName){
		String[] producers = getQueueProducers(queueName);
		for(int x=0; x<producers.length; x++){
			if(producers[x].equals(producerName))
				return true;
		}
		return false;
	}

	public boolean checkForQueueConsumer(String queueName, String consumerName){
		String[] consumers = getQueueConsumers(queueName);
		for(int x=0; x<consumers.length; x++){
			if(consumers[x].equals(consumerName))
				return true;
		}
		return false;
	}
	
	public boolean registerProducer(String queueName, String producerName){
		if(queueExists(queueName) && 
				(getMaxQueueProducers(queueName) == 0 || getQueueProducers(queueName).length < getMaxQueueProducers(queueName))){
			getQueue(queueName).registerProducer(producerName);
			return true;
		}
		return false;
	}
	
	public boolean registerConsumer(String queueName, String consumerName){
		if(queueExists(queueName)){
			getQueue(queueName).registerConsumer(consumerName);
			return true;
		}
		return false;
	}
	
	public boolean unregisterProducer(String queueName, String producerName){
		if(queueExists(queueName) && checkForQueueProducer(queueName, producerName))
			return getQueue(queueName).unregisterProducer(producerName);
		return false;
	}

	public boolean unregisterConsumer(String queueName, String consumerName){
		if(queueExists(queueName) && checkForQueueConsumer(queueName, consumerName))
			return getQueue(queueName).unregisterConsumer(consumerName);
		return false;
	}
}



/**


		//Synchronize on the enabledMetricManager since we can not allow other threads to enable/disable the same metric concurrently
		synchronized(enabledMetricManager){
			//Return true if the metric is already being gathered
			if(enabledMetricManager.containsDescriptor(metricName, queueName, periodicity, queueCapacity))
				return;
			//If we're here, this is a new metric being requested. (Though the same metric may currently be enabled, for instance, with an alternate periodicity)
			//Synchronize on the name to RecordQueue manager so we can see if the requested output queue already exists, without other threads creating/deleting queues in the mean time
			synchronized(queueManager){
				//Check if the output queue exists
				if(queueManager.queueExists(queueName)){
					//The output queue already exists, check if we can re-use it.
					//Check if it has the desired capacity
					if(queueManager.getQueueCapacity(queueName) != queueCapacity)
						throw new Exception("Requested to create queue " + queueName + " with capacity " + queueCapacity + " but queue already exists with capacity " + queueManager.getQueueCapacity(queueName));
					//Check if there are existing producers for this queue
					//In this case, I'm throwing an exception if something else is already producing into the queue
					//But I'm not sure thats the right behavior
					//I can't immediately discern that there are useful cases where we want this to produce into a queue something else is producing into at the same time
					//I can think of scenarios where people make mistakes in the config and do this accidentally so I think I'd like to default to not allowing it
					//If there is a legitimate need for it later then we will need to adjust this code.
					if(queueManager.getQueueProducers(queueName).length != 0)
						throw new Exception("Requested queue " + queueName + " for metric " + metricName + " already exists with " + queueManager.getQueueProducers(queueName).length + " registered producers");
					//OK, queue already exists with requested capacity and it has no registered producers, we can reuse it.
					//Of course, we did not check if there were existing consumers attached to the queue...
					//A consumer might start getting a different type of record from this queue than it got before, one it doens't know how to understand.
					//Hopefully consumers are written in such a way that they can notice and react to this gracefully somewhere in the call stack...
				//In this case, the queue doesn't exist, so we should create it.
				} else {
					if(!queueManager.createQueue(queueName, queueCapacity, 1))
						throw new Exception("RecordQueueManager failed to create queue " + queueName + " with capacity " + queueCapacity + " and 1 max producer");
					createdQueue = true;
				}
				//Register ProcRecordProducer as the producer for the output RecordQueue
				if(!queueManager.registerProducer(queueName, metricId)){
					//We failed to register with the queue, try to delete it if we created it...
					if(createdQueue)
						if(!queueManager.deleteQueue(queueName)){
							//This is bad, we created a queue a moment ago and are now failing to delete it...
							if(queueManager.queueExists(queueName)){
								//So it exists, we created it, but we can't delete it... log a scary error...
								System.err.println("ERROR: Failed to cleanup queue " + queueName + " while attempting to enable metric " + metricName + ", the RecordQueueManager may be inconsistent.");
								throw new Exception("Failed to enable metric due to unknown internal error");
							}
						}
					throw new Exception("Failed to register as a producer for RecordQueue " + queueName + " with " + queueManager.getQueueProducers(queueName).length + " registered and " + queueManager.getMaxQueueProducers(queueName) + " max producers");
				}

**/