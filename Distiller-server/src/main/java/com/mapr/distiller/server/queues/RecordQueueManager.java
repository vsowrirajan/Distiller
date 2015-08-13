package com.mapr.distiller.server.queues;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class RecordQueueManager {
	private boolean DEBUG_ENABLED=true;
	private static ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap;
	private static ConcurrentHashMap<String, Integer> nameToMaxProducerMap;
	
	public RecordQueueManager(){
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Initializing");
		nameToRecordQueueMap = new ConcurrentHashMap<String, RecordQueue>(1000);
		nameToMaxProducerMap = new ConcurrentHashMap<String, Integer>(1000);
	}
	
	public String getQueueType(String queueName){
		return getQueue(queueName).getQueueType();
	}
	
	public String getQueueQualifierKey(String queueName){
		return getQueue(queueName).getQueueQualifierKey();
	}
	
	public boolean createQueue(String queueName, int recordCapacity, int timeCapacity, int maxProducers, String queueType, String qualifierKey){
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Request to create queue " + queueName + 
					" rCap:" + recordCapacity + " tCap:" + timeCapacity + " maxProducer:" + maxProducers + " queueType:" + queueType);
		if(queueType == null)
			queueType = Constants.SUBSCRIPTION_RECORD_QUEUE;

		if(!queueExists(queueName) && maxProducers>=0){
			if(queueType.equals(Constants.SUBSCRIPTION_RECORD_QUEUE))
				nameToRecordQueueMap.put(queueName, new SubscriptionRecordQueue(queueName, recordCapacity, timeCapacity));
			else if (queueType.equals(Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE)){
				try {
					nameToRecordQueueMap.put(queueName, new UpdatingSubscriptionRecordQueue(queueName, recordCapacity, timeCapacity, qualifierKey));
				} catch (Exception e) {
					System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Failed to construct UpdatingSubscriptionRecordQueue for \"" + queueName + "\"");
					return false;
				}
			} else {
				System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Invalid queueType \"" + queueType + "\" requested for queue \"" + queueName + "\"");
				return false;
			}
			nameToMaxProducerMap.put(queueName,  maxProducers);
			if(DEBUG_ENABLED)
				System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Created queue " + queueName);
			return true;
		}
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Could not create queue");
		return false;
	}
	
	public boolean deleteQueue(String queueName){
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Request to delete queue " + queueName);
		if(queueExists(queueName) && getQueue(queueName).listConsumers().length == 0 && getQueue(queueName).listProducers().length ==0){
			nameToRecordQueueMap.remove(queueName);
			nameToMaxProducerMap.remove(queueName);
			if(DEBUG_ENABLED)
				System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Deleted queue " + queueName);
			return true;
		} else if (!queueExists(queueName)){
			if(DEBUG_ENABLED)
				System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Request to delete non-existant queue " + queueName);
			return true;
		}
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Failed to delete queue " + queueName);
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
	
	public int getQueueRecordCapacity(String name) {
		if(nameToRecordQueueMap.get(name)==null)
			return -1;
		return nameToRecordQueueMap.get(name).getQueueRecordCapacity();
	}
	
	public int getQueueTimeCapacity(String name) {
		if(nameToRecordQueueMap.get(name)==null)
			return -1;
		return nameToRecordQueueMap.get(name).getQueueTimeCapacity();
	}

	public String[] getQueueProducers(String name) {
		if(nameToRecordQueueMap.get(name)==null)
			return new String[0];
		return nameToRecordQueueMap.get(name).listProducers();
	}
	
	public String[] getQueueConsumers(String name) {
		if(nameToRecordQueueMap.get(name)==null)
			return new String[0];
		return nameToRecordQueueMap.get(name).listConsumers();
	}
	
	public int getMaxQueueProducers(String name) {
		if(!nameToMaxProducerMap.containsKey(name))
			return -1;
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
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Request to register producer " + producerName + " with queue " + queueName);
		if(queueExists(queueName) && 
				(getMaxQueueProducers(queueName) == 0 || getQueueProducers(queueName).length < getMaxQueueProducers(queueName))){
			getQueue(queueName).registerProducer(producerName);
			if(DEBUG_ENABLED)
				System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Registered producer " + producerName + " with queue " + queueName);
			return true;
		}
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Failed to register producer " + producerName + " with queue " + queueName);
		return false;
	}
	
	public boolean registerConsumer(String queueName, String consumerName){
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Request to register consumer " + consumerName + " with queue " + queueName);
		if(queueExists(queueName)){
			getQueue(queueName).registerConsumer(consumerName);
			if(DEBUG_ENABLED)
				System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Registered consumer " + consumerName + " with queue " + queueName);
			return true;
		}
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Failed to register consumer " + consumerName + " with queue " + queueName);
		return false;
	}
	
	public boolean unregisterProducer(String queueName, String producerName){
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Request to unregister producer " + producerName + " from queue " + queueName);
		if(queueExists(queueName) && checkForQueueProducer(queueName, producerName))
			return getQueue(queueName).unregisterProducer(producerName);
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Failed to unregister producer " + producerName + " from queue " + queueName);
		return false;
	}

	public boolean unregisterConsumer(String queueName, String consumerName){
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Request to unregister consumer " + consumerName + " from queue " + queueName);
		if(queueExists(queueName) && checkForQueueConsumer(queueName, consumerName))
			return getQueue(queueName).unregisterConsumer(consumerName);
		if(DEBUG_ENABLED)
			System.err.println("RecordQueueManager-" + System.identityHashCode(this) + ": Failed to unregister consumer " + consumerName + " from queue " + queueName);
		return false;
	}
}