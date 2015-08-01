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
