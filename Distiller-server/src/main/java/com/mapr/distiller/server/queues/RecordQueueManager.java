package com.mapr.distiller.server.queues;

import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class RecordQueueManager {

	private static final Logger LOG = LoggerFactory
			.getLogger(RecordQueueManager.class);

	private static SortedMap<String, RecordQueue> nameToRecordQueueMap;
	private static ConcurrentHashMap<String, Integer> nameToMaxProducerMap;

	public RecordQueueManager() {
		LOG.info("RecordQueueManager- {} : Initializing",
				System.identityHashCode(this));
		nameToRecordQueueMap = new TreeMap<String, RecordQueue>();
		nameToMaxProducerMap = new ConcurrentHashMap<String, Integer>(1000);
	}

	public String getQueueType(String queueName) {
		return getQueue(queueName).getQueueType();
	}

	public String getQueueQualifierKey(String queueName) {
		return getQueue(queueName).getQueueQualifierKey();
	}

	public boolean createQueue(String queueName, int recordCapacity,
			int timeCapacity, int maxProducers, String queueType,
			String qualifierKey) {
		LOG.debug(
				"RecordQueueManager- {} : Request to create queue {}  rCap: {}  tCap: {}  maxProducer: {}  queueType: {} ",
				System.identityHashCode(this), queueName, recordCapacity,
				timeCapacity, maxProducers, queueType);
		if (queueType == null)
			queueType = Constants.SUBSCRIPTION_RECORD_QUEUE;

		if (!queueExists(queueName) && maxProducers >= 0) {
			if (queueType.equals(Constants.SUBSCRIPTION_RECORD_QUEUE))
				nameToRecordQueueMap.put(queueName,
						new SubscriptionRecordQueue(queueName, recordCapacity,
								timeCapacity));
			else if (queueType
					.equals(Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE)) {
				try {
					nameToRecordQueueMap
							.put(queueName,
									new UpdatingSubscriptionRecordQueue(
											queueName, recordCapacity,
											timeCapacity, qualifierKey));
				} catch (Exception e) {
					LOG.error(
							"RecordQueueManager- {} : Failed to construct UpdatingSubscriptionRecordQueue for \" {} \"",
							System.identityHashCode(this), queueName);
					return false;
				}
			} else {
				LOG.debug(
						"RecordQueueManager- {} : Invalid queueType \" {} \" requested for queue \" {} ",
						System.identityHashCode(this), queueType, queueName);
				return false;
			}
			nameToMaxProducerMap.put(queueName, maxProducers);
			LOG.info("RecordQueueManager- {} : Created queue  {} ",
					System.identityHashCode(this), queueName);
			return true;
		}
		LOG.error("RecordQueueManager- {} : Could not create queue",
				System.identityHashCode(this));
		return false;
	}

	public boolean deleteQueue(String queueName) {
		LOG.info("RecordQueueManager- {} : Request to delete queue  {} ",
				System.identityHashCode(this), queueName);
		if (queueExists(queueName)
				&& getQueue(queueName).listConsumers().length == 0
				&& getQueue(queueName).listProducers().length == 0) {
			nameToRecordQueueMap.remove(queueName);
			nameToMaxProducerMap.remove(queueName);
			LOG.info("RecordQueueManager- {} : Deleted queue ",
					System.identityHashCode(this), queueName);
			return true;
		} else if (!queueExists(queueName)) {
			LOG.debug(
					"RecordQueueManager- {} : Request to delete non-existant queue  {} ",
					System.identityHashCode(this), queueName);
			return true;
		}
		LOG.error("RecordQueueManager- {} : Failed to delete queue  {} ",
				System.identityHashCode(this), queueName);
		return false;
	}

	public RecordQueue getQueue(String name) {
		return nameToRecordQueueMap.get(name);
	}

	public RecordQueue[] getQueues() {
		RecordQueue[] ret = new RecordQueue[nameToRecordQueueMap.size()];
		Iterator<Map.Entry<String, RecordQueue>> it = nameToRecordQueueMap
				.entrySet().iterator();
		int pos = 0;
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
		if (nameToRecordQueueMap.get(name) == null)
			return -1;
		return nameToRecordQueueMap.get(name).getQueueRecordCapacity();
	}

	public int getQueueTimeCapacity(String name) {
		if (nameToRecordQueueMap.get(name) == null)
			return -1;
		return nameToRecordQueueMap.get(name).getQueueTimeCapacity();
	}

	public String[] getQueueProducers(String name) {
		if (nameToRecordQueueMap.get(name) == null)
			return new String[0];
		return nameToRecordQueueMap.get(name).listProducers();
	}

	public String[] getQueueConsumers(String name) {
		if (nameToRecordQueueMap.get(name) == null)
			return new String[0];
		return nameToRecordQueueMap.get(name).listConsumers();
	}

	public int getMaxQueueProducers(String name) {
		if (!nameToMaxProducerMap.containsKey(name))
			return -1;
		return nameToMaxProducerMap.get(name);
	}

	public boolean checkForQueueProducer(String queueName, String producerName) {
		String[] producers = getQueueProducers(queueName);
		for (int x = 0; x < producers.length; x++) {
			if (producers[x].equals(producerName))
				return true;
		}
		return false;
	}

	public boolean checkForQueueConsumer(String queueName, String consumerName) {
		String[] consumers = getQueueConsumers(queueName);
		for (int x = 0; x < consumers.length; x++) {
			if (consumers[x].equals(consumerName))
				return true;
		}
		return false;
	}

	public boolean registerProducer(String queueName, String producerName) {
		LOG.info(
				"RecordQueueManager- {} : Request to register producer  {} with queue {}",
				System.identityHashCode(this), producerName, queueName);
		if (queueExists(queueName)
				&& (getMaxQueueProducers(queueName) == 0 || getQueueProducers(queueName).length < getMaxQueueProducers(queueName))) {
			getQueue(queueName).registerProducer(producerName);
			LOG.info(
					"RecordQueueManager- {} : Registered producer {}  with queue",
					System.identityHashCode(this), producerName, queueName);
			return true;
		}
		LOG.error(
				"RecordQueueManager- {} : Failed to register producer  {}  with queue {}",
				System.identityHashCode(this), producerName, queueName);
		return false;
	}

	public boolean registerConsumer(String queueName, String consumerName) {
		LOG.info(
				"RecordQueueManager- {} : Request to register consumer {}  with queue {} ",
				System.identityHashCode(this), consumerName, queueName);
		if (queueExists(queueName)) {
			getQueue(queueName).registerConsumer(consumerName);
			LOG.info(
					"RecordQueueManager- {} : Registered consumer {} with queue {} ",
					System.identityHashCode(this), consumerName, queueName);
			return true;
		}
		LOG.error(
				"RecordQueueManager- {} : Failed to register consumer  {}  with queue  {}",
				System.identityHashCode(this), consumerName, queueName);
		return false;
	}

	public boolean unregisterProducer(String queueName, String producerName) {
		LOG.info(
				"RecordQueueManager- {} : Request to unregister producer  {} from queue {} ",
				System.identityHashCode(this), producerName, queueName);
		if (queueExists(queueName)
				&& checkForQueueProducer(queueName, producerName))
			return getQueue(queueName).unregisterProducer(producerName);
		LOG.error(
				"RecordQueueManager- {} : Failed to unregister producer {} from queue {}",
				System.identityHashCode(this), producerName, queueName);
		return false;
	}

	public boolean unregisterConsumer(String queueName, String consumerName) {
		LOG.info(
				"RecordQueueManager- {} : Request to unregister consumer {}  from queue {} ",
				System.identityHashCode(this), consumerName, queueName);
		if (queueExists(queueName)
				&& checkForQueueConsumer(queueName, consumerName))
			return getQueue(queueName).unregisterConsumer(consumerName);
		LOG.error(
				"RecordQueueManager- {} : Failed to unregister consumer {}  from queue {}",
				System.identityHashCode(this), consumerName, queueName);
		return false;
	}
}