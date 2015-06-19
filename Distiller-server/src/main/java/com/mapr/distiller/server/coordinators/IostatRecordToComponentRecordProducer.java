package com.mapr.distiller.server.coordinators;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.mapr.distiller.server.processors.IostatRecordToSplitterRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.SingleConsumerRecordQueue;
import com.mapr.distiller.server.queues.SplitterRecordQueue;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;
import com.mapr.distiller.server.readers.PassthroughRecordReader;

public class IostatRecordToComponentRecordProducer extends Thread {
	private String id;
	private ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap;
	private SingleConsumerRecordQueue iostatRecordQueue;
	private SubscriptionRecordQueue systemCpuRecordQueue,
			deviceUsageRecordQueue;
	private SplitterRecordQueue splitterRecordQueue;
	private PassthroughRecordReader passthroughRecordReader;
	private IostatRecordToSplitterRecordProcessor iostatRecordToSplitterRecordProcessor;
	private boolean initialized = false;
	private HashMap<String, RecordQueue> metricNameToRecordQueueMap;
	private RecordProducer recordProducer;
	private boolean registeredOutputQueues = false;
	private boolean shouldStop = false;

	public IostatRecordToComponentRecordProducer(
			ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap,
			String id) {
		this.id = id + ":IR2CR";
		this.nameToRecordQueueMap = nameToRecordQueueMap;

		try {
			this.iostatRecordQueue = (SingleConsumerRecordQueue) nameToRecordQueueMap
					.get("iostatProcessRecords");
			if (this.iostatRecordQueue == null) {
				System.err
						.println("ERROR: "
								+ this.id
								+ ": nameToRecordQueueMap does not have an entry for iostatProcessRecords");
			}
		} catch (Exception e) {
			System.err
					.println("ERROR: "
							+ this.id
							+ ": Caught an exception while looking up iostatProcessRecords queue in nameToRecordQueueMap");
			e.printStackTrace();
		}

		if (this.iostatRecordQueue != null) {

			this.passthroughRecordReader = new PassthroughRecordReader(
					iostatRecordQueue);

			// Output queue for systemCpuRecord's
			this.systemCpuRecordQueue = new SubscriptionRecordQueue(
					"systemCpu", 300); // 300 samples at 1 second interval = 5
										// minutes buffer

			// Output queue for deviceUsageRecord's
			this.deviceUsageRecordQueue = new SubscriptionRecordQueue(
					"deviceUsage", 14400); // 300 samples from 48 disks at 1
											// second interval = 5 minutes
											// buffer

			// Register the new RecordQueues
			try {
				if (nameToRecordQueueMap.putIfAbsent("systemCpu",
						this.systemCpuRecordQueue) != null) {
					System.err
							.println("ERROR: "
									+ id
									+ ": nameToRecordQueueMap already contains systemCpu but not from this instance.");
					throw new Exception();
				}
			} catch (Exception e) {
				System.err
						.println("ERROR: "
								+ id
								+ ": Caught exception while trying to register output RecordQueue in nameToRecordQueueMap, this "
								+ this + " will exit.");
				e.printStackTrace();
			} finally {
				try {
					if (nameToRecordQueueMap.putIfAbsent("deviceUsage",
							this.deviceUsageRecordQueue) != null) {
						System.err
								.println("ERROR: "
										+ id
										+ ": nameToRecordQueueMap already contains deviceUsage but not from this instance.");
						throw new Exception();
					}
				} catch (Exception e) {
					System.err
							.println("ERROR: "
									+ id
									+ ": Caught exception while trying to register output RecordQueue in nameToRecordQueueMap, this "
									+ this + " will exit.");
					e.printStackTrace();
				} finally {
					registeredOutputQueues = true;
				}
			}

			if (registeredOutputQueues) {
				this.metricNameToRecordQueueMap = new HashMap<String, RecordQueue>(
						5);
				this.metricNameToRecordQueueMap.put("systemCpu",
						this.systemCpuRecordQueue);
				this.metricNameToRecordQueueMap.put("deviceUsage",
						this.deviceUsageRecordQueue);

				this.splitterRecordQueue = new SplitterRecordQueue(
						metricNameToRecordQueueMap);

				this.iostatRecordToSplitterRecordProcessor = new IostatRecordToSplitterRecordProcessor();

				recordProducer = new RecordProducer(
						this.passthroughRecordReader,
						this.iostatRecordToSplitterRecordProcessor,
						this.splitterRecordQueue, this.id + ":P");

				initialized = true;
			}
		}
	}

	public void run() {
		if (!initialized) {
			System.err
					.println(id
							+ ": Could not start RecordProducer because this instance is not initialized... exiting.");
			System.exit(1);
		}
		if (!iostatRecordQueue.subscribe("IostatRecordToComponent")) {
			System.err
					.println("Failed to subscribe IostatRecordToComponent to IostatProcess");
			System.exit(1);
			;
		}
		if (!systemCpuRecordQueue.registerProducer("IostatRecordToComponent")) {
			System.err
					.println("Failed to register IostatRecordToComponent with systemCpu");
			System.exit(1);
		}
		if (!deviceUsageRecordQueue.registerProducer("IostatRecordToComponent")) {
			System.err
					.println("Failed to register IostatRecordToComponent with deviceUsage");
			System.exit(1);
		}

		while (!shouldStop) {
			if (!recordProducer.isAlive()) {
				recordProducer = new RecordProducer(
						this.passthroughRecordReader,
						this.iostatRecordToSplitterRecordProcessor,
						this.splitterRecordQueue, this.id + ":P");
				recordProducer.start();
			}
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
			;
		}
		systemCpuRecordQueue.unregisterProducer("IostatRecordToComponent");
		deviceUsageRecordQueue.unregisterProducer("IostatRecordToComponent");
		iostatRecordQueue.unsubscribe("IostatRecordToComponent");
		recordProducer.requestStop();
	}
}
