package com.mapr.distiller.server.coordinators;

import java.util.concurrent.ConcurrentHashMap;

import com.mapr.distiller.server.processors.SystemCpuHighRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;
import com.mapr.distiller.server.readers.PassthroughRecordReader;

public class SystemCpuRecordToSystemCpuHighRecordProducer extends Thread {
	private String id;
	private ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap;
	private SubscriptionRecordQueue systemCpuRecordQueue,
			systemCpuHighRecordQueue;
	private SystemCpuHighRecordProcessor systemCpuHighRecordProcessor;
	private PassthroughRecordReader passthroughRecordReader;
	private RecordProducer recordProducer;
	private double systemCpuHighThreshold;
	private boolean registeredOutputQueue = false, initialized = false,
			shouldStop = false;
	private String outputQueueName;

	public SystemCpuRecordToSystemCpuHighRecordProducer(
			ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap,
			String id, Double systemCpuHighThreshold) {
		this.id = id + ":SCH-" + systemCpuHighThreshold;
		this.nameToRecordQueueMap = nameToRecordQueueMap;
		this.systemCpuHighThreshold = systemCpuHighThreshold;
		this.outputQueueName = "systemCpuHigh-" + systemCpuHighThreshold + "%";

		try {
			this.systemCpuRecordQueue = (SubscriptionRecordQueue) nameToRecordQueueMap
					.get("systemCpu");
			if (this.systemCpuRecordQueue == null) {
				System.err
						.println("ERROR: "
								+ this.id
								+ ": nameToRecordQueueMap does not have an entry for systemCpu");
			}
		} catch (Exception e) {
			System.err
					.println("ERROR: "
							+ this.id
							+ ": Caught an exception while looking up systemCpu RecordQueue in nameToRecordQueueMap");
			e.printStackTrace();
		}

		if (this.systemCpuRecordQueue != null) {
			this.passthroughRecordReader = new PassthroughRecordReader(
					systemCpuRecordQueue);

			this.systemCpuHighRecordQueue = new SubscriptionRecordQueue(
					outputQueueName, 300);

			try {
				if (nameToRecordQueueMap.putIfAbsent(outputQueueName,
						this.systemCpuHighRecordQueue) != null) {
					System.err.println("ERROR: " + id
							+ ": nameToRecordQueueMap already contains "
							+ outputQueueName + " but not from this instance.");
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
				registeredOutputQueue = true;
			}

			if (registeredOutputQueue) {
				this.systemCpuHighRecordProcessor = new SystemCpuHighRecordProcessor(
						systemCpuHighThreshold);

				recordProducer = new RecordProducer(
						this.passthroughRecordReader,
						this.systemCpuHighRecordProcessor,
						this.systemCpuHighRecordQueue, this.id + ":P",
						outputQueueName);
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
		if (!systemCpuRecordQueue.subscribe(outputQueueName)) {
			System.err.println("Failed to subscribe " + outputQueueName
					+ " with systemCpu queue");
			System.exit(1);
		}
		if (!systemCpuHighRecordQueue
				.registerProducer("SystemCpuRecordToSystemCpuHigh")) {
			System.err
					.println("Failed to register SystemCpuRecordToSystemCpuHigh with systemCpuHigh");
			System.exit(1);
		}
		while (!shouldStop) {
			if (!recordProducer.isAlive()) {
				recordProducer = new RecordProducer(
						this.passthroughRecordReader,
						this.systemCpuHighRecordProcessor,
						this.systemCpuHighRecordQueue, this.id + ":P",
						outputQueueName);
				recordProducer.start();
			}
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
			;
		}
		recordProducer.requestStop();
		systemCpuHighRecordQueue
				.unregisterProducer("SystemCpuRecordToSystemCpuHigh");
		systemCpuRecordQueue.unsubscribe(outputQueueName);
	}

}
