package com.mapr.distiller.server.coordinators;

import java.util.concurrent.ConcurrentHashMap;

import com.mapr.distiller.server.processors.SystemCpuMovingAverageRecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;
import com.mapr.distiller.server.readers.PassthroughRecordReader;

public class SystemCpuRecordToSystemCpuMovingAverageRecordProducer extends
		Thread {
	private String id;
	private ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap;
	private SubscriptionRecordQueue systemCpuRecordQueue,
			systemCpuMovingAverageRecordQueue;
	private SystemCpuMovingAverageRecordProcessor systemCpuMovingAverageRecordProcessor;
	private PassthroughRecordReader passthroughRecordReader;
	private RecordProducer recordProducer;
	int movingAverageWidth;
	boolean registeredOutputQueue = false, initialized = false,
			shouldStop = false;
	String outputQueueName;

	public SystemCpuRecordToSystemCpuMovingAverageRecordProducer(
			ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap,
			String id, int movingAverageWidth) {
		this.id = id + ":SCMA-" + movingAverageWidth;
		this.nameToRecordQueueMap = nameToRecordQueueMap;
		this.movingAverageWidth = movingAverageWidth;
		this.outputQueueName = "systemCpuMovingAverage-" + movingAverageWidth;

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

			this.systemCpuMovingAverageRecordQueue = new SubscriptionRecordQueue(
					outputQueueName, 300);

			try {
				if (nameToRecordQueueMap.putIfAbsent(outputQueueName,
						this.systemCpuMovingAverageRecordQueue) != null) {
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
				this.systemCpuMovingAverageRecordProcessor = new SystemCpuMovingAverageRecordProcessor(
						this.movingAverageWidth);

				recordProducer = new RecordProducer(
						this.passthroughRecordReader,
						this.systemCpuMovingAverageRecordProcessor,
						this.systemCpuMovingAverageRecordQueue, this.id + ":P",
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
		if (!systemCpuMovingAverageRecordQueue
				.registerProducer("SystemCpuRecordToSystemCpuMovingAverage")) {
			System.err
					.println("Failed to register SystemCpuRecordToSystemCpuMovingAverage with systemCpuMovingAverage");
			System.exit(1);
		}

		while (!shouldStop) {
			if (!recordProducer.isAlive()) {
				recordProducer = new RecordProducer(
						this.passthroughRecordReader,
						this.systemCpuMovingAverageRecordProcessor,
						this.systemCpuMovingAverageRecordQueue, this.id + ":P",
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
		systemCpuMovingAverageRecordQueue
				.unregisterProducer("SystemCpuRecordToSystemCpuMovingAverage");
		systemCpuRecordQueue.unsubscribe(outputQueueName);
	}

}
