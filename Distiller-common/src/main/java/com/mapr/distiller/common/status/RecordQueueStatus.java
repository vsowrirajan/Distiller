package com.mapr.distiller.common.status;

public class RecordQueueStatus {
	private String name;

	private String type;

	private int recordCapacity;

	private int queueSize;

	private String[] producers;

	private String[] consumers;
	
	public RecordQueueStatus() {
	  
	}	    

	public RecordQueueStatus(String name, String type, int recordCapacity,
			int queueSize, String[] producers, String[] consumers) {
		this.name = name;
		this.type = type;
		this.recordCapacity = recordCapacity;
		this.queueSize = queueSize;
		this.producers = producers;
		this.consumers = consumers;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public int getRecordCapacity() {
		return recordCapacity;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public String[] getProducers() {
		return producers;
	}

	public String[] getConsumers() {
		return consumers;
	}
}
