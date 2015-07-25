package com.mapr.distiller.server.scheduler;

public class GatherMetricEvent {
	private long previousTime, targetTime;
	private String metricName, queueName, producerName;
	private int periodicity;
	
	public GatherMetricEvent(long previousTime, long targetTime, String metricName, String queueName, String producerName, int periodicity) {
		this.previousTime = previousTime;
		this.targetTime = targetTime;
		this.metricName = metricName;
		this.queueName = queueName;
		this.producerName = producerName;
		this.periodicity = periodicity;
	}
	
	public String getProducerName(){
		return producerName;
	}
	public String getMetricName(){
		return metricName;
	}
	
	public long getPreviousTime(){
		return previousTime;
	}
	
	public long getTargetTime(){
		return targetTime;
	}
	
	public int getPeriodicity(){
		return periodicity;
	}
	
	public String getQueueName(){
		return queueName;
	}
	
	public void setPreviousTime(long previousTime){
		this.previousTime = previousTime;
	}
	
	public void setTargetTime(long targetTime){
		this.targetTime = targetTime;
	}
}
