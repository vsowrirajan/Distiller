package com.mapr.distiller.server.scheduler;

import com.mapr.distiller.server.queues.RecordQueue;

public class GatherMetricEvent {
	private long previousTime, targetTime;
	private String metricName;
	RecordQueue outputQueue;
	private int periodicity;
	
	public GatherMetricEvent(long previousTime, long targetTime, String metricName, RecordQueue outputQueue, int periodicity) {
		this.previousTime = previousTime;
		this.targetTime = targetTime;
		this.metricName = metricName;
		this.outputQueue = outputQueue;
		this.periodicity = periodicity;
	}
	
	public RecordQueue getRecordQueue(){
		return outputQueue;
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

	public void setPreviousTime(long previousTime){
		this.previousTime = previousTime;
	}
	
	public void setTargetTime(long targetTime){
		this.targetTime = targetTime;
	}
}
