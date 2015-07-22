package com.mapr.distiller.server.scheduler;

public class GatherMetricEvent {
	private long previousTime, targetTime;
	private String metricName;
	private int periodicity;
	
	public GatherMetricEvent(long previousTime, long targetTime, String metricName, int periodicity) {
		this.previousTime = previousTime;
		this.targetTime = targetTime;
		this.metricName = metricName;
		this.periodicity = periodicity;
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
