package com.mapr.distiller.server.datatypes;

public class ProcMetricDescriptor {
	public String metricName, queueName;
	public int periodicity, queueCapacity;
	
	public ProcMetricDescriptor(String metricName, String queueName, int periodicity, int queueCapacity){
		this.metricName = metricName;
		this.queueName = queueName;
		this.periodicity = periodicity;
		this.queueCapacity = queueCapacity;
	}
	
	public boolean equals(ProcMetricDescriptor d){
		return (metricName == d.metricName && queueName == d.queueName && periodicity == d.periodicity && queueCapacity == d.queueCapacity);
	}
	
	public boolean equals (String metricName, String queueName, int periodicity, int queueCapacity){
		return (this.metricName == metricName && this.queueName == queueName && this.periodicity == periodicity && this.queueCapacity == queueCapacity);
	}
}
