package com.mapr.distiller.server.datatypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcMetricDescriptor {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(ProcMetricDescriptor.class);
	
	public String metricName;
	public int periodicity;
	
	public ProcMetricDescriptor(String metricName, int periodicity){
		this.metricName = metricName;
		this.periodicity = periodicity;
	}
	
	public boolean equals(ProcMetricDescriptor d){
		return (metricName.equals(d.metricName) && periodicity == d.periodicity);
	}
	
	public boolean equals (String metricName, int periodicity){
		return (this.metricName.equals(metricName) && this.periodicity == periodicity);
	}
}
