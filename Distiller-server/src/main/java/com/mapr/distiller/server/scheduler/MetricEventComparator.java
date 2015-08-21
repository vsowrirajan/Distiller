package com.mapr.distiller.server.scheduler;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricEventComparator implements Comparator<GatherMetricEvent>{
	
	private static final Logger LOG = LoggerFactory
			.getLogger(MetricEventComparator.class);
	
	public int compare(GatherMetricEvent e1, GatherMetricEvent e2) {
		if(e1.getTargetTime() < e2.getTargetTime()){
			return -1;
		} else if (e2.getTargetTime() < e1.getTargetTime()){
			return 1;
		}
		return e1.getMetricName().compareTo(e2.getMetricName());
	}

}
