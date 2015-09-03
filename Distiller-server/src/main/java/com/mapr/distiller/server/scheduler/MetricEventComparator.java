package com.mapr.distiller.server.scheduler;

import java.util.Comparator;

public class MetricEventComparator implements Comparator<GatherMetricEvent>{

	public int compare(GatherMetricEvent e1, GatherMetricEvent e2) {
		if(e1.getTargetTime() < e2.getTargetTime()){
			return -1;
		} else if (e2.getTargetTime() < e1.getTargetTime()){
			return 1;
		}
		return e1.getMetricName().compareTo(e2.getMetricName());
	}

}
