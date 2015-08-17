package com.mapr.distiller.server.scheduler;
import java.util.Comparator;
import com.mapr.distiller.server.metricactions.MetricAction;

public class MetricActionScheduleComparator implements Comparator<MetricAction>{
	public int compare(MetricAction e1, MetricAction e2) {
		try {
			if(e1.getNextScheduleTime() < e2.getNextScheduleTime())
				return -1;
			else  if (e1.getNextScheduleTime() > e2.getNextScheduleTime())
				return 1;
		} catch (Exception e){}
		return ((e1.hashCode()<e2.hashCode()) ? -1 : 1);
	}
}
