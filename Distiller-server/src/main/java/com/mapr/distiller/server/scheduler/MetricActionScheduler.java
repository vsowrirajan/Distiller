package com.mapr.distiller.server.scheduler;

import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.metricactions.MetricAction;
import com.mapr.distiller.server.scheduler.MetricActionScheduleComparator;

public class MetricActionScheduler {

	private static final Logger LOG = LoggerFactory
			.getLogger(MetricActionScheduler.class);

	// This holds the list of MetricActions to gather sorted by the time
	// returned by MetricAction.nextScheduledTime(
	private TreeSet<MetricAction> metricSchedule;
	long window;

	public boolean contains(MetricAction a) {
		return metricSchedule.contains(a);
	}

	public MetricActionScheduler(long window) throws Exception {
		if (window < 0)
			throw new Exception("Invalid value for window: " + window
					+ ", expect >= 0");
		this.window = window;
		metricSchedule = new TreeSet<MetricAction>(
				new MetricActionScheduleComparator());
	}

	public void schedule(MetricAction event) throws Exception {
		if (event == null)
			throw new Exception("Can not schedule a null MetricAction");
		synchronized (metricSchedule) {
			if (metricSchedule.add(event)) {
				metricSchedule.notify();
			} else {
				throw new Exception("Failed to add MetricAction "
						+ event.getId() + " to metric schedule, exists:"
						+ metricSchedule.contains(event));
			}
		}
	}

	public void unschedule(MetricAction event) {
		synchronized (metricSchedule) {
			metricSchedule.remove(event);
			metricSchedule.notify();
		}
	}

	public long getNextScheduledTime(boolean blocking) {
		long timeToReturn = -1;
		synchronized (metricSchedule) {
			try {
				timeToReturn = metricSchedule.first().getNextScheduleTime();
			} catch (Exception e) {
				return -1;
			}
			return timeToReturn;
		}
	}

	private MetricAction nonBlockingGetNextAction() {
		synchronized (metricSchedule) {
			MetricAction retVal = null;
			try {
				retVal = metricSchedule.first();
			} catch (Exception e) {
			}
			return retVal;
		}
	}

	public MetricAction getNextScheduledMetricAction(boolean blocking)
			throws Exception {
		MetricAction retVal = null;
		if (!blocking) {
			synchronized (metricSchedule) {
				retVal = nonBlockingGetNextAction();
				try {
					if (retVal != null
							&& retVal.getNextScheduleTime() <= System
									.currentTimeMillis()) {
						if (!metricSchedule.remove(retVal))
							throw new Exception("Failed to remove "
									+ retVal.getId() + " from metric schedule");
						return retVal;
					}
				} catch (Exception e) {
				}
			}
			return null;
		} else {
			while (true) {
				synchronized (metricSchedule) {
					long sleepTime = -1;
					boolean haveSleepTime = true;
					try {
						sleepTime = metricSchedule.first()
								.getNextScheduleTime()
								- System.currentTimeMillis();
					} catch (Exception e) {
						haveSleepTime = false;
					}
					if (haveSleepTime) {
						if (sleepTime <= 0) {
							retVal = nonBlockingGetNextAction();
							if (!metricSchedule.remove(retVal))
								throw new Exception("Failed to remove "
										+ retVal.getId()
										+ " from metric schedule");
							return retVal;
						} else {
							try {
								metricSchedule.wait(sleepTime);
							} catch (Exception e) {
							}
						}
					} else {
						try {
							metricSchedule.wait();
						} catch (Exception e) {
						}
					}
				}
			}
		}
	}

}
