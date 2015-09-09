package com.mapr.distiller.server.controllers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.mapr.distiller.common.status.MetricActionStatus;
import com.mapr.distiller.common.status.RecordProducerStatus;
import com.mapr.distiller.common.status.RecordQueueStatus;
import com.mapr.distiller.server.DistillerMonitor;
import com.mapr.distiller.server.recordtypes.Record;

@RestController
@EnableAutoConfiguration
@RequestMapping("/distiller")
public class CoordinatorController {

	private static final Logger LOG = LoggerFactory
			.getLogger(CoordinatorController.class);

	@Autowired
	private DistillerMonitor monitor;

	public String getCoordinator() {
		LOG.debug("Coordinator status");
		return "Coordinator status page";
	}

	@RequestMapping(value = "/producerStatus", method = RequestMethod.GET)
	public RecordProducerStatus getRecordProducerStatus() {
		LOG.info("Get Record Producer status");
		return monitor.getRecordProducerStatus();
	}

	@RequestMapping(value = "/queues", method = RequestMethod.GET)
	public List<RecordQueueStatus> getRecordQueues() {
		LOG.info("Get Record Queues");
		return monitor.getRecordQueues();
	}

	@RequestMapping(value = "/queues/{name}", method = RequestMethod.GET)
	public RecordQueueStatus getQueueStatus(@PathVariable String name)
			throws Exception {
		LOG.info("Queue " + name + " status");
		return monitor.getQueueStatus(name);
	}

	@RequestMapping(value = "/queues/records/{name}", method = RequestMethod.GET)
	public Record[] getNewest100Records(@PathVariable String name)
			throws Exception {
		int count = 100;
		return monitor.getRecords(name, count);

		/*
		 * LOG.info("Get "+ count + " records from queue " + name); return
		 * records;
		 */
	}

	@RequestMapping(value = "/queues/records/{name}/{count}", method = RequestMethod.GET)
	public Record[] getNewestRecords(@PathVariable String name,
			@PathVariable int count) throws Exception {
		return monitor.getRecords(name, count);
		/*
		 * LOG.info("Get "+ count + " records from queue " + name); return
		 * records;
		 */
	}

	@RequestMapping(value = "/metricActions", method = RequestMethod.GET)
	public List<MetricActionStatus> getMetricActions() {
		LOG.info("Get Metric Actions");
		return monitor.getMetricActions();
	}

	@RequestMapping(value = "/metricActions/{name}", method = RequestMethod.GET)
	public MetricActionStatus getMetricAction(@PathVariable String name) throws Exception {
		LOG.info("Get status for Metric Action " + name);
		return monitor.getMetricAction(name);
	}

	@RequestMapping(value = "/metricActions/enable/{name}", method = RequestMethod.PUT)
	public boolean enableMetric(@PathVariable String name) throws Exception {
		LOG.info("Enable metric " + name);
		return monitor.metricEnable(name);
	}

	@RequestMapping(value = "/metricActions/disable/{name}", method = RequestMethod.PUT)
	public boolean disableMetric(@PathVariable String name) throws Exception {
		LOG.info("Disable metric " + name);
		return monitor.metricDisable(name);
	}
	
	@RequestMapping(value = "/metricActions/isScheduled/{name}", method = RequestMethod.GET)
	public boolean isScheduledMetricAction(@PathVariable String name) throws Exception {
    LOG.info("Is Scheduled for " + name);
    return monitor.isScheduledMetricAction(name);
	}

	@RequestMapping(value = "/metricActions/isRunning/{name}", method = RequestMethod.GET)
	public boolean isRunningMetricAction(@PathVariable String name) throws Exception {
	   LOG.info("Is Running for " + name);
	   return monitor.isRunningMetricAction(name);
	}
}
