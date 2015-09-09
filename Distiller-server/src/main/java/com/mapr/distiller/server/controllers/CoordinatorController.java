package com.mapr.distiller.server.controllers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.mapr.distiller.server.DistillerMonitor;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.status.MetricActionStatus;
import com.mapr.distiller.server.status.RecordProducerStatus;
import com.mapr.distiller.server.status.RecordQueueStatus;

@Controller
@EnableAutoConfiguration
public class CoordinatorController {

	private static final Logger LOG = LoggerFactory
			.getLogger(CoordinatorController.class);

	@Autowired
	private DistillerMonitor monitor;

	@RequestMapping("/")
	@ResponseBody
	public String getCoordinator() {
		LOG.debug("Coordinator status");
		return "Coordinator status page";
	}

	@RequestMapping(value = "/producerStatus", method = RequestMethod.GET)
	@ResponseBody
	public RecordProducerStatus getRecordProducerStatus() {
		LOG.info("Get Record Producer status");
		return monitor.getRecordProducerStatus();
	}

	@RequestMapping(value = "/queues", method = RequestMethod.GET)
	@ResponseBody
	public List<RecordQueueStatus> getRecordQueues() {
		LOG.info("Get Record Queues");
		return monitor.getRecordQueues();
	}

	@RequestMapping(value = "/queues/{name}", method = RequestMethod.GET)
	@ResponseBody
	public RecordQueueStatus getQueueStatus(@PathVariable String name) {
		LOG.info("Queue " + name + " status");
		return monitor.getQueueStatus(name);
	}

	@RequestMapping(value = "/queues/records/{name}", method = RequestMethod.GET)
	@ResponseBody
	public Record[] getNewest100Records(@PathVariable String name) {
		int count = 100;
		return monitor.getRecords(name, count);
		/*
		 * LOG.info("Get "+ count + " records from queue " + name); return
		 * records;
		 */
	}

	@RequestMapping(value = "/queues/records/{name}/{count}", method = RequestMethod.GET)
	@ResponseBody
	public Record[] getNewestRecords(@PathVariable String name,
			@PathVariable int count) {
		return monitor.getRecords(name, count);
		/*
		 * LOG.info("Get "+ count + " records from queue " + name); return
		 * records;
		 */
	}

	@RequestMapping(value = "/metricActions", method = RequestMethod.GET)
	@ResponseBody
	public List<MetricActionStatus> getMetricActions() {
		LOG.info("Get Metric Actions");
		return monitor.getMetricActions();
	}

	@RequestMapping(value = "/metricActions/enable/{name}", method = RequestMethod.PUT)
	@ResponseBody
	public boolean enableMetric(@PathVariable String name) {
		LOG.info("Enable metric " + name);
		return monitor.metricEnable(name);
	}

	@RequestMapping(value = "/metricActions/disable/{name}", method = RequestMethod.PUT)
	@ResponseBody
	public boolean disableMetric(@PathVariable String name) {
		LOG.info("Disable metric " + name);
		return monitor.metricDisable(name);
	}

	@RequestMapping(value = "/requestShutdown", method = RequestMethod.GET)
	@ResponseBody
	public boolean requestShutdown() {
		LOG.info("Request to shutdown");
		return monitor.requestShutdown();
	}

}
