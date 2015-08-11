package com.mapr.distiller.server.metricactions;

import java.util.Map;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.utils.MetricConfig;
import com.mapr.distiller.server.processors.*;
import com.mapr.distiller.server.recordtypes.MetricActionStatusRecord;

public class MetricAction implements Runnable, MetricsSelectable {
	private boolean DEBUG_ENABLED=true;
	Object object = new Object();
	
	protected String id;
	protected volatile boolean gatherMetric;
	protected RecordQueue inputQueue;
	protected RecordQueue outputQueue;
	protected String recordType;

	protected RecordProcessor<Record> recordProcessor;

	protected String selector;
	protected String processor;
	protected String method;
	protected String thresholdKey;
	protected String thresholdValue;
	
	protected int periodicity;
	
	private Record oldRec, newRec;
	
	private long inRecCntr, outRecCntr, processingFailureCntr, putFailureCntr, otherFailureCntr, runningTimems, startTime;
	
	private boolean metricActionStatusRecordsEnabled;
	private long metricActionStatusRecordFrequency;
	private RecordQueue metricActionStatusRecordQueue;

	//private boolean shouldPersist;

	public MetricAction(MetricConfig config, RecordQueueManager queueManager) throws Exception{
		this.oldRec = null;
		this.newRec = null;
		this.inRecCntr=0l;
		this.outRecCntr=0l;
		this.processingFailureCntr=0l;
		this.putFailureCntr=0l;
		this.otherFailureCntr=0l;
		this.runningTimems=0l;
		
		this.id = config.getId();
		this.recordType = config.getRecordType();
		this.selector = config.getSelector();
		this.processor = config.getProcessor();
		this.method = config.getMethod();
		this.periodicity = config.getPeriodicity();
		this.thresholdKey = config.getThresholdKey();
		this.thresholdValue = config.getThresholdValue();
		
		if(DEBUG_ENABLED)
			System.err.println("MetricAction-" + System.identityHashCode(this) + ": Request to initialize metric action from config:" + config.toString()); 
		if(!isValidSelector(config.getSelector()))
			throw new Exception("Failed to constuct MetricAction due to unknown selector type: " + config.getSelector());
		if(!isValidMethod(config.getMethod()))
			throw new Exception("Failed to construct MetricAction due to unknown method type: " + config.getMethod());
		if(config.getPeriodicity()<1000)
			throw new Exception("Failed to construct MetricAction due to periodicity < 1000: " + config.getPeriodicity());
		synchronized(queueManager){
			try{
				setupInputQueue(config, queueManager);
			} catch (Exception e) {
				cleanupInputQueue(config, queueManager);
				throw new Exception("Failed to setup input queue for config: " + config.toString(), e);
			}
			try{
				setupOutputQueue(config, queueManager);
			} catch (Exception e) {
				cleanupOutputQueue(config, queueManager);
				cleanupInputQueue(config, queueManager);
				throw new Exception("Failed to setup output queue for config: " + config.toString(), e);
			}
			this.inputQueue = queueManager.getQueue(config.getInputQueue());
			this.outputQueue = queueManager.getQueue(config.getOutputQueue());
		}
		
		switch(config.getProcessor()) {
			case Constants.DISKSTAT_RECORD_PROCESSOR:
				this.recordProcessor = new DiskstatRecordProcessor();
				break;
				
			case Constants.MFS_GUTS_RECORD_PROCESSOR:
				this.recordProcessor = new MfsGutsRecordProcessor();
				break;
			
			case Constants.NETWORK_INTERFACE_RECORD_PROCESSOR:
				this.recordProcessor = new NetworkInterfaceRecordProcessor();
				break;
		
			case Constants.PROCESS_RESOURCE_RECORD_PROCESSOR:
				this.recordProcessor = new ProcessResourceRecordProcessor();
				break;
	
			case Constants.SYSTEM_CPU_RECORD_PROCESSOR:
				this.recordProcessor = new SystemCpuRecordProcessor();
				break;

			case Constants.SYSTEM_MEMORY_RECORD_PROCESSOR:
				this.recordProcessor = new SystemMemoryRecordProcessor();
				break;	

			case Constants.TCP_CONNECTION_STAT_RECORD_PROCESSOR:
				this.recordProcessor = new TcpConnectionStatRecordProcessor();
				break;

			case Constants.THREAD_RESOURCE_RECORD_PROCESSOR:
				this.recordProcessor = new ThreadResourceRecordProcessor();
				break;
			/**
			case Constants.SLIM_PROCESS_RESOURCE_RECORD_PROCESSOR:
				this.recordProcessor = new SlimProcessResourceRecordProcessor();
				break;

			case Constants.SLIM_THREAD_RESOURCE_RECORD_PROCESSOR:
				this.recordProcessor = new SlimThreadResourceRecordProcessor();
				break;
			**/
			default:
				throw new Exception("Failed to construct MetricAction due to unknown RecordProcessor type: " + config.getProcessor());
		}
		
		if(config.getMetricActionStatusRecordsEnabled()){	
			if(config.getMetricActionStatusRecordFrequency() < 1000){
				throw new Exception("Failed to construct MetricAction due to metric.action.status.record.frequency < 1000: " + config.getMetricActionStatusRecordFrequency());
			}
			this.metricActionStatusRecordsEnabled = true;
			this.metricActionStatusRecordFrequency = config.getMetricActionStatusRecordFrequency();
			synchronized(queueManager){
				try{
					setupMetricActionStatusRecordQueue(queueManager);
				} catch (Exception e) {
					cleanupMetricActionStatusRecordQueue(queueManager);
					throw new Exception("Failed to setup metric action status record queue", e);
				}
			}
		} else {
			this.metricActionStatusRecordsEnabled = false;
		}
		setGathericMetric(true);
	}
	
	private void cleanupMetricActionStatusRecordQueue(RecordQueueManager recordQueueManager){
		recordQueueManager.unregisterProducer(Constants.METRIC_ACTION_STATS_QUEUE_NAME, id);
		recordQueueManager.deleteQueue(Constants.METRIC_ACTION_STATS_QUEUE_NAME);
	}
	
	private void setupMetricActionStatusRecordQueue(RecordQueueManager recordQueueManager) throws Exception {
		if(	
				!(
					(	
						recordQueueManager.queueExists(Constants.METRIC_ACTION_STATS_QUEUE_NAME) &&
						recordQueueManager.getQueueRecordCapacity(Constants.METRIC_ACTION_STATS_QUEUE_NAME) == Constants.METRIC_ACTION_STATS_QUEUE_RECORD_CAPACITY &&
						recordQueueManager.getQueueTimeCapacity(Constants.METRIC_ACTION_STATS_QUEUE_NAME) == Constants.METRIC_ACTION_STATS_QUEUE_TIME_CAPACITY &&
						recordQueueManager.getMaxQueueProducers(Constants.METRIC_ACTION_STATS_QUEUE_NAME) == 0
					)
					||
					recordQueueManager.createQueue(	Constants.METRIC_ACTION_STATS_QUEUE_NAME, 
													Constants.METRIC_ACTION_STATS_QUEUE_RECORD_CAPACITY, 
													Constants.METRIC_ACTION_STATS_QUEUE_TIME_CAPACITY, 
													0) 

				 )
		  )	
		{
			throw new Exception("Failed to obtain output queue: " + Constants.METRIC_ACTION_STATS_QUEUE_NAME);
		}
		if( !(	recordQueueManager.checkForQueueProducer(Constants.METRIC_ACTION_STATS_QUEUE_NAME, id) ||
				recordQueueManager.registerProducer(Constants.METRIC_ACTION_STATS_QUEUE_NAME, id)
			 )
		  )
		{
			throw new Exception("Failed to register producer " + id + " with queue " + Constants.METRIC_ACTION_STATS_QUEUE_NAME);
		}
		this.metricActionStatusRecordQueue = recordQueueManager.getQueue(Constants.METRIC_ACTION_STATS_QUEUE_NAME);
	}
	
	public static MetricAction getInstance(
			MetricConfig metricConfig, RecordQueueManager queueManager) throws Exception {
		return new MetricAction(metricConfig, queueManager);
	}
	public boolean isValidSelector(String selector){
		if (selector==null) return false;
		if (selector.equals("sequential") ||
			selector.equals("cummulative")
		) 
			return true;
		return false;
	}
	
	public boolean isValidMethod(String method){
		if (method==null) return false;
		if (method.equals("movingAverage") ||
			method.equals("isAbove") ||
			method.equals("isBelow")
		) 
			return true;
		return false;
	}	
	private void setupOutputQueue(MetricConfig config, RecordQueueManager recordQueueManager) throws Exception{
		if(	
				!(
					(	
						recordQueueManager.queueExists(config.getOutputQueue()) &&
						recordQueueManager.getQueueRecordCapacity(config.getOutputQueue()) == config.getOutputQueueRecordCapacity() &&
						recordQueueManager.getQueueTimeCapacity(config.getOutputQueue()) == config.getOutputQueueTimeCapacity() &&
						recordQueueManager.getMaxQueueProducers(config.getOutputQueue()) == config.getOutputQueueMaxProducers() && 
						recordQueueManager.getQueueProducers(config.getOutputQueue()).length < config.getOutputQueueMaxProducers()
					)
					||
					recordQueueManager.createQueue(	config.getOutputQueue(), 
													config.getOutputQueueRecordCapacity(), 
													config.getOutputQueueTimeCapacity(), 
													config.getOutputQueueMaxProducers()) 

				 )
		  )	
		{
			throw new Exception("Failed to obtain output queue: " + config.getOutputQueue());
		}
		if( !(	recordQueueManager.checkForQueueProducer(config.getOutputQueue(), config.getId()) ||
				recordQueueManager.registerProducer(config.getOutputQueue(), config.getId())
			 )
		  )
		{
			throw new Exception("Failed to register producer " + config.getId() + " with queue " + config.getOutputQueue());
		}
	}
	
	private void setupInputQueue(MetricConfig config, RecordQueueManager recordQueueManager) throws Exception{
		if(!recordQueueManager.queueExists(config.getInputQueue()))
			throw new Exception("Input queue does not exist: " + config.getInputQueue());
		if( !(	recordQueueManager.checkForQueueConsumer(config.getInputQueue(), config.getId()) ||
				recordQueueManager.registerConsumer(config.getInputQueue(), config.getId())
			 )
		  )
		{
			throw new Exception("Failed to register consumer " + config.getId() + " with queue " + config.getOutputQueue());
		}
	}
	
	public void cleanupOutputQueue(MetricConfig config, RecordQueueManager recordQueueManager){
		recordQueueManager.unregisterProducer(config.getOutputQueue(), config.getId());
		recordQueueManager.deleteQueue(config.getOutputQueue());
	}
	
	public void cleanupInputQueue(MetricConfig config, RecordQueueManager recordQueueManager){
		recordQueueManager.unregisterConsumer(config.getInputQueue(), config.getId());
		recordQueueManager.deleteQueue(config.getInputQueue());
	}
	
	@Override
	public void run() {
		startTime = System.currentTimeMillis();
		long lastStatus = startTime;
		if(DEBUG_ENABLED)
			System.err.println("MetricAction-" + System.identityHashCode(this) + ": Started metric action " + this.id);
		while (!Thread.interrupted()) {
			if (isGathericMetric()) {
				if(selector.equals("sequential"))
					selectSequentialRecords();
				else if(selector.equals("cummulative"))
					selectCumulativeRecords();
				else 
					System.err.println("Unknown selector: " + selector);
				if (metricActionStatusRecordsEnabled &&
					System.currentTimeMillis() >= lastStatus + metricActionStatusRecordFrequency)
				{
					lastStatus = System.currentTimeMillis();
					metricActionStatusRecordQueue.put(id, new MetricActionStatusRecord(id, inRecCntr, outRecCntr, processingFailureCntr, putFailureCntr, otherFailureCntr, runningTimems, startTime));
				}
				try {
					Thread.sleep(this.periodicity);
				} catch (InterruptedException e) {
					System.out.println("Thread got interrupted - Going down");
					break;
				}
			}

			else {
				if(DEBUG_ENABLED)
					System.err.println("MetricAction-" + System.identityHashCode(this) + ": Sleeping until metric gathering is enabled");
				try {
					synchronized (object) {
						object.wait();
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if(DEBUG_ENABLED)
			System.err.println("MetricAction-" + System.identityHashCode(this) + ": Shutting down.");
	}

	@Override
	public void selectSequentialRecords() {
		long st = System.currentTimeMillis();
		
		while((newRec = inputQueue.get(id, false)) != null){
			inRecCntr++;
			
			//Break if this is the first record read from the input queue and the selector is a type that requires two records
			if(method.equals("movingAverage") && oldRec == null){
				oldRec = newRec;
				break;
			}
			if(method.equals("movingAverage")){
				try {
					Record outputRec = recordProcessor.movingAverage(oldRec, newRec);
					try {
						outputQueue.put(id, outputRec);
						outRecCntr++;
					} catch (Exception e) {
						putFailureCntr++;
					}
				} catch (Exception e) {
					processingFailureCntr++;
				}
				oldRec = newRec;
			} 
			else if (method.equals("isAbove")){
				try {
					if(recordProcessor.isAboveThreshold(newRec, thresholdKey, thresholdValue)){
						try {
							outputQueue.put(id,  newRec);
							outRecCntr++;
						} catch (Exception e) {
							putFailureCntr++;
						}
					}
				} catch (Exception e) {
					processingFailureCntr++;
				}
			}
			else if (method.equals("isBelow")){
				try {
					if(recordProcessor.isBelowThreshold(newRec, thresholdKey, thresholdValue)){
						try {
							outputQueue.put(id,  newRec);
							outRecCntr++;
						} catch (Exception e) {
							putFailureCntr++;
						}
					}
				} catch (Exception e) {
					processingFailureCntr++;
				}
			}
			else {
				otherFailureCntr++;
			}
		}
		runningTimems += System.currentTimeMillis() - st;
	}

	@Override
	public void selectCumulativeRecords() {
		long st = System.currentTimeMillis();
		
		while((newRec = inputQueue.get(id, false)) != null){
			inRecCntr++;
			
			if(newRec.getPreviousTimestamp()==-1l){
				System.err.println("Can not perform cumulative movingAverage against raw Records");
				otherFailureCntr++;
				break;
			}
			//Break if this is the first record read from the input queue and the selector is a type that requires two records
			if(method.equals("movingAverage") && oldRec == null){
				oldRec = newRec;
				try{
					outputQueue.put(id,  oldRec);
					outRecCntr++;
				} catch (Exception e) {
					putFailureCntr++;
				}
				break;
			} 
			else if(method.equals("movingAverage")){
				try {
					oldRec = recordProcessor.movingAverage(oldRec, newRec);
					try {
						outputQueue.put(id, oldRec);
						outRecCntr++;
					} catch (Exception e) {
						putFailureCntr++;
					}
				} catch (Exception e) {
					oldRec = newRec;
					processingFailureCntr++;
				}
			} 
			else {
				otherFailureCntr++;
			}
		}
		runningTimems += System.currentTimeMillis() - st;
	}

	@Override
	public void selectTimeSeparatedRecords() {
		// TODO Auto-generated method stub

	}
	

	public void suspend() throws InterruptedException {
		System.out.println("Stopping metric with id = " + id);
		if (gatherMetric) {
			gatherMetric = false;
		}

		else {
			System.out.println("Already suspended metric " + id);
		}
	}

	public void resume() {
		synchronized (object) {
			if (!gatherMetric) {
				System.out.println("Resuming metric with id = " + id);
				gatherMetric = true;
				object.notifyAll();
			}

			else {
				System.out.println("Already running Metric " + id);
			}
		}
	}

	public void kill() {
		System.out.println("Kill metric with id = " + id);
		gatherMetric = false;
	}

	public boolean isGathericMetric() {
		return gatherMetric;
	}

	public void setGathericMetric(boolean isGathericMetric) {
		this.gatherMetric = isGathericMetric;
	}

	public MetricAction(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

}
