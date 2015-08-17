package com.mapr.distiller.server.metricactions;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.utils.MetricConfig;
import com.mapr.distiller.server.processors.*;
import com.mapr.distiller.server.recordtypes.MetricActionStatusRecord;
import com.mapr.distiller.server.scheduler.Schedule;
import com.mapr.distiller.server.scheduler.MetricActionScheduler;
import com.mapr.distiller.server.selectors.related.RelatedRecordSelector;
import com.mapr.distiller.server.selectors.related.BasicRelatedRecordSelector;

public class MetricAction implements Runnable, MetricsSelectable {
	private boolean DEBUG_ENABLED=false;
	Object object = new Object();
	private Schedule schedule;
	private MetricActionScheduler metricActionScheduler;
	
	protected String id;
	protected volatile boolean gatherMetric;
	protected RecordQueue inputQueue;
	protected RecordQueue outputQueue;
	protected RecordQueue relatedInputQueue;
	protected RecordQueue relatedOutputQueue;
	protected String recordType;

	protected RecordProcessor<Record> recordProcessor;
	protected RelatedRecordSelector<Record, Record> relatedRecordSelector;

	protected String selector;
	protected String processor;
	protected String method;
	protected String thresholdKey;
	protected String thresholdValue;
	
	protected int periodicity;
	
	private Record oldRec, newRec;
	private LinkedList<Record> recordList;
	private HashMap<String, Record> recordMap;
	private HashMap<String, Long> lastSeenIterationMap;
	private HashMap<String, Long> lastPutTimeMap;
	private long lastSeenCount;
	
	private long inRecCntr, outRecCntr, processingFailureCntr, putFailureCntr, otherFailureCntr, runningTimems, startTime;
	
	private boolean metricActionStatusRecordsEnabled;
	private long metricActionStatusRecordFrequency;
	private RecordQueue metricActionStatusRecordQueue;
	private MetricConfig config;
	private RecordQueueManager queueManager;
	private long timeSelectorMinDelta;
	private long timeSelectorMaxDelta;
	private String selectorQualifierKey;
	private long cumulativeSelectorFlushTime;
	private long lastStatus = -1l;
	private long iterationCount = 0l;

	public String printSchedule(){
		return schedule.toString();
	}
	
	public MetricAction(MetricConfig config, RecordQueueManager queueManager, MetricActionScheduler metricActionScheduler) throws Exception{
		this.metricActionScheduler = metricActionScheduler;
		this.recordList = new LinkedList<Record>();
		this.recordMap = new HashMap<String, Record>();
		this.lastSeenIterationMap = new HashMap<String, Long>();
		this.lastPutTimeMap = new HashMap<String, Long>();
		this.lastSeenCount=0l;
		this.config = config;
		this.queueManager = queueManager;
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
		this.timeSelectorMinDelta = config.getTimeSelectorMinDelta();
		this.timeSelectorMaxDelta = config.getTimeSelectorMaxDelta();
		this.selectorQualifierKey = config.getSelectorQualifierKey();
		this.cumulativeSelectorFlushTime = config.getCumulativeSelectorFlushTime();
		
		this.schedule = new Schedule(this.periodicity,
									 1d,	//Hard coding here to disable adaptive scheduling (not sure if design is good yet))
									 0d );	//Hard coding here to disable adaptive scheduling

		
		synchronized(queueManager){
			try{
				setupInputQueue(config, queueManager);
			} catch (Exception e) {
				cleanupInputQueue(config, queueManager);
				throw new Exception("Failed to setup input queue for config: " + config.toString(), e);
			}
			try{
				setupRelatedInputQueue(config, queueManager);
			} catch (Exception e) {
				cleanupRelatedInputQueue(config, queueManager);
				cleanupInputQueue(config, queueManager);
				throw new Exception("Failed to setup related queue for config: " + config.toString(), e);
			}
			try{
				setupRelatedOutputQueue(config, queueManager);
			} catch (Exception e) {
				cleanupRelatedOutputQueue(config, queueManager);
				cleanupRelatedInputQueue(config, queueManager);
				cleanupInputQueue(config, queueManager);
				throw new Exception("Failed to setup related queue for config: " + config.toString(), e);
			}
			try{
				setupOutputQueue(config, queueManager);
			} catch (Exception e) {
				cleanupOutputQueue(config, queueManager);
				cleanupRelatedOutputQueue(config, queueManager);
				cleanupRelatedInputQueue(config, queueManager);
				cleanupInputQueue(config, queueManager);
				throw new Exception("Failed to setup output queue for config: " + config.toString(), e);
			}
			this.inputQueue = queueManager.getQueue(config.getInputQueue());
			this.outputQueue = queueManager.getQueue(config.getOutputQueue());
			if(config.getRelatedSelectorEnabled()){
				this.relatedInputQueue = queueManager.getQueue(config.getRelatedInputQueueName());
				this.relatedOutputQueue = queueManager.getQueue(config.getRelatedOutputQueueName());
			}
		}
		
		if(config.getRelatedSelectorEnabled()){
			switch(config.getRelatedSelectorName()){
			case Constants.BASIC_RELATED_RECORD_SELECTOR:
				this.relatedRecordSelector = new BasicRelatedRecordSelector(id, 
																			relatedInputQueue,
																			relatedOutputQueue, 
																			config.getRelatedSelectorMethod(),
																			config.getSelectorQualifierKey(),
																			config.getSelectorQualifierValue());
				break;
				
			default:
				throw new Exception("Unknown value for " + Constants.SELECTOR_RELATED_NAME + " - value: " + 
									config.getRelatedSelectorName());
			}
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

			case Constants.SLIM_PROCESS_RESOURCE_RECORD_PROCESSOR:
				this.recordProcessor = new SlimProcessResourceRecordProcessor();
				break;

			case Constants.SLIM_THREAD_RESOURCE_RECORD_PROCESSOR:
				this.recordProcessor = new SlimThreadResourceRecordProcessor();
				break;

			default:
				throw new Exception("Failed to construct MetricAction due to unknown RecordProcessor type: " + config.getProcessor());
		}
		
		if(config.getMetricActionStatusRecordsEnabled()){	
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
		setGatherMetric(true);
		
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
													0, null, null) 

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
			MetricConfig metricConfig, RecordQueueManager queueManager, MetricActionScheduler metricActionScheduler) throws Exception {
		return new MetricAction(metricConfig, queueManager, metricActionScheduler);
	}

	private void setupOutputQueue(MetricConfig config, RecordQueueManager recordQueueManager) throws Exception{
		if(	
				!(
					(	
						recordQueueManager.queueExists(config.getOutputQueue()) &&
						recordQueueManager.getQueueRecordCapacity(config.getOutputQueue()) == config.getOutputQueueRecordCapacity() &&
						recordQueueManager.getQueueTimeCapacity(config.getOutputQueue()) == config.getOutputQueueTimeCapacity() &&
						recordQueueManager.getMaxQueueProducers(config.getOutputQueue()) == config.getOutputQueueMaxProducers() && 
						recordQueueManager.getQueueProducers(config.getOutputQueue()).length < config.getOutputQueueMaxProducers() &&
						( ( ( recordQueueManager.getQueueType(config.getOutputQueue())==null ||
						      recordQueueManager.getQueueType(config.getOutputQueue()).equals(Constants.SUBSCRIPTION_RECORD_QUEUE)
						    )
						    && 
						    config.getOutputQueueType()==null
						  ) 
						  ||
						  ( recordQueueManager.getQueueType(config.getOutputQueue()).equals(config.getOutputQueueType()) &&
							( !recordQueueManager.getQueueType(config.getOutputQueue()).equals(Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE) ||
							  recordQueueManager.getQueueQualifierKey(config.getOutputQueue()).equals(config.getUpdatingSubscriptionQueueKey())
							)
					      )
						)
						
					)
					||
					(
						!recordQueueManager.queueExists(config.getOutputQueue()) &&
						recordQueueManager.createQueue(	config.getOutputQueue(), 
														config.getOutputQueueRecordCapacity(), 
														config.getOutputQueueTimeCapacity(), 
														config.getOutputQueueMaxProducers(),
														config.getOutputQueueType(),
														config.getUpdatingSubscriptionQueueKey()) 
					)

				 )
		  )	
		{
			if (recordQueueManager.queueExists(config.getOutputQueue()) && 
				recordQueueManager.getQueueProducers(config.getOutputQueue()).length == recordQueueManager.getMaxQueueProducers(config.getOutputQueue()))
				throw new Exception("Failed to obtain output queue \"" + config.getOutputQueue() + "\" as max producers has been reached.");		
			throw new Exception("Failed to obtain output queue: " +
					" qe:" + recordQueueManager.queueExists(config.getOutputQueue()) + 
					" oq:" + config.getOutputQueue() + 
					" oqrc:" + config.getOutputQueueRecordCapacity() + "/" + recordQueueManager.getQueueRecordCapacity(config.getOutputQueue()) + 
					" oqtc:" + config.getOutputQueueTimeCapacity() + "/" + recordQueueManager.getQueueTimeCapacity(config.getOutputQueue()) + 
					" oqmp:" + config.getOutputQueueMaxProducers() + "/" + recordQueueManager.getMaxQueueProducers(config.getOutputQueue()) +  
					" oqt:" + config.getOutputQueueType() + "/" + recordQueueManager.getQueueType(config.getOutputQueue()) + 
					" sqk:" + config.getSelectorQualifierKey() + "/" + recordQueueManager.getQueueQualifierKey(config.getOutputQueue()) + 
					" rp:" + recordQueueManager.getQueueProducers(config.getOutputQueue()).length
					);
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
			throw new Exception("Failed to register consumer " + config.getId() + " with queue " + config.getInputQueue());
		}
	}

	private void setupRelatedInputQueue(MetricConfig config, RecordQueueManager recordQueueManager) throws Exception{
		if(!config.getRelatedSelectorEnabled())
			return;
		if(!recordQueueManager.queueExists(config.getRelatedInputQueueName()))
			throw new Exception("Related queue does not exist: " + config.getRelatedInputQueueName());
		if( !(	recordQueueManager.checkForQueueConsumer(config.getRelatedInputQueueName(), config.getId()) ||
				recordQueueManager.registerConsumer(config.getRelatedInputQueueName(), config.getId())
			 )
		  )
		{
			throw new Exception("Failed to register consumer " + config.getId() + " with queue " + config.getRelatedInputQueueName());
		}
	}

	private void setupRelatedOutputQueue(MetricConfig config, RecordQueueManager recordQueueManager) throws Exception{
		if(!config.getRelatedSelectorEnabled())
			return;
		if(	
				!(
					(	
						recordQueueManager.queueExists(config.getRelatedOutputQueueName()) &&
						recordQueueManager.getQueueRecordCapacity(config.getRelatedOutputQueueName()) == config.getRelatedOutputQueueRecordCapacity() &&
						recordQueueManager.getQueueTimeCapacity(config.getRelatedOutputQueueName()) == config.getRelatedOutputQueueTimeCapacity() &&
						recordQueueManager.getMaxQueueProducers(config.getRelatedOutputQueueName()) == config.getRelatedOutputQueueMaxProducers() && 
						recordQueueManager.getQueueProducers(config.getRelatedOutputQueueName()).length < config.getRelatedOutputQueueMaxProducers() &&
						recordQueueManager.getQueueType(config.getRelatedOutputQueueName()).equals(Constants.SUBSCRIPTION_RECORD_QUEUE)
					)
					||
					(
						!recordQueueManager.queueExists(config.getRelatedOutputQueueName()) &&
						recordQueueManager.createQueue(	config.getRelatedOutputQueueName(), 
														config.getRelatedOutputQueueRecordCapacity(), 
														config.getRelatedOutputQueueTimeCapacity(), 
														config.getRelatedOutputQueueMaxProducers(),
														Constants.SUBSCRIPTION_RECORD_QUEUE,
														null) 
					)

				 )
		  )	
		{
			if (recordQueueManager.queueExists(config.getRelatedOutputQueueName()) && 
				recordQueueManager.getQueueProducers(config.getRelatedOutputQueueName()).length == recordQueueManager.getMaxQueueProducers(config.getRelatedOutputQueueName()))
				throw new Exception("Failed to obtain output queue \"" + config.getRelatedOutputQueueName() + "\" as max producers has been reached.");		
			throw new Exception("Failed to obtain output queue: " +
					" qe:" + recordQueueManager.queueExists(config.getRelatedOutputQueueName()) + 
					" oq:" + config.getRelatedOutputQueueName() + 
					" oqrc:" + config.getRelatedOutputQueueRecordCapacity() + "/" + recordQueueManager.getQueueRecordCapacity(config.getRelatedOutputQueueName()) + 
					" oqtc:" + config.getRelatedOutputQueueTimeCapacity() + "/" + recordQueueManager.getQueueTimeCapacity(config.getRelatedOutputQueueName()) + 
					" oqmp:" + config.getRelatedOutputQueueMaxProducers() + "/" + recordQueueManager.getMaxQueueProducers(config.getRelatedOutputQueueName()) +  
					" oqt:" + Constants.SUBSCRIPTION_RECORD_QUEUE + "/" + recordQueueManager.getQueueType(config.getRelatedOutputQueueName()) + 
					" sqk:" + config.getSelectorQualifierKey() + "/" + recordQueueManager.getQueueQualifierKey(config.getRelatedOutputQueueName()) + 
					" rp:" + recordQueueManager.getQueueProducers(config.getRelatedOutputQueueName()).length
					);
		}
		if( !(	recordQueueManager.checkForQueueProducer(config.getRelatedOutputQueueName(), config.getId()) ||
				recordQueueManager.registerProducer(config.getRelatedOutputQueueName(), config.getId())
			 )
		  )
		{
			throw new Exception("Failed to register producer " + config.getId() + " with queue " + config.getRelatedOutputQueueName());
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
	
	public void cleanupRelatedInputQueue(MetricConfig config, RecordQueueManager recordQueueManager){
		if(config.getRelatedSelectorEnabled()){
			recordQueueManager.unregisterConsumer(config.getRelatedInputQueueName(), config.getId());
			recordQueueManager.deleteQueue(config.getRelatedInputQueueName());
		}
	}

	public void cleanupRelatedOutputQueue(MetricConfig config, RecordQueueManager recordQueueManager){
		if(config.getRelatedSelectorEnabled()){
			recordQueueManager.unregisterProducer(config.getRelatedOutputQueueName(), config.getId());
			recordQueueManager.deleteQueue(config.getRelatedOutputQueueName());
		}
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		if(startTime==-1){
			startTime = System.currentTimeMillis();
			lastStatus = startTime;
		}
		if(!isGatherMetric()) {
			System.err.println("MetricAction-" + System.identityHashCode(this) + ": Received request to run metric while it is disabled");
		} else {
			if(DEBUG_ENABLED)
				System.err.println("MetricAction-" + System.identityHashCode(this) + ": Started metric action " + this.id);
			try {
				if(selector.equals(Constants.SEQUENTIAL_SELECTOR)){
					try {
						selectSequentialRecords();
					} catch (Exception e) {
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running sequential record selection for " + this.id);
						throw e;
					}
				} else if(selector.equals(Constants.CUMULATIVE_SELECTOR)){
					try {
						selectCumulativeRecords();
					} catch (Exception e) {
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running cumulative record selection for " + this.id);
						throw e;
					}
				} else if(selector.equals(Constants.TIME_SELECTOR)){
					try {
						selectTimeSeparatedRecords();
					} catch (Exception e) {
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running time separated record selection for " + this.id);
						throw e;
					}
				} else if(selector.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR)){
					try {
						selectSequentialRecordsWithQualifier();
					} catch (Exception e) {
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running sequential record selection with qualifier for " + this.id);
						throw e;
					}
					//This is not a suitable way to implement cleaning.  Just doing this for right now.
					//TODO: Fix this.
					iterationCount++;
					if(iterationCount == 10){
						cleanRecordMap();
						iterationCount=0;
					}
				} else if(selector.equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR)){
					try {
						selectCumulativeRecordsWithQualifier();
					} catch (Exception e) {
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running cumulative record selection with qualifier for " + this.id);
						throw e;
					}
					//TODO: Fix this
					iterationCount++;
					if(iterationCount == 10){
						cleanRecordMap();
						iterationCount=0;
					}

				} else {
					System.err.println("MetricAction-" + System.identityHashCode(this) + ": Unknown selector: " + selector);
					throw new Exception();
				}
			} catch (Exception e) {
				System.err.println("MetricAction-" + System.identityHashCode(this) + ": Exiting due to exception while processing records:");
				e.printStackTrace();
				return;
			}
			if (metricActionStatusRecordsEnabled &&
				System.currentTimeMillis() >= lastStatus + metricActionStatusRecordFrequency)
			{
				lastStatus = System.currentTimeMillis();
				metricActionStatusRecordQueue.put(id, new MetricActionStatusRecord(id, inRecCntr, outRecCntr, processingFailureCntr, putFailureCntr, otherFailureCntr, runningTimems, startTime));
			}
			schedule.setTimestamps(start, System.currentTimeMillis());
			try{
				schedule.advanceSchedule();
			 	metricActionScheduler.schedule(this);
			} catch (Exception e) {
				//We should never be here};
				System.err.println("Unknown exception:");
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		if(DEBUG_ENABLED)
			System.err.println("MetricAction-" + System.identityHashCode(this) + ": Completed metric action " + id);
	}
	
	public void disable(){
		gatherMetric=false;
		cleanupOutputQueue(config, queueManager);
		cleanupRelatedOutputQueue(config, queueManager);
		cleanupRelatedInputQueue(config, queueManager);
		cleanupInputQueue(config, queueManager);
		cleanupMetricActionStatusRecordQueue(queueManager);
	}

	@Override
	public void selectSequentialRecords() throws Exception{
		long st = System.currentTimeMillis();
		
		while((newRec = inputQueue.get(id, false)) != null){
			inRecCntr++;
			
			//Break if this is the first record read from the input queue and the selector is a type that requires two records
			if(method.equals(Constants.MERGE_RECORDS) && oldRec == null){
				oldRec = newRec;
				break;
			}
			if(method.equals(Constants.MERGE_RECORDS)){
				try {
					Record outputRec = recordProcessor.merge(oldRec, newRec);
					try {
						outputQueue.put(id, outputRec);
						outRecCntr++;
					} catch (Exception e) {
						if(DEBUG_ENABLED){
							System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
							e.printStackTrace();
						}
						putFailureCntr++;
					}
					if(config.getRelatedSelectorEnabled())
						try {
							long st2 = System.currentTimeMillis();
							relatedRecordSelector.selectRelatedRecords(outputRec);
							System.out.println("Related selection took " + (System.currentTimeMillis() - st2));
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
				oldRec = newRec;
			} 
			else if (method.equals(Constants.IS_ABOVE)){
				try {
					if(recordProcessor.isAboveThreshold(newRec, thresholdKey, thresholdValue)){
						try {
							outputQueue.put(id,  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(DEBUG_ENABLED){
								System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
								e.printStackTrace();
							}
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								relatedRecordSelector.selectRelatedRecords(newRec);
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
			}
			else if (method.equals(Constants.IS_BELOW)){
				try {
					if(recordProcessor.isBelowThreshold(newRec, thresholdKey, thresholdValue)){
						try {
							outputQueue.put(id,  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(DEBUG_ENABLED){
								System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
								e.printStackTrace();
							}
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								relatedRecordSelector.selectRelatedRecords(newRec);
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
			}
			else if (method.equals(Constants.IS_EQUAL)){
				try {
					if(recordProcessor.isEqual(newRec, thresholdKey, thresholdValue)){
						try {
							outputQueue.put(id,  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(DEBUG_ENABLED){
								System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
								e.printStackTrace();
							}
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								relatedRecordSelector.selectRelatedRecords(newRec);
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
			}
			else if (method.equals(Constants.IS_NOT_EQUAL)){
				try {
					if(recordProcessor.isNotEqual(newRec, thresholdKey, thresholdValue)){
						try {
							outputQueue.put(id,  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(DEBUG_ENABLED){
								System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
								e.printStackTrace();
							}
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								relatedRecordSelector.selectRelatedRecords(newRec);
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
			}
			else {
				throw new Exception("Method " + method + " is not implemented");
			}
		}
		runningTimems += System.currentTimeMillis() - st;
	}

	@Override
	public void selectCumulativeRecords() throws Exception{
		long st = System.currentTimeMillis();
		long lastRecordPut=0;
		
		while((newRec = inputQueue.get(id, false)) != null){
			inRecCntr++;
			
			if(method.equals(Constants.MERGE_RECORDS) && oldRec == null){
				oldRec = newRec;
				try{
					if(cumulativeSelectorFlushTime==-1){
						outputQueue.put(id,  oldRec);
						outRecCntr++;
					} else
						lastRecordPut = newRec.getTimestamp();
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
						e.printStackTrace();
					}
					putFailureCntr++;
				}
				if(config.getRelatedSelectorEnabled())
					try {
						relatedRecordSelector.selectRelatedRecords(oldRec);
					} catch (Exception e) {
						throw new Exception("Failed to process related records", e);
					}
				break;
			} 
			else if(method.equals(Constants.MERGE_RECORDS)){
				try {
					oldRec = recordProcessor.merge(oldRec, newRec);
					try {
						if(cumulativeSelectorFlushTime==-1){
							outputQueue.put(id, oldRec);
							outRecCntr++;
						} else if(newRec.getTimestamp() - lastRecordPut >= cumulativeSelectorFlushTime){
							outputQueue.put(id, oldRec);
							lastRecordPut = newRec.getTimestamp();
							outRecCntr++;
						}
					} catch (Exception e) {
						if(DEBUG_ENABLED){
							System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
							e.printStackTrace();
						}
						putFailureCntr++;
					}
					if(config.getRelatedSelectorEnabled())
						try {
							relatedRecordSelector.selectRelatedRecords(oldRec);
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					oldRec = newRec;
					processingFailureCntr++;
				}
			} 
			else 
				throw new Exception("Method " + method + " is not implemented");
		}
		runningTimems += System.currentTimeMillis() - st;
	}

	@Override
	public void selectCumulativeRecordsWithQualifier() throws Exception{
		long st = System.currentTimeMillis();
		while((newRec = inputQueue.get(id, false)) != null){
			inRecCntr++;
			Record outputRec = null;
			if(recordMap.containsKey(newRec.getValueForQualifier(selectorQualifierKey))){
				if(lastSeenIterationMap.get(newRec.getValueForQualifier(selectorQualifierKey)).longValue() == lastSeenCount)
					lastSeenCount++;
				try{
					outputRec = recordProcessor.merge(recordMap.get(newRec.getValueForQualifier(selectorQualifierKey)), newRec);
					try {	
						if (cumulativeSelectorFlushTime != -1 &&
							!lastPutTimeMap.containsKey(outputRec.getValueForQualifier(selectorQualifierKey))){
							lastPutTimeMap.put(outputRec.getValueForQualifier(selectorQualifierKey), new Long(outputRec.getTimestamp()));
						}
						else if (cumulativeSelectorFlushTime != -1 && 
							newRec.getTimestamp() - lastPutTimeMap.get(outputRec.getValueForQualifier(selectorQualifierKey)).longValue() >= cumulativeSelectorFlushTime){
							outputQueue.put(id, outputRec);
							lastPutTimeMap.put(outputRec.getValueForQualifier(selectorQualifierKey), new Long(outputRec.getTimestamp()));
							outRecCntr++;
							if(config.getRelatedSelectorEnabled())
								try {
									relatedRecordSelector.selectRelatedRecords(outputRec);
								} catch (Exception e) {
									throw new Exception("Failed to process related records", e);
								}
						} else if (cumulativeSelectorFlushTime == -1){
							outputQueue.put(id, outputRec);
							outRecCntr++;
							if(config.getRelatedSelectorEnabled())
								try {
									relatedRecordSelector.selectRelatedRecords(outputRec);
								} catch (Exception e) {
									throw new Exception("Failed to process related records", e);
								}
						}
					} catch (Exception e) {
						putFailureCntr++;
						if(DEBUG_ENABLED){
							System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
			}
			if(!recordMap.containsKey(newRec.getValueForQualifier(selectorQualifierKey))){
				if(outputRec!= null){
					if (cumulativeSelectorFlushTime != -1) {
						lastPutTimeMap.put(outputRec.getValueForQualifier(selectorQualifierKey), new Long(outputRec.getTimestamp()));
					}
				} else {
					if (cumulativeSelectorFlushTime != -1) {
						lastPutTimeMap.put(newRec.getValueForQualifier(selectorQualifierKey), new Long(newRec.getTimestamp()));
					}
				}
			}
			if(outputRec != null) {
				recordMap.put(outputRec.getValueForQualifier(selectorQualifierKey), outputRec);
				if (cumulativeSelectorFlushTime != -1 && !lastPutTimeMap.containsKey(outputRec.getValueForQualifier(selectorQualifierKey)))
					lastPutTimeMap.put(outputRec.getValueForQualifier(selectorQualifierKey), new Long(outputRec.getTimestamp()));
			} else {
				recordMap.put(newRec.getValueForQualifier(selectorQualifierKey), newRec);
				if (cumulativeSelectorFlushTime != -1  && !lastPutTimeMap.containsKey(newRec.getValueForQualifier(selectorQualifierKey)))
					lastPutTimeMap.put(newRec.getValueForQualifier(selectorQualifierKey), new Long(newRec.getTimestamp()));
			}
			lastSeenIterationMap.put(newRec.getValueForQualifier(selectorQualifierKey), new Long(lastSeenCount));
			
			
		}
		runningTimems += System.currentTimeMillis() - st;
	}
	
	@Override
	public void selectTimeSeparatedRecords() throws Exception {
		long st = System.currentTimeMillis();
		while((newRec = inputQueue.get(id, false)) != null){
			inRecCntr++;
			recordList.add(newRec);
			
			while
			(	recordList.size() > 1 && 
					newRec.getTimestamp() - recordList.getFirst().getTimestamp() >= timeSelectorMinDelta &&
					(	timeSelectorMaxDelta==-1 || 
						newRec.getTimestamp() - recordList.getFirst().getTimestamp() <= timeSelectorMaxDelta
					)
			)
			{
				try{
					Record outputRec = recordProcessor.merge(recordList.getFirst(), newRec);
					try {
						outputQueue.put(id, outputRec);
						outRecCntr++;
					} catch (Exception e) {
						putFailureCntr++;
						if(DEBUG_ENABLED){
							System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
							e.printStackTrace();
						}
					}
					if(config.getRelatedSelectorEnabled())
						try {
							relatedRecordSelector.selectRelatedRecords(outputRec);
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
				recordList.removeFirst();
			} 
		}
		runningTimems += System.currentTimeMillis() - st;
	}
	
	@Override
	public void selectSequentialRecordsWithQualifier() throws Exception {
		long st = System.currentTimeMillis();
		while((newRec = inputQueue.get(id, false)) != null){
			inRecCntr++;
			if(recordMap.containsKey(newRec.getValueForQualifier(selectorQualifierKey))){
				if(lastSeenIterationMap.get(newRec.getValueForQualifier(selectorQualifierKey)).longValue() == lastSeenCount)
					lastSeenCount++;
				try{
					Record outputRec = recordProcessor.merge(recordMap.get(newRec.getValueForQualifier(selectorQualifierKey)), newRec);
					try {
						outputQueue.put(id, outputRec);
						outRecCntr++;
					} catch (Exception e) {
						putFailureCntr++;
						if(DEBUG_ENABLED){
							System.err.println("MetricAction-" + System.identityHashCode(this) + ": Queue put failure:");
							e.printStackTrace();
						}
					}
					if(config.getRelatedSelectorEnabled())
						try {
							relatedRecordSelector.selectRelatedRecords(outputRec);
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					if(DEBUG_ENABLED){
						System.err.println("MetricAction-" + System.identityHashCode(this) + ": Record processing failure:");
						e.printStackTrace();
					}
					processingFailureCntr++;
				}
			}
			recordMap.put(newRec.getValueForQualifier(selectorQualifierKey), newRec);
			lastSeenIterationMap.put(newRec.getValueForQualifier(selectorQualifierKey), new Long(lastSeenCount));
		}
		runningTimems += System.currentTimeMillis() - st;
	}
	
	public void cleanRecordMap() {
		Iterator<Map.Entry<String, Long>> i = lastSeenIterationMap.entrySet().iterator();
		LinkedList<String> elementsToRemove = new LinkedList<String>();
		while(i.hasNext()){
			Map.Entry<String, Long> e = i.next();
			if(e.getValue().longValue() < lastSeenCount - 1)
				elementsToRemove.add(e.getKey());
		}
		while(elementsToRemove.size() != 0){
			lastSeenIterationMap.remove(elementsToRemove.getFirst());
			lastPutTimeMap.remove(elementsToRemove.getFirst());
			recordMap.remove(elementsToRemove.getFirst());
			elementsToRemove.removeFirst();
		}
	}

	public boolean isGatherMetric() {
		return gatherMetric;
	}

	public void setGatherMetric(boolean isGatherMetric) {
		this.gatherMetric = isGatherMetric;
	}

	public MetricAction(String id) {
		this.id = id;
	}

	public long getNextScheduleTime() throws Exception{
		return schedule.getNextTime();
	}
	public String getId() {
		return id;
	}
	
	public int getPeriodicity(){
		return periodicity;
	}

}
