package com.mapr.distiller.server.metricactions;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.mapr.distiller.server.persistance.Persistor;
import com.mapr.distiller.server.persistance.MapRDBSyncPersistor;
import com.mapr.distiller.server.persistance.LocalFileSystemPersistor;
import com.mapr.distiller.server.queues.MapRDBInputRecordQueue;
import com.mapr.distiller.server.queues.LocalFileInputRecordQueue;

public class MetricAction implements Runnable, MetricsSelectable {
	private static final Logger LOG = LoggerFactory
			.getLogger(MetricAction.class);
	private Persistor persistor;
	private Schedule schedule;
	private MetricActionScheduler metricActionScheduler;
	
	protected volatile boolean metricEnabled;
	protected RecordQueue inputQueue;
	protected RecordQueue outputQueue;
	protected RecordQueue relatedInputQueue;
	protected RecordQueue relatedOutputQueue;
	protected RecordQueue metricActionStatusRecordQueue;

	protected RecordProcessor<Record> recordProcessor;
	protected RelatedRecordSelector<Record, Record> relatedRecordSelector;
	
	private Record oldRec, newRec;
	private LinkedList<Record> recordList;
	private HashMap<String, Record> recordMap;
	private HashMap<String, Long> lastSeenIterationMap;
	private HashMap<String, Long> lastPutTimeMap;
	private long lastSeenCount;
	
	private long inRecCntr, outRecCntr, processingFailureCntr, putFailureCntr, otherFailureCntr, runningTimems, startTime;
	
	private MetricConfig config;
	private RecordQueueManager queueManager;

	private long lastStatus = -1l;
	private long iterationCount = 0l;
	
	public MetricConfig getConfig(){
		return config;
	}

	public long[] getCounters(){
		return new long[] 
		{ 	inRecCntr, 
			outRecCntr, 
			processingFailureCntr, 
			putFailureCntr, 
			otherFailureCntr, 
			runningTimems, 
			startTime
		};
	}
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
		
		this.metricEnabled=false; //Ignore whats in the config because this gets set to true by calling to enableMetric() which does the necessary setup work.
		this.schedule = new Schedule(config.getPeriodicity(),
									 1d,	//Hard coding here to disable adaptive scheduling (not sure if design is good yet))
									 0d );	//Hard coding here to disable adaptive scheduling 
		if(!config.getSelector().equals(Constants.PERSISTING_SELECTOR)){
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
	
				case Constants.DIFFERENTIAL_VALUE_RECORD_PROCESSOR:
					this.recordProcessor = new DifferentialValueRecordProcessor();
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
					
				case Constants.PROCESS_EFFICIENCY_RECORD_PROCESSOR:
					this.recordProcessor = new ProcessEfficiencyRecordProcessor();
					break;
					
				case Constants.PASSTHROUGH_RECORD_PROCESSOR:
					this.recordProcessor = new PassthroughRecordProcessor();
					break;
	
				case Constants.LOAD_AVERAGE_RECORD_PROCESSOR:
					this.recordProcessor = new LoadAverageRecordProcessor();
					break;
	
				default:
					throw new Exception("Failed to construct MetricAction due to unknown RecordProcessor type: " + config.getProcessor());
			}
		}

		if(config.getSelector().equals(Constants.PERSISTING_SELECTOR)){
			if(config.getPersistorName().equals(Constants.MAPRDB)){
				try {
					/**
					this.persistor = new MapRDBPersistor(	config.getOutputQueue(), //Which in this case is the path to the MapRDB table
															config.getMapRDBWorkDirEnabled(),
															config.getMapRDBWorkDirBatchSize(),	
															config.getMapRDBCreateTables(),
															config.getId() 
															);
					**/
					this.persistor = new MapRDBSyncPersistor(	config.getOutputQueue(), //Which in this case is the path to the MapRDB table
																config.getMapRDBWorkDirEnabled(),
																config.getMapRDBWorkDirBatchSize(),	
																config.getMapRDBCreateTables(),
																config.getId() 
																);
					
				} catch (Exception e) {
					//throw new Exception("Failed to construct MapRDBPersistor", e);
					throw new Exception("Failed to construct MapRDBSyncPersistor", e);
				}
				//config.getMapRDBPersistanceManager().registerPersistor((MapRDBPersistor)this.persistor);
				config.getMapRDBSyncPersistanceManager().registerPersistor((MapRDBSyncPersistor)this.persistor);

			} else if(config.getPersistorName().equals(Constants.LOCAL_FILE_SYSTEM)){
				try {
					this.persistor = new LocalFileSystemPersistor(	config.getLfspWriteBatchSize(),
																config.getLfspFlushFrequency(),
																config.getLfspRecordsPerFile(),	
																config.getId() 
																);
				} catch (Exception e) {
					throw new Exception("Failed to construct LocalFileSystemPersistor", e);
				}
				config.getLocalFileSystemPersistanceManager().registerPersistor((LocalFileSystemPersistor)this.persistor);
			} else {
				throw new Exception("Unknown value for " + Constants.PERSISTING_SELECTOR + ": " + config.getPersistorName());
			}
		}
		
		if(config.getInputQueueType()!=null && config.getInputQueueType().equals(Constants.MAPRDB_INPUT_RECORD_QUEUE)){
			try {
				this.inputQueue = new MapRDBInputRecordQueue(	config.getId(),
																config.getInputQueue(),
																config.getMapRDBInputQueueScanner(),
																config.getMapRDBInputQueueScanStartTime(),
																config.getMapRDBInputQueueScanEndTime()
															);
				this.inputQueue.registerConsumer(config.getId());
			} catch (Exception e) {
				throw new Exception("Failed to construct MapRDBInputRecordQueue", e);
			}
		} else if(config.getInputQueueType()!=null && config.getInputQueueType().equals(Constants.LOCAL_FILE_INPUT_RECORD_QUEUE)){
			try {
				this.inputQueue = new LocalFileInputRecordQueue(	config.getId(),
																config.getLocalFileInputMetricName(),
																config.getInputQueue(),					//Which in this case is the local FS dir containing the input files
																config.getLocalFileInputQueueScanner(),
																config.getLocalFileInputQueueStartTimestamp(),
																config.getLocalFileInputQueueEndTimestamp()
															);
				if(!this.inputQueue.registerConsumer(config.getId())){
					throw new Exception("Failed to register " + config.getId() + " with inputQueue " + inputQueue.getQueueName());
				}
			} catch (Exception e) {
				throw new Exception("Failed to construct LocalFileInputRecordQueue", e);
			}
		}
		if (config.getSelector().equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) &&
			config.getMethod().equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)){
			LOG.warn("The use of " + Constants.INPUT_RECORD_SELECTOR + "=" + Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR + 
					" in conjunction with the use of " + Constants.INPUT_RECORD_PROCESSOR_METHOD + "=" + Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE + 
					" can produce output records out of chronological order. Most parts of the Distiller system presume" + 
					" input streams of Records are in chronological order unless explicitly specified otherwise, and thus" + 
					" providing stream of output records from this MetricAction as input to another MetricAction is likely" + 
					" to fail, error, lose records, etc., unless that subsequent MetricAction was explicitly configured to" + 
					" expect input records that are out of chronological order. As of the time of this writing, I work" + 
					" around this by persisting the output Records from this type of MetricAction, then periodically" + 
					" sorting sections of output and replaying them into the subsequent MetricAction.  But that approach" + 
					" is not without it's own problems.");
		}
	}
	
	private void cleanupMetricActionStatusRecordQueue(RecordQueueManager recordQueueManager){
		recordQueueManager.unregisterProducer(Constants.METRIC_ACTION_STATS_QUEUE_NAME, config.getId());
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
		if( !(	recordQueueManager.checkForQueueProducer(Constants.METRIC_ACTION_STATS_QUEUE_NAME, config.getId()) ||
				recordQueueManager.registerProducer(Constants.METRIC_ACTION_STATS_QUEUE_NAME, config.getId())
			 )
		  )
		{
			throw new Exception("Failed to register producer " + config.getId() + " with queue " + Constants.METRIC_ACTION_STATS_QUEUE_NAME);
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
		if(!metricEnabled) {
			LOG.warn("MetricAction-" + System.identityHashCode(this) + ": Received request to run metric while it is disabled");
		} else {
			if(LOG.isDebugEnabled())
				LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Started metric action " + config.getId());
			try {
				if(config.getProcessor()!=null && config.getProcessor().equals(Constants.PASSTHROUGH_RECORD_PROCESSOR)){
					try {
						passthroughRecords();
					} catch (Exception e) {
						LOG.error("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running passthrough record selection for " + config.getId(), e);
						throw e;
					}
				} else if(config.getSelector().equals(Constants.SEQUENTIAL_SELECTOR)){
					try {
						selectSequentialRecords();
					} catch (Exception e) {
						LOG.error("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running sequential record selection for " + config.getId(), e);
						throw e;
					}
				} else if(config.getSelector().equals(Constants.CUMULATIVE_SELECTOR)){
					try {
						selectCumulativeRecords();
					} catch (Exception e) {
						LOG.error("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running cumulative record selection for " + config.getId(), e);
						throw e;
					}
				} else if(config.getSelector().equals(Constants.TIME_SELECTOR)){
					try {
						selectTimeSeparatedRecords();
					} catch (Exception e) {
						LOG.error("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running time separated record selection for " + config.getId(), e);
						throw e;
					}
				} else if(config.getSelector().equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR)){
					try {
						selectSequentialRecordsWithQualifier();
					} catch (Exception e) {
						LOG.error("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running sequential record selection with qualifier for " + config.getId(), e);
						throw e;
					}
					//This is not a suitable way to implement cleaning.  Just doing this for right now.
					//TODO: Fix this.
					iterationCount++;
					if(iterationCount == 10){
						cleanRecordMap();
						iterationCount=0;
					}
				} else if(config.getSelector().equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR)){
					try {
						selectCumulativeRecordsWithQualifier();
					} catch (Exception e) {
						LOG.error("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while running cumulative record selection with qualifier for " + config.getId(), e);
						throw e;
					}
					//TODO: Fix this
					iterationCount++;
					if(iterationCount == 10){
						cleanRecordMap();
						iterationCount=0;
					}
				} else if(config.getSelector().equals(Constants.PERSISTING_SELECTOR)){
					try {
						persistRecords();
					} catch (Exception e) {
						LOG.error("MetricAction-" + System.identityHashCode(this) + ": Caught an exception while persisting records for " + config.getId(), e);
						throw e;
					}
				} else {
					LOG.error("MetricAction-" + System.identityHashCode(this) + ": Unknown selector: " + config.getSelector());
					throw new Exception();
				}
			} catch (Exception e) {
				LOG.error("MetricAction-" + System.identityHashCode(this) + ": Exiting due to exception while processing records", e);
				return;
			}
			if (config.getMetricActionStatusRecordsEnabled() &&
				System.currentTimeMillis() >= lastStatus + config.getMetricActionStatusRecordFrequency())
			{
				lastStatus = System.currentTimeMillis();
				metricActionStatusRecordQueue.put(config.getId(), new MetricActionStatusRecord(config.getId(), inRecCntr, outRecCntr, processingFailureCntr, putFailureCntr, otherFailureCntr, runningTimems, startTime));
			}
			
			//This next section of code needs to be the last section of code in the run method.  Update the scheduling and exit.
			//The Coordinator will start to run this again once its in the schedule, even if this hasn't exited yet.
			//That can happen depending on when threads are suspended.
			schedule.setTimestamps(start, System.currentTimeMillis());
			try{
				schedule.advanceSchedule();
				metricActionScheduler.schedule(this);
			} catch (Exception e) {
				//We should never be here
				LOG.error("MetricAction-" + System.identityHashCode(this) + ": Unhandled exception", e);
				System.exit(1);
			}
		}
		if(LOG.isDebugEnabled())
			LOG.debug(" MetricAction-" + System.identityHashCode(this) + ": Completed metric action " + config.getId() + " and rescheduled with " + printSchedule());
	}
	
	public void advanceSchedule(){
		try {
			schedule.advanceSchedule();
			metricActionScheduler.schedule(this);
		} catch (Exception e){}
	}

	public void disableMetric(){
		metricEnabled=false;
		cleanupMetricActionStatusRecordQueue(queueManager);
		
		//Cleanup output queues, only if the selector is NOT a persisting selector.
		//Persisting selectors do not have output queues, they have persistors that are flushed/disabled differently.
		if( config.getSelector() == null
			||
			!config.getSelector().equals(Constants.PERSISTING_SELECTOR)
		  )
		{
			cleanupOutputQueue(config, queueManager);
			cleanupRelatedOutputQueue(config, queueManager);
		}
		//Cleanup input queues, only if the input queue type is not a persistance input queue.
		//Persistance input queues are members of the metric action, they are not record queues
		//managed/tracked by the RecordQueueManager.
		if( config.getInputQueueType() == null 
			||
			( !config.getInputQueueType().equals(Constants.LOCAL_FILE_INPUT_RECORD_QUEUE)
			  &&
			  !config.getInputQueueType().equals(Constants.MAPRDB_INPUT_RECORD_QUEUE)
			)
		  )
		{
			cleanupInputQueue(config, queueManager);
			cleanupRelatedInputQueue(config, queueManager);
		}
	}

	@Override
	public void persistRecords() throws Exception{
		long st = System.currentTimeMillis();
		while((newRec = inputQueue.get(config.getId(), false)) != null){
			inRecCntr++;
			try{
				if(newRec==null){
					System.err.println("Have null newRec");
					(new Exception()).printStackTrace();
				}
				persistor.persist(newRec);
				outRecCntr++;
			} catch (Exception e) {
				putFailureCntr++;
			}
		}
		runningTimems += System.currentTimeMillis() - st;
	}
	
	public void passthroughRecords() throws Exception{
		long st = System.currentTimeMillis();

		while( outputQueue.queueSize() < outputQueue.getQueueRecordCapacity() &&
			   (newRec = inputQueue.get(config.getId(), false)) != null){
			inRecCntr++;
			try {
				outputQueue.put(config.getId(), newRec);
				outRecCntr++;
			} catch (Exception e) {
				if(LOG.isDebugEnabled())
					LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
				putFailureCntr++;
			}
			if(config.getRelatedSelectorEnabled()){
				try {
					long[] ret = relatedRecordSelector.selectRelatedRecords(newRec);
					inRecCntr += ret[0];
					outRecCntr += ret[1];
					putFailureCntr += ret[2];
					otherFailureCntr += ret[3];
				} catch (Exception e) {
					throw new Exception("Failed to process related records", e);
				}
			}
		}
		runningTimems += System.currentTimeMillis() - st;
	}
	
	@Override
	public void selectSequentialRecords() throws Exception{
		long st = System.currentTimeMillis();

		while( (newRec = inputQueue.get(config.getId(), false)) != null){
			inRecCntr++;
			//Break if this is the first record read from the input queue and the selector is a type that requires two records
			if 
			( ( config.getMethod().equals(Constants.MERGE_RECORDS) ||
				config.getMethod().equals(Constants.DIFFERENTIAL) ||
				config.getMethod().equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)
			  )
			  && 
			  oldRec == null
			)
			{
				oldRec = newRec;
				break;
			}
			if(config.getMethod().equals(Constants.MERGE_RECORDS)){
				try {
					Record outputRec = recordProcessor.merge(oldRec, newRec);
					try {
						outputQueue.put(config.getId(), outputRec);
						outRecCntr++;
					} catch (Exception e) {
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
						putFailureCntr++;
					}
					if(config.getRelatedSelectorEnabled())
						try {
							long[] ret = relatedRecordSelector.selectRelatedRecords(outputRec);
							inRecCntr += ret[0];
							outRecCntr += ret[1];
							putFailureCntr += ret[2];
							otherFailureCntr += ret[3];
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
				oldRec = newRec;
			} 
			else if(config.getMethod().equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)){
				try {
					Record[] outputRecs = recordProcessor.mergeChronologicallyConsecutive(oldRec, newRec);
					newRec = outputRecs[0];
					if(outputRecs[1] != null) {
						try {
							outputQueue.put(config.getId(), outputRecs[1]);
							outRecCntr++;
						} catch (Exception e) {
							if(LOG.isDebugEnabled())
								LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								long[] ret = relatedRecordSelector.selectRelatedRecords(outputRecs[1]);
								inRecCntr += ret[0];
								outRecCntr += ret[1];
								putFailureCntr += ret[2];
								otherFailureCntr += ret[3];
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
				oldRec = newRec;
			} 
			else if(config.getMethod().equals(Constants.DIFFERENTIAL)){
				try {
					Record outputRec = recordProcessor.diff(oldRec, newRec, config.getThresholdKey());
					try {
						outputQueue.put(config.getId(), outputRec);
						outRecCntr++;
					} catch (Exception e) {
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
						putFailureCntr++;
					}
					if(config.getRelatedSelectorEnabled())
						try {
							long[] ret = relatedRecordSelector.selectRelatedRecords(outputRec);
							inRecCntr += ret[0];
							outRecCntr += ret[1];
							putFailureCntr += ret[2];
							otherFailureCntr += ret[3];
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
				oldRec = newRec;
			} 
			
			else if (config.getMethod().equals(Constants.IS_ABOVE)){
				try {
					if(recordProcessor.isAboveThreshold(newRec, config.getThresholdKey(), config.getThresholdValue())){
						try {
							outputQueue.put(config.getId(),  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(LOG.isDebugEnabled())
								LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								long[] ret = relatedRecordSelector.selectRelatedRecords(newRec);
								inRecCntr += ret[0];
								outRecCntr += ret[1];
								putFailureCntr += ret[2];
								otherFailureCntr += ret[3];
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
			}
			else if (config.getMethod().equals(Constants.IS_BELOW)){
				try {
					if(recordProcessor.isBelowThreshold(newRec, config.getThresholdKey(), config.getThresholdValue())){
						try {
							outputQueue.put(config.getId(),  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(LOG.isDebugEnabled())
								LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								long[] ret = relatedRecordSelector.selectRelatedRecords(newRec);
								inRecCntr += ret[0];
								outRecCntr += ret[1];
								putFailureCntr += ret[2];
								otherFailureCntr += ret[3];
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
			}
			else if (config.getMethod().equals(Constants.IS_EQUAL)){
				try {
					if(recordProcessor.isEqual(newRec, config.getThresholdKey(), config.getThresholdValue())){
						try {
							outputQueue.put(config.getId(),  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(LOG.isDebugEnabled())
								LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								long[] ret = relatedRecordSelector.selectRelatedRecords(newRec);
								inRecCntr += ret[0];
								outRecCntr += ret[1];
								putFailureCntr += ret[2];
								otherFailureCntr += ret[3];
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
			}
			else if (config.getMethod().equals(Constants.IS_NOT_EQUAL)){
				try {
					if(recordProcessor.isNotEqual(newRec, config.getThresholdKey(), config.getThresholdValue())){
						try {
							outputQueue.put(config.getId(),  newRec);
							outRecCntr++;
						} catch (Exception e) {
							if(LOG.isDebugEnabled())
								LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
							putFailureCntr++;
						}
						if(config.getRelatedSelectorEnabled())
							try {
								long[] ret = relatedRecordSelector.selectRelatedRecords(newRec);
								inRecCntr += ret[0];
								outRecCntr += ret[1];
								putFailureCntr += ret[2];
								otherFailureCntr += ret[3];
							} catch (Exception e) {
								throw new Exception("Failed to process related records", e);
							}
					}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
			}
			else if (config.getMethod().equals(Constants.CONVERT)){
				try {
					Record outputRec = recordProcessor.convert(newRec);
					try {
						outputQueue.put(config.getId(), outputRec);
						outRecCntr++;
					} catch (Exception e) {
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
						putFailureCntr++;
					}
					if(config.getRelatedSelectorEnabled())
						try {
							long[] ret = relatedRecordSelector.selectRelatedRecords(outputRec);
							inRecCntr += ret[0];
							outRecCntr += ret[1];
							putFailureCntr += ret[2];
							otherFailureCntr += ret[3];
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					LOG.info("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
			}
			else {
				throw new Exception("Method " + config.getMethod() + " is not implemented");
			}
		}
		runningTimems += System.currentTimeMillis() - st;
	}

	@Override
	public void selectCumulativeRecords() throws Exception{
		long st = System.currentTimeMillis();
		long lastRecordPut=0;
		
		while((newRec = inputQueue.get(config.getId(), false)) != null){
			inRecCntr++;
			
			if(config.getMethod().equals(Constants.MERGE_RECORDS) && oldRec == null){
				oldRec = newRec;
				try{
					if(config.getCumulativeSelectorFlushTime()==-1){
						outputQueue.put(config.getId(),  oldRec);
						outRecCntr++;
					} else
						lastRecordPut = newRec.getTimestamp();
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
					putFailureCntr++;
				}
				if(config.getRelatedSelectorEnabled())
					try {
						long[] ret = relatedRecordSelector.selectRelatedRecords(oldRec);
						inRecCntr += ret[0];
						outRecCntr += ret[1];
						putFailureCntr += ret[2];
						otherFailureCntr += ret[3];
					} catch (Exception e) {
						throw new Exception("Failed to process related records", e);
					}
				break;
			} 
			else if(config.getMethod().equals(Constants.MERGE_RECORDS)){
				try {
					oldRec = recordProcessor.merge(oldRec, newRec);
					try {
						if(config.getCumulativeSelectorFlushTime()==-1){
							outputQueue.put(config.getId(), oldRec);
							outRecCntr++;
						} else if(newRec.getTimestamp() - lastRecordPut >= config.getCumulativeSelectorFlushTime()){
							outputQueue.put(config.getId(), oldRec);
							lastRecordPut = newRec.getTimestamp();
							outRecCntr++;
						}
					} catch (Exception e) {
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
						putFailureCntr++;
					}
					if(config.getRelatedSelectorEnabled())
						try {
							long[] ret = relatedRecordSelector.selectRelatedRecords(oldRec);
							inRecCntr += ret[0];
							outRecCntr += ret[1];
							putFailureCntr += ret[2];
							otherFailureCntr += ret[3];
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					oldRec = newRec;
					processingFailureCntr++;
				}
			} 
			else 
				throw new Exception("Method " + config.getMethod() + " is not implemented");
		}
		runningTimems += System.currentTimeMillis() - st;
	}

	@Override
	public void selectCumulativeRecordsWithQualifier() throws Exception{
		long st = System.currentTimeMillis();
		while((newRec = inputQueue.get(config.getId(), false)) != null){
			inRecCntr++;
			Record outputRec = null;
			if(recordMap.containsKey(newRec.getValueForQualifier(config.getSelectorQualifierKey()))){
				if(lastSeenIterationMap.get(newRec.getValueForQualifier(config.getSelectorQualifierKey())).longValue() == lastSeenCount)
					lastSeenCount++;
				try{
					outputRec = recordProcessor.merge(recordMap.get(newRec.getValueForQualifier(config.getSelectorQualifierKey())), newRec);
					try {	
						if (config.getCumulativeSelectorFlushTime() != -1 &&
							!lastPutTimeMap.containsKey(outputRec.getValueForQualifier(config.getSelectorQualifierKey()))){
							lastPutTimeMap.put(outputRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(outputRec.getTimestamp()));
						}
						else if (config.getCumulativeSelectorFlushTime() != -1 && 
							newRec.getTimestamp() - lastPutTimeMap.get(outputRec.getValueForQualifier(config.getSelectorQualifierKey())).longValue() >= config.getCumulativeSelectorFlushTime()){
							outputQueue.put(config.getId(), outputRec);
							lastPutTimeMap.put(outputRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(outputRec.getTimestamp()));
							outRecCntr++;
							if(config.getRelatedSelectorEnabled())
								try {
									long[] ret = relatedRecordSelector.selectRelatedRecords(outputRec);
									inRecCntr += ret[0];
									outRecCntr += ret[1];
									putFailureCntr += ret[2];
									otherFailureCntr += ret[3];
								} catch (Exception e) {
									throw new Exception("Failed to process related records", e);
								}
						} else if (config.getCumulativeSelectorFlushTime() == -1){
							outputQueue.put(config.getId(), outputRec);
							outRecCntr++;
							if(config.getRelatedSelectorEnabled())
								try {
									long[] ret = relatedRecordSelector.selectRelatedRecords(outputRec);
									inRecCntr += ret[0];
									outRecCntr += ret[1];
									putFailureCntr += ret[2];
									otherFailureCntr += ret[3];
								} catch (Exception e) {
									throw new Exception("Failed to process related records", e);
								}
						}
					} catch (Exception e) {
						putFailureCntr++;
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
					}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
					processingFailureCntr++;
				}
			}
			if(!recordMap.containsKey(newRec.getValueForQualifier(config.getSelectorQualifierKey()))){
				if(outputRec!= null){
					if (config.getCumulativeSelectorFlushTime() != -1) {
						lastPutTimeMap.put(outputRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(outputRec.getTimestamp()));
					}
				} else {
					if (config.getCumulativeSelectorFlushTime() != -1) {
						lastPutTimeMap.put(newRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(newRec.getTimestamp()));
					}
				}
			}
			if(outputRec != null) {
				recordMap.put(outputRec.getValueForQualifier(config.getSelectorQualifierKey()), outputRec);
				if (config.getCumulativeSelectorFlushTime() != -1 && !lastPutTimeMap.containsKey(outputRec.getValueForQualifier(config.getSelectorQualifierKey())))
					lastPutTimeMap.put(outputRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(outputRec.getTimestamp()));
			} else {
				recordMap.put(newRec.getValueForQualifier(config.getSelectorQualifierKey()), newRec);
				if (config.getCumulativeSelectorFlushTime() != -1  && !lastPutTimeMap.containsKey(newRec.getValueForQualifier(config.getSelectorQualifierKey())))
					lastPutTimeMap.put(newRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(newRec.getTimestamp()));
			}
			lastSeenIterationMap.put(newRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(lastSeenCount));
			
			
		}
		runningTimems += System.currentTimeMillis() - st;
	}
	
	@Override
	public void selectTimeSeparatedRecords() throws Exception {
		long st = System.currentTimeMillis();
		while((newRec = inputQueue.get(config.getId(), false)) != null){
			inRecCntr++;
			recordList.add(newRec);
			
			while
			(	recordList.size() > 1 && 
					newRec.getTimestamp() - recordList.getFirst().getTimestamp() >= config.getTimeSelectorMinDelta() &&
					(	config.getTimeSelectorMaxDelta()==-1 || 
						newRec.getTimestamp() - recordList.getFirst().getTimestamp() <= config.getTimeSelectorMaxDelta()
					)
			)
			{
				try{
					Record outputRec = recordProcessor.merge(recordList.getFirst(), newRec);
					try {
						outputQueue.put(config.getId(), outputRec);
						outRecCntr++;
					} catch (Exception e) {
						putFailureCntr++;
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
					}
					if(config.getRelatedSelectorEnabled()){
						try {
							long[] ret = relatedRecordSelector.selectRelatedRecords(outputRec);
							inRecCntr += ret[0];
							outRecCntr += ret[1];
							putFailureCntr += ret[2];
							otherFailureCntr += ret[3];
						} catch (Exception e) {
							throw new Exception("Failed to process related records", e);
						}
					}
				} catch (Exception e) {
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
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
		while((newRec = inputQueue.get(config.getId(), false)) != null){
			inRecCntr++;
			try {
				if(recordMap.containsKey(newRec.getValueForQualifier(config.getSelectorQualifierKey()))){
					if(lastSeenIterationMap.get(newRec.getValueForQualifier(config.getSelectorQualifierKey())).longValue() == lastSeenCount)
						lastSeenCount++;
					try{
						Record outputRec = null;
						Record[] outputRecs = null;
						if(config.getMethod().equals(Constants.MERGE_RECORDS)){
							outputRec = recordProcessor.merge(recordMap.get(newRec.getValueForQualifier(config.getSelectorQualifierKey())), newRec);
						} else if(config.getMethod().equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)){
							outputRecs = recordProcessor.mergeChronologicallyConsecutive(recordMap.get(newRec.getValueForQualifier(config.getSelectorQualifierKey())), newRec);
							outputRec = outputRecs[1];
							newRec = outputRecs[0];
						} else {
							throw new Exception("Unknown method: " + config.getMethod());
						}
						if(outputRec != null){
							try {
								outputQueue.put(config.getId(), outputRec);
								outRecCntr++;
							} catch (Exception e) {
								putFailureCntr++;
								if(LOG.isDebugEnabled())
									LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
							}
							if(config.getRelatedSelectorEnabled()){
								try {
									long[] ret = relatedRecordSelector.selectRelatedRecords(outputRec);
									inRecCntr += ret[0];
									outRecCntr += ret[1];
									putFailureCntr += ret[2];
									otherFailureCntr += ret[3];
								} catch (Exception e) {
									throw new Exception("Failed to process related records", e);
								}
							}
						}
					} catch (Exception e) {
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Record processing failure", e);
						processingFailureCntr++;
					}
				}
				recordMap.put(newRec.getValueForQualifier(config.getSelectorQualifierKey()), newRec);
				lastSeenIterationMap.put(newRec.getValueForQualifier(config.getSelectorQualifierKey()), new Long(lastSeenCount));
			} catch (Exception e) {
				throw new Exception("Failed to process input record " + newRec.toString(), e);
			}
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
			if (config.getSelector().equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) &&
				config.getMethod().equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)){
				Record r = recordMap.get(elementsToRemove.getFirst());
				if(r != null){
					try {
						outputQueue.put(config.getId(), r);
						outRecCntr++;
					} catch (Exception e) {
						putFailureCntr++;
						if(LOG.isDebugEnabled())
							LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
					}
					if(config.getRelatedSelectorEnabled()){
						try {
							long[] ret = relatedRecordSelector.selectRelatedRecords(r);
							inRecCntr += ret[0];
							outRecCntr += ret[1];
							putFailureCntr += ret[2];
							otherFailureCntr += ret[3];
						} catch (Exception e) {
							LOG.error("Failed to process related records for input record " + r.toString(), e);
						}
					}
				}
			}
			recordMap.remove(elementsToRemove.getFirst());
			elementsToRemove.removeFirst();
		}
	}

	public boolean getMetricEnabled() {
		return metricEnabled;
	}

	public void enableMetric() throws Exception{
		this.metricEnabled = true;
		synchronized(queueManager){
			if 
			( config.getInputQueueType() == null || 
			 !(  config.getInputQueueType().equals(Constants.MAPRDB_INPUT_RECORD_QUEUE) ||
				 config.getInputQueueType().equals(Constants.LOCAL_FILE_INPUT_RECORD_QUEUE)
			  )
			)
			{
				try{
					setupInputQueue(config, queueManager);
					this.inputQueue = queueManager.getQueue(config.getInputQueue());
				} catch (Exception e) {
					cleanupInputQueue(config, queueManager);
					throw new Exception("Failed to setup input queue for config: " + config.toString(), e);
				}
			}
			if(!config.getSelector().equals(Constants.PERSISTING_SELECTOR)){
				try{
					setupRelatedInputQueue(config, queueManager);
					if(config.getRelatedSelectorEnabled())
						this.relatedInputQueue = queueManager.getQueue(config.getRelatedInputQueueName());
				} catch (Exception e) {
					cleanupRelatedInputQueue(config, queueManager);
					cleanupInputQueue(config, queueManager);
					throw new Exception("Failed to setup related queue for config: " + config.toString(), e);
				}
				try{
					setupRelatedOutputQueue(config, queueManager);
					if(config.getRelatedSelectorEnabled()){
						this.relatedOutputQueue = queueManager.getQueue(config.getRelatedOutputQueueName());
						switch(config.getRelatedSelectorName()){
						case Constants.BASIC_RELATED_RECORD_SELECTOR:
							this.relatedRecordSelector = new BasicRelatedRecordSelector(config.getId(), 
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
				this.outputQueue = queueManager.getQueue(config.getOutputQueue());
			}
			try{
				setupMetricActionStatusRecordQueue(queueManager);
			} catch (Exception e) {
				cleanupMetricActionStatusRecordQueue(queueManager);
				cleanupOutputQueue(config, queueManager);
				cleanupRelatedOutputQueue(config, queueManager);
				cleanupRelatedInputQueue(config, queueManager);
				cleanupInputQueue(config, queueManager);
				throw new Exception("Failed to setup metric action status record queue", e);
			}
		}
	}

	public long getNextScheduleTime() throws Exception{
		return schedule.getNextTime();
	}
	public String getId() {
		return config.getId();
	}
	
	public void flushLastRecords(){
		if (config.getSelector().equals(Constants.SEQUENTIAL_SELECTOR) &&
			config.getMethod().equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE) && 
			oldRec != null){
			try {
				outputQueue.put(config.getId(), oldRec);
				outRecCntr++;
			} catch (Exception e) {
				putFailureCntr++;
				if(LOG.isDebugEnabled())
					LOG.info("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
			}
			if(config.getRelatedSelectorEnabled()){
				try {
					long[] ret = relatedRecordSelector.selectRelatedRecords(oldRec);
					inRecCntr += ret[0];
					outRecCntr += ret[1];
					putFailureCntr += ret[2];
					otherFailureCntr += ret[3];
				} catch (Exception e) {
					LOG.info("MetricAction-" + System.identityHashCode(this) + ": Failed to process related records", e);
				}
			}
		} else if ( config.getSelector().equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) &&
				   	config.getMethod().equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE) ) {
			for(Record r : recordMap.values()) {
				try {
					outputQueue.put(config.getId(), r);
					outRecCntr++;
				} catch (Exception e) {
					putFailureCntr++;
					if(LOG.isDebugEnabled())
						LOG.debug("MetricAction-" + System.identityHashCode(this) + ": Queue put failure", e);
				}
				if(config.getRelatedSelectorEnabled()){
					try {
						long[] ret = relatedRecordSelector.selectRelatedRecords(r);
						inRecCntr += ret[0];
						outRecCntr += ret[1];
						putFailureCntr += ret[2];
						otherFailureCntr += ret[3];
					} catch (Exception e) {
						LOG.error("Failed to process related records for input record " + r.toString(), e);
					}
				}
			}
		}
	}
	
	public int getPeriodicity(){
		return config.getPeriodicity();
	}
	
	public void flushRelatedSelection(){
		if(relatedRecordSelector != null){
			relatedRecordSelector.selectRelatedRecords();
		}
	}

}
