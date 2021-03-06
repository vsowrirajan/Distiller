package com.mapr.distiller.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mapr.distiller.common.status.MetricActionStatus;
import com.mapr.distiller.common.status.RecordProducerStatus;
import com.mapr.distiller.common.status.RecordQueueStatus;

import com.mapr.distiller.server.metricactions.MetricAction;
import com.mapr.distiller.server.persistance.LocalFileSystemPersistanceManager;
import com.mapr.distiller.server.persistance.MapRDBSyncPersistanceManager;
import com.mapr.distiller.server.producers.raw.MfsGutsRecordProducer;
import com.mapr.distiller.server.producers.raw.ProcRecordProducer;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;
import com.mapr.distiller.server.recordtypes.ProcessResourceRecord;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.scheduler.MetricActionScheduler;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.utils.MetricConfig.MetricConfigBuilder;
import com.mapr.distiller.server.utils.MetricConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Coordinator implements Runnable, DistillerMonitor {

	private static final Logger LOG = LoggerFactory
			.getLogger(Coordinator.class);

	private static Object coordinatorLock;

	private SortedMap<String, MetricConfig> metricConfigMap;
	private SortedMap<String, MetricAction> metricActionsIdMap;
	private SortedMap<String, Boolean> metricActionsEnableMap;
	private ExecutorService executor = Executors.newFixedThreadPool(5);

	private SortedMap<String, Future<MetricAction>> metricActionsIdFuturesMap;

	private ProcRecordProducer procRecordProducer;
	private MfsGutsRecordProducer mfsGutsRecordProducer;
	private RecordQueueManager recordQueueManager;
	private MetricActionScheduler metricActionScheduler;
	private String configFileLocation;

	private static int myPid;
	private static long myStarttime;
	private static String maprdbHostname;
	//private static MapRDBPersistanceManager maprdbPersistanceManager;
	private static MapRDBSyncPersistanceManager maprdbSyncPersistanceManager;
	private static LocalFileSystemPersistanceManager localFileSystemPersistanceManager;
	
	private static boolean shouldExit;

	// Constructor
	public Coordinator() {
		// Nothing is done here because the work is really done in the main()
		// function as this is the primary class for distiller-server
		// That's not great but will do for now.
		LOG.info("Coordinator- {} : initialized", System.identityHashCode(this));
	}

	/**
	 * Methods related to creation of MetricConfig and MetricAction objects
	 */
	//This method is called once during Distiller startup as the configuration file is parsed.
	//Creation of metrics after this method is called must be done using the "createMetric" method
	//This method is called to create a MetricConfig for each sub-block of config in the "distiller" config block of the conf file 
	//The MetricConfig objects it generates are created one at a time using calls to createMetricConfig
	public void createMetricConfigs(Config baseConfig) throws Exception {
		ConfigObject distillerConfigObject = baseConfig.getObject("distiller");
		Set<String> metricConfigNames = distillerConfigObject.keySet();
		Config distillerConfig = distillerConfigObject.toConfig();
		// Create metricActions
		synchronized(coordinatorLock){
			for (String metric : metricConfigNames) {
				LOG.info("Coordinator- {} : Creating a MetricConfig for element {} ", System.identityHashCode(this), metric);
				Config configBlock = distillerConfig.getObject(metric).toConfig();
				MetricConfig metricConfig = null;
				try {
					metricConfig = createMetricConfig(configBlock);
					if(metricConfigMap.containsKey(metricConfig.getId()))
						throw new Exception("The value for " + Constants.METRIC_NAME + " is not unique in the configuration, " + 
											"duplicate value found in block " + metric + ", value: " + metricConfig.getId());
					metricConfigMap.put(metricConfig.getId(), metricConfig);
				} catch (Exception e) {
					throw new Exception("Failed to process configuration block \"" + metric + "\"", e);
				}
			}
			Iterator<Map.Entry<String, MetricConfig>> i = metricConfigMap.entrySet().iterator();
			while(i.hasNext()){
				Map.Entry<String, MetricConfig> e = i.next();
				MetricConfig metricConfig = e.getValue();
				if (metricConfig.getSelector()!=null && metricConfig.getSelector().equals(Constants.PERSISTING_SELECTOR) &&
					metricConfig.getPersistorName().equals(Constants.MAPRDB)) {
					//if(maprdbPersistanceManager == null){
					//	try {
							//maprdbPersistanceManager = new MapRDBPersistanceManager("maprfs:///",
							//														metricConfig.getMapRDBAsyncPutTimeout(),
							//														myPid,
							//														myStarttime,
							//														maprdbHostname,
							//														metricConfig.getMapRDBLocalWorkDirByteLimit(),
							//														metricConfig.getMapRDBLocalWorkDirPath());
							//maprdbPersistanceManager.start();
					if(maprdbSyncPersistanceManager == null){
						try {
							maprdbSyncPersistanceManager = new MapRDBSyncPersistanceManager(metricConfig.getMapRDBAsyncPutTimeout(),
																							myPid,
																							myStarttime,
																							maprdbHostname,
																							metricConfig.getMapRDBLocalWorkDirByteLimit(),
																							metricConfig.getMapRDBLocalWorkDirPath(),
																							metricConfig.getMapRDBAsyncPutTimeout());
							maprdbSyncPersistanceManager.start();
						} catch (Exception e2) {
							throw new Exception("Failed to initialize MapRDBSyncPersistanceManager for metric " + metricConfig.getId(), e2);
						}
					}
					metricConfig.setMapRDBSyncPersistanceManager(maprdbSyncPersistanceManager);
				}
				else if (metricConfig.getSelector()!=null && metricConfig.getSelector().equals(Constants.PERSISTING_SELECTOR) &&
						metricConfig.getPersistorName().equals(Constants.LOCAL_FILE_SYSTEM)) 
				{
					if(localFileSystemPersistanceManager == null){
						try {
							localFileSystemPersistanceManager = 
									new LocalFileSystemPersistanceManager(myPid,
																		  myStarttime,
																		  maprdbHostname,
																		  metricConfig.getLfspMaxOutputDirSize(),
																		  metricConfig.getLfspOutputDir());
							localFileSystemPersistanceManager.start();
						} catch (Exception e2) {
							throw new Exception("Failed to initialize LocalFileSystemPersistanceManager for metric " + metricConfig.getId(), e2);
						}
					}
					metricConfig.setLocalFileSystemPersistanceManager(localFileSystemPersistanceManager);
				}

			}
		}
	}

	//Creates a single MetricConfig object from the block of configuration passed as argument
	public static MetricConfig createMetricConfig(Config configBlock) throws Exception{
		//Generates a MetricConfig from a Config object while performing error checking.
		//If there is a problem with the content of the configuration then this will throw an exception.
		//If this returns succesfully, it doesn't imply the metric can be gathered, it just implies the values provided in the config are valid.
		//For instance, this function does not ensure that no other metric is already running with this name, nor does it try to resolve such a situation.

		String id = null;
		String inputQueue = null;
		String inputQueueType = null;
		String lfspOutputDir = null;
		String localFileInputMetricName = null;
		String localFileInputQueueScanner = null;
		String maprdbInputQueueScanEndTime = null;
		String maprdbInputQueueScanStartTime = null;
		String maprdbInputQueueScanner = null;
		String maprdbLocalWorkDirPath = null;
		String method = null;
		String metricDescription = null;
		String outputQueue = null;
		String outputQueueType = null;
		String persistorName = null;
		String procRecordProducerMetricName = null;
		String processor = null;
		String rawRecordProducerName = null;
		String recordType = null;
		String relatedInputQueueName = null;
		String relatedOutputQueueName = null;
		String relatedSelectorMethod = null;
		String relatedSelectorName = null;
		String selector = null;
		String selectorQualifierKey = null;
		String selectorQualifierValue = null;
		String thresholdKey = null;
		String thresholdValue = null;
		String updatingSubscriptionQueueKey = null;
		boolean generateJavaStackTraces = false;
		boolean maprdbCreateTables=false;
		boolean maprdbWorkDirEnabled=false;
		boolean metricActionStatusRecordsEnabled=false;
		boolean metricEnabled=false;
		boolean rawProducerMetricsEnabled = false;
		boolean relatedSelectorEnabled = false;
		int lfspFlushFrequency = -1;
		int lfspRecordsPerFile = 0;
		int lfspWriteBatchSize = -1;
		int maprdbAsyncPutTimeout = -1;
		int maprdbWorkDirBatchSize = 1000;
		int outputQueueMaxProducers = -1;
		int outputQueueRecordCapacity = -1;
		int outputQueueTimeCapacity = -1;
		int periodicity = -1;
		int relatedOutputQueueMaxProducers = -1;
		int relatedOutputQueueRecordCapacity = -1;
		int relatedOutputQueueTimeCapacity = -1;
		long cumulativeSelectorFlushTime = -1;
		long lfspMaxOutputDirSize = -1;
		long localFileInputQueueEndTimestamp = 0;
		long localFileInputQueueStartTimestamp = -1;
		long maprdbLocalWorkDirByteLimit;
		long metricActionStatusRecordFrequency=-1;
		long timeSelectorMaxDelta = -1;
		long timeSelectorMinDelta = -1;

		//Completely optional parameters
		try { 
			generateJavaStackTraces = configBlock.getBoolean(Constants.PRP_GENERATE_JAVA_STACK_TRACES);
		} catch (Exception e) {}
		try {
			localFileInputQueueScanner = configBlock.getString(Constants.LOCAL_FILE_INPUT_QUEUE_SCANNER);
		} catch (Exception e){
			localFileInputQueueScanner = Constants.TIMESTAMP_SCANNER;
		}
		try {
			localFileInputQueueStartTimestamp = configBlock.getLong(Constants.LOCAL_FILE_INPUT_QUEUE_START_TIMESTAMP);
		} catch (Exception e){
			localFileInputQueueStartTimestamp = 0;
		}
		try {
			localFileInputQueueEndTimestamp = configBlock.getLong(Constants.LOCAL_FILE_INPUT_QUEUE_END_TIMESTAMP);
		} catch (Exception e){
			localFileInputQueueEndTimestamp = -1;
		}
		try {
			localFileInputMetricName = configBlock.getString(Constants.LOCAL_FILE_INPUT_METRIC_NAME);
		} catch (Exception e){}
		try {
			lfspOutputDir = configBlock.getString(Constants.LFSP_OUTPUT_DIR);
		} catch (Exception e){
			lfspOutputDir = "/opt/mapr/distiller/output";
		}
		try {
			lfspMaxOutputDirSize = configBlock.getLong(Constants.LFSP_MAX_OUTPUT_DIR_SIZE);
		} catch (Exception e){
			lfspMaxOutputDirSize = -1;
		}
		try {
			lfspWriteBatchSize = configBlock.getInt(Constants.LFSP_WRITE_BATCH_SIZE);
		} catch (Exception e){
			lfspWriteBatchSize = 100;
		}
		try {
			lfspFlushFrequency = configBlock.getInt(Constants.LFSP_FLUSH_FREQUENCY);
		} catch (Exception e){
			lfspFlushFrequency = 60000;
		}
		try {
			lfspRecordsPerFile = configBlock.getInt(Constants.LFSP_RECORDS_PER_FILE);
		} catch (Exception e){
			lfspRecordsPerFile = 86400;
		}
		try {
			maprdbInputQueueScanner = configBlock.getString(Constants.MAPRDB_INPUT_QUEUE_SCANNER);
		} catch (Exception e) {}
		try {
			maprdbInputQueueScanStartTime = configBlock.getString(Constants.MAPRDB_INPUT_QUEUE_SCAN_START_TIME);
		} catch (Exception e) {}
		try {
			maprdbInputQueueScanEndTime = configBlock.getString(Constants.MAPRDB_INPUT_QUEUE_SCAN_END_TIME);
		} catch (Exception e) {}
		try {
			maprdbCreateTables = configBlock.getBoolean(Constants.MAPRDB_CREATE_TABLES);
		} catch (Exception e) {
			maprdbCreateTables=false;	//Default to false
		}
		try {
			maprdbAsyncPutTimeout = configBlock.getInt(Constants.MAPRDB_PUT_TIMEOUT);
		} catch (Exception e) {
			maprdbAsyncPutTimeout = 60;	//Default to 1 minute
		}
		if(maprdbAsyncPutTimeout < 30){
			throw new Exception("Value of " + Constants.MAPRDB_PUT_TIMEOUT + " must be >= 30, value: " + maprdbAsyncPutTimeout);
		}
		try {
			maprdbLocalWorkDirByteLimit = configBlock.getLong(Constants.MAPRDB_LOCAL_WORK_DIR_BYTE_LIMIT);
		} catch (Exception e) {
			maprdbLocalWorkDirByteLimit = 1073741824l;	//Default to 1GB
		}
		if(maprdbLocalWorkDirByteLimit < 1048576){
			throw new Exception("Value of " + Constants.MAPRDB_LOCAL_WORK_DIR_BYTE_LIMIT + " must be >= 1048576, value: " + maprdbLocalWorkDirByteLimit);
		}
		try {
			maprdbLocalWorkDirPath = configBlock.getString(Constants.MAPRDB_LOCAL_WORK_DIR_PATH);
		} catch (Exception e) {
			maprdbLocalWorkDirPath = "/opt/mapr/distiller/workdir";	//Default path for work dir
		}
		if(maprdbLocalWorkDirPath.equals("")){
			throw new Exception("Value of " + Constants.MAPRDB_LOCAL_WORK_DIR_PATH + " can not be an empty path");
		}
		try {
			maprdbWorkDirBatchSize = configBlock.getInt(Constants.MAPRDB_WORK_DIR_BATCH_SIZE);
		} catch (Exception e) {
			maprdbWorkDirBatchSize = 100;	//Default to 100 Record batch size
		}
		if(maprdbWorkDirBatchSize < 10) {
			throw new Exception("Value of " + Constants.MAPRDB_WORK_DIR_BATCH_SIZE + " must be >= 10, value: " + maprdbWorkDirBatchSize);
		}
		try {
			maprdbWorkDirEnabled = configBlock.getBoolean(Constants.MAPRDB_ENABLE_WORK_DIR);
		} catch (Exception e) {
			maprdbWorkDirEnabled = false;	//Default to false
		}
		try {
			rawProducerMetricsEnabled = configBlock.getBoolean(Constants.RAW_PRODUCER_METRICS_ENABLED);
		} catch (Exception e) {}
		try {
			metricDescription = configBlock.getString(Constants.METRIC_DESCRIPTION);
		} catch (Exception e) {}
		try {
			cumulativeSelectorFlushTime = configBlock.getLong(Constants.SELECTOR_CUMULATIVE_FLUSH_TIME);
		} catch (Exception e) {}
		try {
			metricActionStatusRecordsEnabled = configBlock.getBoolean(Constants.METRIC_ACTION_STATUS_RECORDS_ENABLED);
		} catch (Exception e) {}
		try {
			outputQueueType = configBlock.getString(Constants.OUTPUT_QUEUE_TYPE);
		} catch (Exception e) {}
		try {
			inputQueueType = configBlock.getString(Constants.INPUT_QUEUE_TYPE);
		} catch (Exception e) {}
		if (inputQueueType != null &&
			inputQueueType.equals(Constants.LOCAL_FILE_INPUT_RECORD_QUEUE) &&
			localFileInputMetricName == null){
			throw new Exception("Value must be specified for " + Constants.LOCAL_FILE_INPUT_METRIC_NAME + " when " + 
								Constants.INPUT_QUEUE_TYPE + "=" + Constants.LOCAL_FILE_INPUT_RECORD_QUEUE);
		}
		try {
			relatedSelectorEnabled = configBlock.getBoolean(Constants.RELATED_SELECTOR_ENABLED);
		} catch (Exception e) {}
		try {
			metricEnabled = configBlock.getBoolean(Constants.METRIC_ENABLED);
		} catch (Exception e) {}
		
		//Always required parameters:
		try {
			id = configBlock.getString(Constants.METRIC_NAME);
			if(id.equals(""))
				throw new Exception("Value for " + Constants.METRIC_NAME + " can not be an empty string");
		} catch (Exception e) {
			throw new Exception("Invalid value for parameter " + Constants.METRIC_NAME);
		}
		try {
			recordType = configBlock.getString(Constants.RECORD_TYPE);
			if(!isValidRecordType(recordType))
				throw new Exception("ERROR: Unknown value for " + Constants.RECORD_TYPE + ": " + recordType);
		} catch (Exception e) {
			throw new Exception("ERROR: All metric definitions must specify a valid name for parameter " + Constants.RECORD_TYPE, e);
		}
		try {
			outputQueue = configBlock.getString(Constants.OUTPUT_QUEUE_NAME);
			if(outputQueue.equals("")) throw new Exception();
			outputQueue = outputQueue.replace("${MAPRDB_HOSTNAME}", maprdbHostname);
		} catch (Exception e) {
			if(!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
				boolean gotSelector = false;
				try {
					selector = configBlock.getString(Constants.INPUT_RECORD_SELECTOR);
					gotSelector=true;
				} catch (Exception e2) {}
				if(!gotSelector || !selector.equals(Constants.PERSISTING_SELECTOR)) {
					boolean gotPersistorName = false;
					try {
						persistorName = configBlock.getString(Constants.PERSISTOR_NAME);
						gotPersistorName = true;
					} catch (Exception e3) {}
					if(!gotPersistorName || !persistorName.equals(Constants.LOCAL_FILE_SYSTEM))
						throw new Exception("A value is required for " + Constants.OUTPUT_QUEUE_NAME +
										" when " + Constants.RECORD_TYPE + "=" + recordType);
				}
			}
		}
		try {
			outputQueueRecordCapacity = configBlock.getInt(Constants.OUTPUT_QUEUE_CAPACITY_RECORDS);
		} catch (Exception e) {
			if(!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
				boolean gotSelector = false;
				try {
					selector = configBlock.getString(Constants.INPUT_RECORD_SELECTOR);
					gotSelector=true;
				} catch (Exception e2) {}
				if(!gotSelector || !selector.equals(Constants.PERSISTING_SELECTOR)) {
					throw new Exception("A value is required for " + Constants.OUTPUT_QUEUE_CAPACITY_RECORDS +
										" when " + Constants.RECORD_TYPE + "=" + recordType);
				}
			}
		}
		try {
			outputQueueTimeCapacity = configBlock.getInt(Constants.OUTPUT_QUEUE_CAPACITY_SECONDS);
			if(outputQueueTimeCapacity < 0) throw new Exception();
		} catch (Exception e) {
			if(!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
				boolean gotSelector = false;
				try {
					selector = configBlock.getString(Constants.INPUT_RECORD_SELECTOR);
					gotSelector=true;
				} catch (Exception e2) {}
				if(!gotSelector || !selector.equals(Constants.PERSISTING_SELECTOR)) {
					throw new Exception("A value is required for " + Constants.OUTPUT_QUEUE_CAPACITY_SECONDS +
										" when " + Constants.RECORD_TYPE + "=" + recordType);
				}
			}
		}
		try {
			outputQueueMaxProducers = configBlock.getInt(Constants.OUTPUT_QUEUE_MAX_PRODUCERS);
			if(outputQueueMaxProducers < 0) throw new Exception();
		} catch (Exception e) {
			if (!isRawRecordType(recordType) &&
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)) {
				boolean gotSelector = false;
				try {
					selector = configBlock.getString(Constants.INPUT_RECORD_SELECTOR);
					gotSelector=true;
				} catch (Exception e2) {}
				if(!gotSelector || !selector.equals(Constants.PERSISTING_SELECTOR)) {
					throw new Exception("All metrics require a value >= 0 for " + Constants.OUTPUT_QUEUE_MAX_PRODUCERS);
				}
			}
		}
		
		//Conditionally required parameters:
		try {
			inputQueue = configBlock.getString(Constants.INPUT_QUEUE_NAME);
			if(inputQueue.equals(""))
				throw new Exception("An empty string was provided for " + Constants.INPUT_QUEUE_NAME);
			inputQueue = inputQueue.replace("${MAPRDB_HOSTNAME}", maprdbHostname);
		} catch (Exception e) {
			if (!isRawRecordType(recordType) &&
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("Valid value must be specified for " + Constants.INPUT_QUEUE_NAME + 
									" when a non-raw tpye is specified for " + Constants.RECORD_TYPE, e);
		}
		try {
			periodicity = configBlock.getInt(Constants.PERIODICITY_MS);
			if(periodicity < 1000) throw new Exception();
		} catch (Exception e) {
			if (!isRawRecordType(recordType) || 
				recordType.equals(Constants.PROC_RECORD_PRODUCER_RECORD) ){
				throw new Exception("A value >= 1000 must be specified for " + Constants.PERIODICITY_MS + 
									" when " + Constants.RECORD_TYPE + "=" + recordType);
			}
		}
		try {
			procRecordProducerMetricName = configBlock.getString(Constants.PROC_RECORD_PRODUCER_METRIC_NAME);
			if(!ProcRecordProducer.isValidMetricName(procRecordProducerMetricName)) 
				throw new Exception("Invalid value for " + Constants.PROC_RECORD_PRODUCER_METRIC_NAME + ": " + 
									procRecordProducerMetricName);
		} catch (Exception e) {
			if(recordType.equals(Constants.PROC_RECORD_PRODUCER_RECORD))
				throw new Exception("A valid name must be specified for " + Constants.PROC_RECORD_PRODUCER_METRIC_NAME +
									" when " + Constants.RECORD_TYPE + "=" + Constants.PROC_RECORD_PRODUCER_RECORD, e);
		}
		try {
			selector = configBlock.getString(Constants.INPUT_RECORD_SELECTOR);
			if(!isValidRecordSelector(selector)) 
				throw new Exception("Invalid value for " + Constants.INPUT_RECORD_SELECTOR + 
									": " + selector);
		} catch (Exception e) {
			if (!isRawRecordType(recordType) &&
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("Valid value must be specified for " + Constants.INPUT_RECORD_SELECTOR + 
						" when a non-raw tpye is specified for " + Constants.RECORD_TYPE, e);
		}
		try {
			persistorName = configBlock.getString(Constants.PERSISTOR_NAME);
			if(!isValidPersistor(persistorName))
				throw new Exception("Invalid value for " + Constants.PERSISTOR_NAME + 
									": " + persistorName);
		} catch (Exception e) {
			if(selector!=null && selector.equals(Constants.PERSISTING_SELECTOR))
				throw new Exception("Valid value must be specified for " + Constants.PERSISTOR_NAME + 
						" when " + Constants.INPUT_RECORD_SELECTOR + "=" + Constants.PERSISTING_SELECTOR, e);
		}
		try {
			processor = configBlock.getString(Constants.INPUT_RECORD_PROCESSOR_NAME);
			if(!isValidProcessorName(processor)) 
				throw new Exception("Invalid value for " + Constants.INPUT_RECORD_PROCESSOR_NAME +
									": " + processor);
		} catch (Exception e) {
			if (!isRawRecordType(recordType) &&
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD) && 
				!selector.equals(Constants.PERSISTING_SELECTOR) ) 
				throw new Exception("Valid value must be specified for " + Constants.INPUT_RECORD_PROCESSOR_NAME + 
						" when a non-raw tpye is specified for " + Constants.RECORD_TYPE, e);
		}
		try {
			method = configBlock.getString(Constants.INPUT_RECORD_PROCESSOR_METHOD);
			if(!isValidProcessorMethod(method)) 
				throw new Exception("Unknown value for " + Constants.INPUT_RECORD_PROCESSOR_METHOD + 
									": " + method);
		} catch (Exception e) {
			if (!isRawRecordType(recordType) &&
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD) && 
				!selector.equals(Constants.PERSISTING_SELECTOR) &&
				!processor.equals(Constants.PASSTHROUGH_RECORD_PROCESSOR))
				throw new Exception("Use of non-raw " + Constants.RECORD_TYPE + "=" + recordType + 
									" requires a valid value for " + Constants.INPUT_RECORD_PROCESSOR_METHOD, e);
		}
		if (!isRawRecordType(recordType) &&
			!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD) &&
			!selectorSupportsMethod(selector, method) && 
			!processor.equals(Constants.PASSTHROUGH_RECORD_PROCESSOR))
			throw new Exception(Constants.INPUT_RECORD_PROCESSOR_METHOD + "=" + method + 
								" is not supported by " + Constants.INPUT_RECORD_SELECTOR + 
								"=" + selector);
		try {
			thresholdKey = configBlock.getString(Constants.THRESHOLD_KEY);
			if(thresholdKey.equals("")) throw new Exception();
		} catch (Exception e) {
			if(processorMethodRequiresThresholdKey(method))
				throw new Exception("A value must be provided for " + Constants.THRESHOLD_KEY + 
									" when " + Constants.INPUT_RECORD_PROCESSOR_METHOD +
									"=" + method, e);
		}
		try {
			thresholdValue = configBlock.getString(Constants.THRESHOLD_VALUE);
			if(thresholdKey.equals("")) throw new Exception();
		} catch (Exception e) {
			if(processorMethodRequiresThresholdValue(method))
				throw new Exception("A value must be provided for " + Constants.THRESHOLD_VALUE + 
									" when " + Constants.INPUT_RECORD_PROCESSOR_METHOD +
									"=" + method, e);
		}
		try {
			timeSelectorMinDelta = configBlock.getLong(Constants.TIME_SELECTOR_MIN_DELTA);
			if(timeSelectorMinDelta<1000)
				throw new Exception("A value of >=1000 must be specified for " + 
									Constants.TIME_SELECTOR_MIN_DELTA);
		} catch (Exception e) {
			if(selector!=null && selector.equals(Constants.TIME_SELECTOR))
				throw new Exception("A valid value must be specified for " + Constants.TIME_SELECTOR_MIN_DELTA + 
									" when " + Constants.INPUT_RECORD_SELECTOR + 
									"=" + Constants.TIME_SELECTOR, e);
		}
		try {
			timeSelectorMaxDelta = configBlock.getLong(Constants.TIME_SELECTOR_MAX_DELTA);
			if(timeSelectorMaxDelta != -1 && timeSelectorMaxDelta<1000)
				throw new Exception("A value of -1 or >=1000 must be specified for " + 
									Constants.TIME_SELECTOR_MAX_DELTA);
			if(timeSelectorMaxDelta != -1 && timeSelectorMaxDelta < timeSelectorMinDelta)
				throw new Exception("The value for " + Constants.TIME_SELECTOR_MAX_DELTA + "(" + 
									timeSelectorMaxDelta + ") is not greater than the value for " + 
									Constants.TIME_SELECTOR_MIN_DELTA + "(" + timeSelectorMinDelta + ")" );
		} catch (Exception e) {
			if(selector!=null && selector.equals(Constants.TIME_SELECTOR))
				throw new Exception("A valid value must be specified for " + Constants.TIME_SELECTOR_MAX_DELTA + 
									" when " + Constants.INPUT_RECORD_SELECTOR + 
									"=" + Constants.TIME_SELECTOR, e);
		}
		try {
			selectorQualifierKey = configBlock.getString(Constants.SELECTOR_QUALIFIER_KEY);
			if(selectorQualifierKey.equals(""))
				throw new Exception("An empty string was provided for " + Constants.SELECTOR_QUALIFIER_KEY);
		} catch (Exception e) {
			if(selectorRequiresQualifierKey(selector))
				throw new Exception("A valid value must be specified for " + Constants.SELECTOR_QUALIFIER_KEY + 
									" when " + Constants.INPUT_RECORD_SELECTOR + "=" + selector, e);
		}
		try {
			selectorQualifierValue = configBlock.getString(Constants.SELECTOR_QUALIFIER_VALUE);
			if(selectorQualifierValue.equals(""))
				throw new Exception("An empty string was provided for " + Constants.SELECTOR_QUALIFIER_VALUE);
		} catch (Exception e) {
			if(selectorRequiresQualifierValue(selector))
				throw new Exception("A valid value must be specified for " + Constants.SELECTOR_QUALIFIER_VALUE + 
									" when " + Constants.INPUT_RECORD_SELECTOR + "=" + selector, e);
		}
		try {
			relatedInputQueueName = configBlock.getString(Constants.SELECTOR_RELATED_INPUT_QUEUE_NAME);
			if(relatedInputQueueName.equals(""))
				throw new Exception("An empty string was provided for " + Constants.SELECTOR_RELATED_INPUT_QUEUE_NAME);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A value must be provided for " + Constants.SELECTOR_RELATED_INPUT_QUEUE_NAME + 
						" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedOutputQueueName = configBlock.getString(Constants.SELECTOR_RELATED_OUTPUT_QUEUE_NAME);
			if(relatedInputQueueName.equals(""))
				throw new Exception("An empty string was provided for " + Constants.SELECTOR_RELATED_OUTPUT_QUEUE_NAME);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A value must be provided for " + Constants.SELECTOR_RELATED_OUTPUT_QUEUE_NAME + 
						" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedOutputQueueRecordCapacity = configBlock.getInt(Constants.RELATED_OUTPUT_QUEUE_CAPACITY_RECORDS);
			if(relatedOutputQueueRecordCapacity < 0) 
				throw new Exception("A value >= 0 must be specified for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_RECORDS + 
				" - value: " + relatedOutputQueueRecordCapacity);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A value >= 0 is required for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_RECORDS +
									" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedOutputQueueTimeCapacity = configBlock.getInt(Constants.RELATED_OUTPUT_QUEUE_CAPACITY_SECONDS);
			if(relatedOutputQueueTimeCapacity < 0)
				throw new Exception("A value >= 0 must be specified for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_SECONDS + 
									" - value: " + relatedOutputQueueTimeCapacity);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A value >= 0 is required for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_SECONDS +
									" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedOutputQueueMaxProducers = configBlock.getInt(Constants.RELATED_OUTPUT_QUEUE_MAX_PRODUCERS);
			if(relatedOutputQueueMaxProducers < 0)
				throw new Exception("A value >= 0 must be specified for " + Constants.RELATED_OUTPUT_QUEUE_MAX_PRODUCERS + 
									" - value: " + relatedOutputQueueMaxProducers);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A value >= 0 is required for " + Constants.RELATED_OUTPUT_QUEUE_MAX_PRODUCERS +
									" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedSelectorName = configBlock.getString(Constants.SELECTOR_RELATED_NAME);
			if(!isValidRelatedSelectorName(relatedSelectorName))
				throw new Exception("Invalid value for " + Constants.SELECTOR_RELATED_NAME +
									" - " + relatedSelectorName);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A valid value must be provided for " + Constants.SELECTOR_RELATED_NAME + 
						" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedSelectorMethod = configBlock.getString(Constants.SELECTOR_RELATED_METHOD);
			if(!isValidRelatedSelectorMethod(relatedSelectorMethod))
				throw new Exception("Invalid value for " + Constants.SELECTOR_RELATED_METHOD + 
									" - " + relatedSelectorMethod);
		} catch (Exception e) {	
			if(relatedSelectorEnabled)
				throw new Exception("A valid value must be provided for " + Constants.SELECTOR_RELATED_METHOD + 
						" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			updatingSubscriptionQueueKey = configBlock.getString(Constants.UPDATING_SUBSCRIPTION_QUEUE_KEY);
			if(updatingSubscriptionQueueKey.equals(""))
				throw new Exception("An empty string was provided for " + Constants.UPDATING_SUBSCRIPTION_QUEUE_KEY);
		} catch (Exception e) {
			if(outputQueueType!=null && outputQueueType.equals(Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE))
				throw new Exception("Use of " + Constants.OUTPUT_QUEUE_TYPE + "=" + Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE + 
									" requires a value for " + Constants.UPDATING_SUBSCRIPTION_QUEUE_KEY, e);
		}
		try {
			metricActionStatusRecordFrequency = configBlock.getLong(Constants.METRIC_ACTION_STATUS_RECORD_FREQUENCY);
			if(metricActionStatusRecordFrequency < 1000)
				throw new Exception("A value >= 1000 must be specified for " + Constants.METRIC_ACTION_STATUS_RECORD_FREQUENCY);
		} catch (Exception e) {
			if(metricActionStatusRecordsEnabled)
				throw new Exception("A valid value must be specified for " + Constants.METRIC_ACTION_STATUS_RECORD_FREQUENCY + 
									" when " + Constants.METRIC_ACTION_STATUS_RECORDS_ENABLED + "=true", e);
		}
		try {
			rawRecordProducerName = configBlock.getString(Constants.RAW_RECORD_PRODUCER_NAME);
			if(!isRawRecordProducerName(rawRecordProducerName))
				throw new Exception("Invalid raw record producer name:" + rawRecordProducerName);
		} catch (Exception e) {
			if (recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("A valid name must be specified for " + Constants.RAW_RECORD_PRODUCER_NAME + 
									" when " + Constants.RECORD_TYPE + "=" + Constants.RAW_RECORD_PRODUCER_STAT_RECORD, e);					
			if (rawProducerMetricsEnabled)
				throw new Exception("A valid name must be specified for " + Constants.RAW_RECORD_PRODUCER_NAME + 
									" when " + Constants.RAW_PRODUCER_METRICS_ENABLED + "=true", e);
		}

		MetricConfigBuilder metricConfigBuilder = null;
		try {
			metricConfigBuilder = new MetricConfigBuilder(id, inputQueue, outputQueue, outputQueueRecordCapacity, 
					outputQueueTimeCapacity, outputQueueMaxProducers, periodicity, recordType, procRecordProducerMetricName, 
					rawProducerMetricsEnabled, metricDescription, rawRecordProducerName, selector, processor, method,
					metricActionStatusRecordsEnabled, metricActionStatusRecordFrequency, thresholdKey, thresholdValue,
					timeSelectorMaxDelta, timeSelectorMinDelta, selectorQualifierKey, cumulativeSelectorFlushTime,
					outputQueueType, selectorQualifierValue, relatedInputQueueName, relatedSelectorName, relatedSelectorMethod, updatingSubscriptionQueueKey, 
					relatedOutputQueueName, relatedOutputQueueRecordCapacity, relatedOutputQueueTimeCapacity, 
					relatedOutputQueueMaxProducers, relatedSelectorEnabled, metricEnabled, persistorName,
					maprdbCreateTables, myPid, myStarttime, inputQueueType, maprdbInputQueueScanner, 
					maprdbInputQueueScanStartTime, maprdbInputQueueScanEndTime, maprdbAsyncPutTimeout,
					maprdbLocalWorkDirByteLimit, maprdbLocalWorkDirPath, maprdbWorkDirBatchSize, maprdbWorkDirEnabled, 
					lfspOutputDir, lfspMaxOutputDirSize, lfspWriteBatchSize, lfspFlushFrequency, lfspRecordsPerFile,
					localFileInputQueueScanner, localFileInputQueueStartTimestamp, localFileInputQueueEndTimestamp, 
					localFileInputMetricName, generateJavaStackTraces);
		} catch (Exception e) {
			throw new Exception("Failed to construct MetricConfigBuilder", e);
		}
		return metricConfigBuilder.build();
	}

	// This method is called once during Distiller startup as the MetricConfig
	// objects created from parsing the config file are built into MetricAction
	// objects and scheduled for execution.
	// Creation of metrics after this method is called must be done using the
	// "createMetric" method
	public void createMetricActions() throws Exception{
		LOG.info("Coordinator-{} : request to create metric actions" , System.identityHashCode(this));
		
		int initializationSuccesses=0;
		int consecutiveIterationsWithNoSuccesses=0;
		int maxIterationsWithNoSuccesses=2;
		boolean initializationComplete=false;
		synchronized(coordinatorLock){
			while(	!initializationComplete && 
					consecutiveIterationsWithNoSuccesses<maxIterationsWithNoSuccesses )
			{
				int ss = initializationSuccesses;
				for (MetricConfig config : metricConfigMap.values()) {
					MetricAction newAction = null;
					if(!config.getMetricActionCreated()){
						try {
							newAction = createMetricAction(config);
						} catch (Exception e) {
							throw new Exception("Exception during createMetricAction for " + config.getId(), e);
						}
						if(newAction!=null ){
							metricActionsIdMap.put(newAction.getId(), newAction);
							config.setMetricActionCreated(true);
							LOG.debug("Coordinator- {} : MetricAction created for {}", System.identityHashCode(this), newAction.getId());
						} else if(isRawRecordType(config.getRecordType()) || (config.isInitialized() && config.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))){
							config.setMetricActionCreated(true);
							initializationSuccesses++;
							LOG.debug("Coordinator- {} : Raw metric enabled for {} {} ", System.identityHashCode(this), config.getRecordType(), config.getProcRecordProducerMetricName());
						}
					}
					if(newAction!=null){
						if(config.getMetricEnabled()){
							try {
								enableMetricAction(newAction);
								LOG.info("Coordinator- {} : Enabled MetricAction {}", System.identityHashCode(this), newAction.getId());
							} catch (Exception e) {
								throw new Exception("Failed to call enableMetricAction for " + newAction.getId(), e);
							}
						} else {
							metricActionsEnableMap.put(newAction.getId(), new Boolean(false));
						}
						initializationSuccesses++;
					}
				}
				if(ss == initializationSuccesses)
					consecutiveIterationsWithNoSuccesses++;
				else
					consecutiveIterationsWithNoSuccesses=0;
				
				if(initializationSuccesses==metricConfigMap.size())
					initializationComplete=true;
			} 
			if(!initializationComplete){
				for (MetricConfig config : metricConfigMap.values()) {
					if(!config.getMetricActionCreated())
						LOG.error("Coordinator-"
								+ System.identityHashCode(this)
								+ ": Failed to create MetricAction for MetricConfig "
								+ config.getId()
								+ " inputQueue:"
								+ config.getInputQueue()
								+ " inputQExists:"
								+ ((config.getInputQueue() == null) ? "null"
										: recordQueueManager.queueExists(config
												.getInputQueue()))
								+ ((config.getRelatedSelectorEnabled()) ? (" relatedQueue:"
										+ config.getRelatedInputQueueName()
										+ " relatedQExists:" + recordQueueManager
										.queueExists(config
												.getRelatedInputQueueName()))
										: ""));
				}
				throw new Exception("Failed to initialize " + (metricConfigMap.size() - initializationSuccesses) + " metric(s), initialized:" + initializationSuccesses + " size:" + metricConfigMap.size());
			}
		}
	}

	// This method builds a MetricAction from a MetricConfig, e.g. a schedulable
	// object (MetricAction) from a description of what should be done
	// (MetricConfig)
	private MetricAction createMetricAction(MetricConfig config) throws Exception {
		MetricAction metricAction = null;
		boolean initialized=false;
		
		if(config.isInitialized() && !config.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
			throw new Exception("MetricConfig " + config.getId() + " is already initialized.");
		if(config.getMetricEnabled()){
			if(inputQueueRequiredForType(config.getInputQueueType())){
				if( config.getInputQueue()!=null &&
					!recordQueueManager.queueExists(config.getInputQueue()) ) 
				{
					return null;
				}
			}
			if( config.getRelatedSelectorEnabled() &&
				!recordQueueManager.queueExists(config.getRelatedInputQueueName()) )
			{
				return null;
			}
		}
		
		switch (config.getRecordType()) {

		case Constants.MFS_GUTS_RECORD_PRODUCER_RECORD:
			if(!enableMfsGutsRecordProducer(config)){
				throw new Exception("Coordinato: Failed to enable MfsGutsRecordProducer");
			} else {
				LOG.info("Coordinator: Enabled MfsGutsRecordProducer");
				initialized=true;
			}
			break;

		case Constants.PROC_RECORD_PRODUCER_RECORD:
			if(!enableProcRecordProducerMetric(config)){
				throw new Exception("Coordinator: Failed to enable ProcRecordProducer metric " + config.getProcRecordProducerMetricName());
			} else {
				LOG.info("Coordinator: Enabled ProcRecordProducer metric " + config.getProcRecordProducerMetricName());
				initialized=true;
			}
			break;

		case Constants.SYSTEM_MEMORY_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.SYSTEM_CPU_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.DISK_STAT_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.NETWORK_INTERFACE_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.THREAD_RESOURCE_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.PROCESS_RESOURCE_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.SLIM_THREAD_RESOURCE_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.SLIM_PROCESS_RESOURCE_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.TCP_CONNECTION_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.LOAD_AVERAGE_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.DIFFERENTIAL_VALUE_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.PROCESS_EFFICIENCY_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;

		case Constants.MFS_GUTS_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				LOG.error("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;
			
		case Constants.RAW_RECORD_PRODUCER_STAT_RECORD:
			if(!enableRawRecordProducerStats(config)){
				if
				( ( config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME) 
					&& 
					procRecordProducer!=null
				  )
				  ||
				  ( config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME) 
					&&
					mfsGutsRecordProducer!=null
				  )
				)	
				{
					throw new Exception("Coordinator: Failed to enable stats for raw record producer " + config.getRawRecordProducerName());
				}
			} else {
				LOG.info("Coordinator: Enabled stats for raw record producer "
						+ config.getRawRecordProducerName());
				initialized=true;
			}
			break;

		default:
			throw new IllegalArgumentException("Unknown record type \"" + config.getRecordType() + "\" specified for metric \"" + config.getId() + "\"");
		}
		
		if(initialized){
			config.setInitialized(true);
			return metricAction;
		}
		else 
			return null;
	}

	// This method constructs, registers and schedules (if enabled) MetricConfig
	// and MetricAction objects based on a provided block of config
	public void createMetric(Config configBlock) throws Exception{
		synchronized(coordinatorLock){
			MetricConfig metricConfig = null;
			MetricAction metricAction = null;
			try {
				metricConfig = createMetricConfig(configBlock);
				if(metricConfigMap.containsKey(metricConfig.getId()))
					throw new Exception("The value for " + Constants.METRIC_NAME + " is not unique in the configuration, " + 
										"duplicate value found: " + metricConfig.getId());
				if (metricConfig.getSelector().equals(Constants.PERSISTING_SELECTOR) &&
					metricConfig.getPersistorName().equals(Constants.MAPRDB)) {
					//if(maprdbPersistanceManager == null){
					//	try {
					//		maprdbPersistanceManager = new MapRDBPersistanceManager("maprfs:///",
					//				metricConfig.getMapRDBAsyncPutTimeout(),
					//				myPid,
					//				myStarttime,
					//				maprdbHostname,
					//				metricConfig.getMapRDBLocalWorkDirByteLimit(),
					//				metricConfig.getMapRDBLocalWorkDirPath());
					//		maprdbPersistanceManager.start();
					//	} catch (Exception e2) {
					//		throw new Exception("Failed to initialize MapRDBMonitor for metric " + metricConfig.getId(), e2);
					//	}
					//}
					//metricConfig.setMapRDBPersistanceManager(maprdbPersistanceManager);
					if(maprdbSyncPersistanceManager == null){
						try {
							maprdbSyncPersistanceManager = new MapRDBSyncPersistanceManager(metricConfig.getMapRDBAsyncPutTimeout(),
																							myPid,
																							myStarttime,
																							maprdbHostname,
																							metricConfig.getMapRDBLocalWorkDirByteLimit(),
																							metricConfig.getMapRDBLocalWorkDirPath(),
																							metricConfig.getMapRDBAsyncPutTimeout());
							maprdbSyncPersistanceManager.start();
						} catch (Exception e2) {
							throw new Exception("Failed to initialize MapRDBSyncPersistanceManager for metric " + metricConfig.getId(), e2);
						}
					}
					metricConfig.setMapRDBSyncPersistanceManager(maprdbSyncPersistanceManager);
				}
				else if (metricConfig.getSelector().equals(Constants.PERSISTING_SELECTOR) &&
						metricConfig.getPersistorName().equals(Constants.LOCAL_FILE_SYSTEM)) 
				{
					if(localFileSystemPersistanceManager == null){
						try {
							localFileSystemPersistanceManager = 
									new LocalFileSystemPersistanceManager(myPid,
																		  myStarttime,
																		  maprdbHostname,
																		  metricConfig.getLfspMaxOutputDirSize(),
																		  metricConfig.getLfspOutputDir());

							localFileSystemPersistanceManager.start();
						} catch (Exception e2) {
							throw new Exception("Failed to initialize LocalFileSystemPersistanceManager for metric " + metricConfig.getId(), e2);
						}
					}
					metricConfig.setLocalFileSystemPersistanceManager(localFileSystemPersistanceManager);
				}
				metricAction = createMetricAction(metricConfig);
				if(metricAction!=null){
					metricConfig.setMetricActionCreated(true);
					metricConfigMap.put(metricConfig.getId(), metricConfig);
					metricActionsIdMap.put(metricAction.getId(), metricAction);
					if(metricConfig.getMetricEnabled()){
						try {
							enableMetricAction(metricAction);
						} catch (Exception e) {
							throw new Exception("Failed to enable MetricAction", e);
						}
					} else {
						metricActionsEnableMap.put(metricAction.getId(), new Boolean(false));
					}
				} else if(isRawRecordType(metricConfig.getRecordType()) || metricConfig.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
					metricConfig.setMetricActionCreated(true);
				} else
					throw new Exception("Unexpected condition.");
			} catch (Exception e) {
				if(metricConfig!=null){
					try {
						metricActionsIdFuturesMap.remove(metricConfig.getId());
					} catch (Exception e2){}
					try {
metricActionsEnableMap.remove(metricConfig.getId());
					} catch (Exception e2){}
					try {
						metricActionsIdMap.remove(metricConfig.getId());
					} catch (Exception e2){}
					try {
						metricConfigMap.remove(metricConfig.getId());
					} catch (Exception e2){}
				}
				throw new Exception("Failed to create metric.", e);
			}
		}
	}

	/**
	 * Methods related to enabling metrics
	 */
	// Call this method to enable a metric based on the id parameter of a
	// MetricConfig
	private void enableMetric(String metricName) throws Exception{
		synchronized(coordinatorLock){
			MetricAction metricToEnable = metricActionsIdMap.get(metricName);
			if(metricToEnable!=null){
				try {
					enableMetricAction(metricToEnable);
				} catch (Exception e) {
					throw new Exception("Failed to enable metric " + metricToEnable.getId(), e);
				}
			} else {
				if(metricConfigMap.containsKey(metricName)){
					MetricConfig configForMetric = metricConfigMap.get(metricName);
					if (!isRawRecordType(configForMetric.getRecordType()) &&
						!configForMetric.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
						throw new Exception("No such metric:" + metricName);
					} else if(configForMetric.getRecordType().equals(Constants.PROC_RECORD_PRODUCER_RECORD)){
						try {
							if(!enableProcRecordProducerMetric(configForMetric))
								throw new Exception("Failed to enable ProcRecordProducer metric ");
						} catch (Exception e) {
							throw new Exception("Failed to enable ProcRecordProducer metric " + configForMetric.getProcRecordProducerMetricName(), e);
						}
					} else if (configForMetric.getRecordType().equals(Constants.MFS_GUTS_RECORD_PRODUCER_RECORD)) {
						try {
							if(!enableMfsGutsRecordProducer(configForMetric))
								throw new Exception("Failed to enable MfsGutsRecordProducer");
						} catch (Exception e) {
							throw new Exception("Failed to enable MfsGutsRecordProducer", e);
						}
					} else if (configForMetric.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)) {
						try {
							if(!enableRawRecordProducerStats(configForMetric))
								throw new Exception("Failed to enable raw record producer stats");
						} catch (Exception e) {
							throw new Exception("Failed to enable raw record producer stats for " + configForMetric.getRawRecordProducerName(), e);
						}
					} else {
						throw new Exception("Failed to enable metric " + metricName + " due to unknown record type " + configForMetric.getRecordType());
					}
				} else {
					throw new Exception("No such metric: " + metricName);
				}
			}
		}
	}

	// Call this method to enable a single MetricAction (e.g. schedule it for
	// execution)
	private void enableMetricAction(MetricAction metricAction) throws Exception {
		synchronized(coordinatorLock){
			metricAction.enableMetric();
			metricActionsEnableMap.put(metricAction.getId(), new Boolean(true));
			metricActionScheduler.schedule(metricAction);
		}
	}
	//Call this method to enable MfsGutsRecordProducer
	private boolean enableMfsGutsRecordProducer(MetricConfig config){
		boolean createdQueue=false;
		if(mfsGutsRecordProducer != null) {
			if(!mfsGutsRecordProducer.isAlive())
				LOG.warn("Coordinator: MfsGutsRecordProducer was initialized but is no longer running.  Will attempt to recreate it.");
			else {
				LOG.info("Coordinator: MfsGutsRecordProducer is already running.");
				return false;
			}
		}
		synchronized(coordinatorLock){
			if(	
					!(
						(	
							recordQueueManager.queueExists(config.getOutputQueue()) &&
							recordQueueManager.getQueueRecordCapacity(config.getOutputQueue()) == config.getOutputQueueRecordCapacity() &&
							recordQueueManager.getQueueTimeCapacity(config.getOutputQueue()) == config.getOutputQueueTimeCapacity() &&
							recordQueueManager.getMaxQueueProducers(config.getOutputQueue()) == 1 &&
							(	recordQueueManager.getQueueProducers(config.getOutputQueue()).length == 0 ||
								recordQueueManager.checkForQueueProducer(config.getOutputQueue(), Constants.MFS_GUTS_RECORD_PRODUCER_NAME)
							)
						)
						||
						( 
							recordQueueManager.createQueue(config.getOutputQueue(), config.getOutputQueueRecordCapacity(), config.getOutputQueueTimeCapacity(), 1, null, null) &&
							(createdQueue=true)
						)
					 )
				  )	
			{
				LOG.error("Coordinator: Failed to enable metric because output RecordQueue \"" + 
						config.getOutputQueue() + "\" could not be created");
				return false;
			}
			if(
				!(
					recordQueueManager.checkForQueueProducer(config.getOutputQueue(), Constants.MFS_GUTS_RECORD_PRODUCER_NAME) ||
					recordQueueManager.registerProducer(config.getOutputQueue(), Constants.MFS_GUTS_RECORD_PRODUCER_NAME)
				 )
			  )
			{
				LOG.error("Coordinator: Failed to enable metric because producer \"" + 
						Constants.MFS_GUTS_RECORD_PRODUCER_NAME + "\" could not be registered with queue \"" + config.getOutputQueue() + "\"");
				if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
					LOG.error("Coordinator: Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
				return false;
			}
			mfsGutsRecordProducer = new MfsGutsRecordProducer(recordQueueManager.getQueue(config.getOutputQueue()), Constants.MFS_GUTS_RECORD_PRODUCER_NAME);
			mfsGutsRecordProducer.start();
			
			if(config.getRawProducerMetricsEnabled()){
				config.setRawRecordProducerName(Constants.MFS_GUTS_RECORD_PRODUCER_NAME);
				enableRawRecordProducerStats(config);
			}
			
			return true;	
		}
	}
	//Call this method to enable a ProcRecordProducer metric
	private boolean enableProcRecordProducerMetric(MetricConfig config){
		boolean createdQueue=false, registeredProducer=false;
		synchronized(coordinatorLock){
			if(!ProcRecordProducer.isValidMetricName(config.getProcRecordProducerMetricName())){
				LOG.error("Coordinator: Can not enable ProcRecordProducer metric due to invalid metric name: \"" + config.getProcRecordProducerMetricName() + "\"");
				return false;
			}
			if(procRecordProducer == null || !procRecordProducer.isAlive()){
				LOG.error("Coordinator: Failed to enable metric because ProdRecordProducer is not alive");
				return false;
			}
			if(	
				!(
					(	
						recordQueueManager.queueExists(config.getOutputQueue()) &&
						recordQueueManager.getQueueRecordCapacity(config.getOutputQueue()) == config.getOutputQueueRecordCapacity() &&
						recordQueueManager.getQueueTimeCapacity(config.getOutputQueue()) == config.getOutputQueueTimeCapacity() &&
						recordQueueManager.getMaxQueueProducers(config.getOutputQueue()) == 1 &&
						(	recordQueueManager.getQueueProducers(config.getOutputQueue()).length == 0 ||
							recordQueueManager.checkForQueueProducer(config.getOutputQueue(), Constants.PROC_RECORD_PRODUCER_NAME)
						)
					)
					||
					( 
						recordQueueManager.createQueue(config.getOutputQueue(), config.getOutputQueueRecordCapacity(), config.getOutputQueueTimeCapacity(), 1, null, null) &&
						(createdQueue=true)
					)
				 )
			  )	
			{
				LOG.error("Coordinator: Failed to enable metric because output RecordQueue \"" + 
						config.getOutputQueue() + "\" could not be created");
				return false;
			}
			if(
				!(
					recordQueueManager.checkForQueueProducer(config.getOutputQueue(), Constants.PROC_RECORD_PRODUCER_NAME) ||
					(
						recordQueueManager.registerProducer(config.getOutputQueue(), Constants.PROC_RECORD_PRODUCER_NAME) &&
						(registeredProducer=true)
					)
				 )
			  )
			{
				LOG.error("Coordinator: Failed to enable metric because producer \"" + 
						Constants.PROC_RECORD_PRODUCER_NAME + "\" could not be registered with queue \"" + config.getOutputQueue() + "\"");
				if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
					LOG.error("Coordinator: Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
				return false;
			}
			try {
				procRecordProducer.enableMetric(config.getProcRecordProducerMetricName(), recordQueueManager.getQueue(config.getOutputQueue()), config.getPeriodicity());
			} catch (Exception e) {
				LOG.error("Coordinator: Failed to enable ProcRecordProducer metric " + config.getProcRecordProducerMetricName() + " with exception:");
				e.printStackTrace();
				if(registeredProducer && !recordQueueManager.unregisterProducer(config.getOutputQueue(), Constants.PROC_RECORD_PRODUCER_NAME))
					LOG.error("Coordinator: Failed to unregister producer \"" + Constants.PROC_RECORD_PRODUCER_NAME + 
							"\" from queue \"" + config.getOutputQueue() + "\" while cleaning up");
				if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
					LOG.error("Coordinator: Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
				return false;
			}
						if(config.getRawProducerMetricsEnabled()){
				config.setRawRecordProducerName(Constants.PROC_RECORD_PRODUCER_NAME);
				enableRawRecordProducerStats(config);
			}
			
			return true;
		}
	}
	//Call this method to enable stats for a raw record producer (e.g. MfsGutsRecordProducer or ProcRecordProducer)
	private boolean enableRawRecordProducerStats(MetricConfig config){
		boolean createdQueue = false, registeredProducer = false;
		synchronized(coordinatorLock){
			if (!config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME) &&
					!config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)){
					LOG.warn("Coordinator: Unknown raw.record.producer.name: " + config.getRawRecordProducerName());
					return false;
				}
					
				if(	!(	recordQueueManager.queueExists(Constants.RAW_PRODUCER_STATS_QUEUE_NAME) || 
						(	recordQueueManager.createQueue( Constants.RAW_PRODUCER_STATS_QUEUE_NAME, 
															Constants.RAW_PRODUCER_STATS_QUEUE_RECORD_CAPACITY, 
															Constants.RAW_PRODUCER_STATS_QUEUE_TIME_CAPACITY, 0, null, null) && 
							(createdQueue=true)	
						)
					 )
				)	
				{
					LOG.error("Coordinator: Failed to retrieve output queue for raw producer metrics: \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\"");
				} 
				else if(	!(	recordQueueManager.checkForQueueProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName()) ||
								(	recordQueueManager.registerProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName()) &&
										(registeredProducer=true)
								)
						 	 )
					   )
				{
					LOG.error("Coordinator: Failed to register \"" + config.getRawRecordProducerName() + 
								"\" as a producer with queue \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\"");
					if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
						LOG.error("Coordinator: Failed to delete queue \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + 
							"\" while cleaning up.");
				}
				else if (config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)) {
					if(mfsGutsRecordProducer==null){
						LOG.error("Failed to enable MfsGutsRecordProducer stats because it is not yet initialized");
						return false;
					}
					if (!mfsGutsRecordProducer.producerMetricsEnabled() && 
							!mfsGutsRecordProducer.enableProducerMetrics(recordQueueManager.getQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
						   )
						{
							LOG.error("Coordinator: Failed to enable raw producer metrics for ProcRecordProducer");
							if (registeredProducer && 
								!recordQueueManager.unregisterProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName())
							   )
								LOG.error("Coordinator: Failed to unregister producer \"" + 
										config.getRawRecordProducerName() + "\" while cleaning up.");
							if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
								LOG.error("Coordinator: Failed to delete queue \"" + 
										Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\" while cleaning up.");
						}
				}
				else if (config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME)) {
					if (!procRecordProducer.producerMetricsEnabled() && 
						!procRecordProducer.enableProducerMetrics(recordQueueManager.getQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
					   )
					{
						LOG.error("Coordinator: Failed to enable raw producer metrics for ProcRecordProducer");
						if (registeredProducer && 
							!recordQueueManager.unregisterProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName())
						   )
							LOG.error("Coordinator: Failed to unregister producer \"" + 
									config.getRawRecordProducerName() + "\" while cleaning up.");
						if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
							LOG.error("Coordinator: Failed to delete queue \"" + 
									Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\" while cleaning up.");
					}
				}
				return true;
		}
	}
	/**
	 * Methods related to disabling metrics
	 */
	// Call this method to disable a metric based on the id parameter of a
	// MetricConfig
	public void disableMetric(String metricName) throws Exception {
		synchronized (coordinatorLock) {
			MetricAction metricToDisable = metricActionsIdMap.get(metricName);
			if(metricToDisable!=null){
				try {
					disableMetricAction(metricToDisable);
				} catch (Exception e){
					throw new Exception("Failed to disable metric " + metricToDisable.getId(), e);
				}
			} else {
				if(metricConfigMap.containsKey(metricName)){
					MetricConfig configForMetric = metricConfigMap.get(metricName);
					if (!isRawRecordType(configForMetric.getRecordType()) &&
						!configForMetric.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
						throw new Exception("No such metric:" + metricName);
					} else if(configForMetric.getRecordType().equals(Constants.PROC_RECORD_PRODUCER_RECORD)){
						try {
							disableProcRecordProducerMetric(configForMetric);
						} catch (Exception e) {
							throw new Exception("Failed to disable ProcRecordProducer metric " + configForMetric.getProcRecordProducerMetricName(), e);
						}
					} else if (configForMetric.getRecordType().equals(Constants.MFS_GUTS_RECORD_PRODUCER_RECORD)) {
						try {
							disableMfsGutsRecordProducer(configForMetric);
						} catch (Exception e) {
							throw new Exception("Failed to disable MfsGutsRecordProducer", e);
						}
					} else if (configForMetric.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)) {
						try {
							disableRawRecordProducerStats(configForMetric);
						} catch (Exception e) {
							throw new Exception("Failed to disable raw record producer stats for " + configForMetric.getRawRecordProducerName(), e);
						}
					} else {
						throw new Exception("Failed to disable metric " + metricName + " due to unknown record type " + configForMetric.getRecordType());
					}
				} else {
					throw new Exception("No such metric: " + metricName);
				}
			}
		}
	}
	// Call this method to disable a MetricAction
	public void disableMetricAction(MetricAction metricAction) throws Exception {
		synchronized(coordinatorLock){
			if(!metricActionsEnableMap.containsKey(metricAction.getId()))
				throw new Exception("Current state for MetricAction " + metricAction.getId() + " not found");
			
			if (metricActionsIdFuturesMap.containsKey(metricAction.getId())) {
				Future<?> future = metricActionsIdFuturesMap.get(metricAction.getId());
				if(!future.isDone()){
					LOG.info(metricAction.getId() + " is still running, cancelling it");
					future.cancel(true);
					if(!future.isDone() && !future.isCancelled())
						throw new Exception("Bad state for " + metricAction.getId() + " isDone:" + future.isDone() + " isCancelled:" + future.isCancelled());
				}
				metricActionsIdFuturesMap.remove(metricAction.getId());
			}
			
			if(metricActionScheduler.contains(metricAction)){
				LOG.info("Removing " + metricAction.getId() + " from schedule");
				metricActionScheduler.unschedule(metricAction);
			}
			metricActionsEnableMap.put(metricAction.getId(), false);
			metricAction.disableMetric();
		}
	}
	// Call this method to disable MfsGutsRecordProducer
	public void disableMfsGutsRecordProducer(MetricConfig config) throws Exception{
		synchronized(coordinatorLock){
			if(mfsGutsRecordProducer == null){
				throw new Exception("Failed to disable metric because MfsGutsRecordProducer is not initialized");
			}
			mfsGutsRecordProducer.requestExit();
			while(mfsGutsRecordProducer.isAlive()){
				try {
					Thread.sleep(100);
				} catch (Exception e){}
			}
			recordQueueManager.unregisterProducer(config.getOutputQueue(), Constants.MFS_GUTS_RECORD_PRODUCER_NAME);
			recordQueueManager.deleteQueue(config.getOutputQueue());
		}
	}
	//Call this method to disable a ProcRecordProducer metric 
	private void disableProcRecordProducerMetric(MetricConfig config) throws Exception {
		synchronized(coordinatorLock){
			if(!ProcRecordProducer.isValidMetricName(config.getProcRecordProducerMetricName())){
				throw new Exception("Can not disable ProcRecordProducer metric due to invalid metric name: \"" + config.getProcRecordProducerMetricName() + "\"");
			}
			if(procRecordProducer == null){
				throw new Exception("Failed to disable metric because ProdRecordProducer is not initialized");
			}
			procRecordProducer.disableMetric(config.getProcRecordProducerMetricName(), recordQueueManager.getQueue(config.getOutputQueue()), config.getPeriodicity());
			recordQueueManager.unregisterProducer(config.getOutputQueue(), Constants.PROC_RECORD_PRODUCER_NAME);
			recordQueueManager.deleteQueue(config.getOutputQueue());
		}
	}
	// Call this method to disable a stats for a raw record producer (e.g.
	// MfsGutsRecordProducer or ProcRecordProducer)
	public void disableRawRecordProducerStats(MetricConfig config)
			throws Exception {
		synchronized(coordinatorLock) {
			if (config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)) {
				if(mfsGutsRecordProducer==null){
					throw new Exception("Failed to disable MfsGutsRecordProducer stats because it is not yet initialized");
				}
				mfsGutsRecordProducer.disableProducerMetrics();
			} else if (config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME)) {
				if(procRecordProducer==null){
					throw new Exception("Failed to disable ProcRecordProducer stats because it is not initialized");
				}
				procRecordProducer.disableProducerMetrics();
			} else {
				throw new Exception("Can not disable raw record producer stats, invalid producer name:" + config.getRawRecordProducerName());
			}
			recordQueueManager.unregisterProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName());
			recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME);
		}
	}

	/**
	 * Method to delete a metric
	 */
	public void deleteMetric(String metricName) throws Exception {
		LOG.info("Request to delete metric " + metricName);
		synchronized(coordinatorLock){
			MetricConfig config = metricConfigMap.get(metricName);
			if(config == null){
				throw new Exception("Unknown metric: " + metricName);
			}
			if (config.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD) ||
				isRawRecordType(config.getRecordType()) ){
				return;
			}
			if (!metricActionsEnableMap.containsKey(config.getId()) ||
				!metricActionsIdMap.containsKey(config.getId())) {
				throw new Exception("Metric " + metricName + " is in an inconsistent state: enableMap:" + 
						metricActionsEnableMap.containsKey(config.getId()) + " idMap:" + 
						metricActionsIdMap.containsKey(config.getId()));
			}
			if(metricActionsEnableMap.get(config.getId()))
				throw new Exception("Can not delete metric " + metricName + " because it is currently enabled");
			
			metricActionsIdFuturesMap.remove(config.getId());
			metricActionsEnableMap.remove(config.getId());
			metricActionsIdMap.remove(config.getId());
			metricConfigMap.remove(config.getId());
			LOG.info("Deleted metric " + metricName);
		}
	}

	//Methods for error checking configuration
	public static boolean isRawRecordProducerName(String name){
		if(name==null) return false;
		if (name.equals(Constants.PROC_RECORD_PRODUCER_NAME) ||
			name.equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)
		)
			return true;
		return false;
	}
	public static boolean isRawRecordType(String name){
		if(name==null) return false;
		if (name.equals(Constants.PROC_RECORD_PRODUCER_RECORD) ||
			name.equals(Constants.MFS_GUTS_RECORD_PRODUCER_RECORD)
		)
			return true;
		return false;
	}
	public static boolean processorMethodRequiresThresholdKey(String name){
		if(name==null) return false;
		if (name.equals(Constants.IS_ABOVE) ||
			name.equals(Constants.IS_BELOW) ||
			name.equals(Constants.IS_EQUAL) ||
			name.equals(Constants.IS_NOT_EQUAL) ||
			name.equals(Constants.DIFFERENTIAL)
		)
			return true;
		return false;
	}
	public static boolean processorMethodRequiresThresholdValue(String name){
		if(name==null) return false;
		if (name.equals(Constants.IS_ABOVE) ||
			name.equals(Constants.IS_BELOW) ||
			name.equals(Constants.IS_EQUAL) ||
			name.equals(Constants.IS_NOT_EQUAL)
		)
			return true;
		return false;
	}
	public static boolean selectorRequiresQualifierKey(String name){
		if(name==null) return false;
		if (name.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) ||
			name.equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR)
		)
			return true;
		return false;
	}
	public static boolean methodRequiresQualifierKey(String name){
		if(name==null) return false;
		if(name.equals(Constants.DIFFERENTIAL))
			return true;
		return false;
	}
	public static boolean selectorRequiresQualifierValue(String name){
		return false;
	}
	public static boolean isValidProcessorMethod(String name){
		if(name==null) return false;
		if (name.equals(Constants.IS_ABOVE) ||
			name.equals(Constants.IS_BELOW) ||
			name.equals(Constants.IS_EQUAL) ||
			name.equals(Constants.IS_NOT_EQUAL) ||
			name.equals(Constants.MERGE_RECORDS) ||
			name.equals(Constants.DIFFERENTIAL) ||
			name.equals(Constants.CONVERT) || 
			name.equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)
		)
			return true;
		return false;
	}
	public static boolean isValidProcessorName(String name){
		if(name==null) return false;
		if (name.equals(Constants.DISKSTAT_RECORD_PROCESSOR) ||
			name.equals(Constants.MFS_GUTS_RECORD_PROCESSOR) ||
			name.equals(Constants.NETWORK_INTERFACE_RECORD_PROCESSOR) ||
			name.equals(Constants.PROCESS_RESOURCE_RECORD_PROCESSOR) ||
			name.equals(Constants.SYSTEM_MEMORY_RECORD_PROCESSOR) ||
			name.equals(Constants.SYSTEM_CPU_RECORD_PROCESSOR) ||
			name.equals(Constants.TCP_CONNECTION_STAT_RECORD_PROCESSOR) ||
			name.equals(Constants.THREAD_RESOURCE_RECORD_PROCESSOR) ||
			name.equals(Constants.SLIM_PROCESS_RESOURCE_RECORD_PROCESSOR) ||
			name.equals(Constants.SLIM_THREAD_RESOURCE_RECORD_PROCESSOR) ||
			name.equals(Constants.DIFFERENTIAL_VALUE_RECORD_PROCESSOR) ||
			name.equals(Constants.PASSTHROUGH_RECORD_PROCESSOR) || 
			name.equals(Constants.PROCESS_EFFICIENCY_RECORD_PROCESSOR) ||
			name.equals(Constants.LOAD_AVERAGE_RECORD_PROCESSOR)
		)
			return true;
		return false;
	}
	public static boolean isValidRecordSelector(String name){
		if(name==null) return false;
		if (name.equals(Constants.SEQUENTIAL_SELECTOR) ||
			name.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) ||
			name.equals(Constants.CUMULATIVE_SELECTOR) ||
			name.equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR) ||
			name.equals(Constants.TIME_SELECTOR) ||
			name.equals(Constants.PERSISTING_SELECTOR)
		)
			return true;
		return false;
	}
	public static boolean isValidRecordType(String name){
		if(name==null) return false;
		if (name.equals(Constants.SYSTEM_CPU_RECORD) ||
			name.equals(Constants.SYSTEM_MEMORY_RECORD) ||
			name.equals(Constants.TCP_CONNECTION_RECORD) ||
			name.equals(Constants.THREAD_RESOURCE_RECORD) ||
			name.equals(Constants.SLIM_PROCESS_RESOURCE_RECORD) ||
			name.equals(Constants.SLIM_THREAD_RESOURCE_RECORD) ||
			name.equals(Constants.DISK_STAT_RECORD) ||
			name.equals(Constants.NETWORK_INTERFACE_RECORD) ||
			name.equals(Constants.PROCESS_RESOURCE_RECORD) ||
			name.equals(Constants.MFS_GUTS_RECORD) ||
			name.equals(Constants.PROC_RECORD_PRODUCER_RECORD) ||
			name.equals(Constants.MFS_GUTS_RECORD_PRODUCER_RECORD) ||
			name.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD) ||
			name.equals(Constants.DIFFERENTIAL_VALUE_RECORD) || 
			name.equals(Constants.LOAD_AVERAGE_RECORD) ||
			name.equals(Constants.PROCESS_EFFICIENCY_RECORD)
		)
			return true;
		return false;
	}
	public static boolean isValidRelatedSelectorName(String name){
		if(name==null) return false;
		if (name.equals(Constants.BASIC_RELATED_RECORD_SELECTOR)
		)
			return true;
		return false;
	}
	public static boolean isValidRelatedSelectorMethod(String name){
		if(name==null) return false;
		if (name.equals(Constants.TIME_BASED_WINDOW)
		)
			return true;
		return false;
	}
	public static boolean selectorSupportsMethod(String selector, String method){
		//Persisting selection does not apply any processing to records so this always returns true
		//All processing of records should be done prior to passing to persisting selector.
		if(selector.equals(Constants.PERSISTING_SELECTOR))
			return true;
		
		//Something is wrong in this case, probably a bad configuration from the user.
		if(selector==null || method==null) return false;
		
		if (selector.equals(Constants.SEQUENTIAL_SELECTOR)){
			if (method.equals(Constants.IS_ABOVE) ||
				method.equals(Constants.IS_BELOW) ||
				method.equals(Constants.IS_EQUAL) ||
				method.equals(Constants.IS_NOT_EQUAL) ||
				method.equals(Constants.MERGE_RECORDS) ||
				method.equals(Constants.DIFFERENTIAL) ||
				method.equals(Constants.CONVERT) || 
				method.equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)
			){
				return true;
			}
		} else if(selector.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR)){
			if (method.equals(Constants.MERGE_RECORDS) ||
				method.equals(Constants.DIFFERENTIAL) ||
				method.equals(Constants.MERGE_CHRONOLOGICALLY_CONSECUTIVE)){
				return true;
			}
		} else if(selector.equals(Constants.CUMULATIVE_SELECTOR)){
			if (method.equals(Constants.MERGE_RECORDS) ||
				method.equals(Constants.DIFFERENTIAL) ){
				return true;
			}
		} else if(selector.equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR)){
			if (method.equals(Constants.MERGE_RECORDS) ||
				method.equals(Constants.DIFFERENTIAL) ){
				return true;
			}
		} else if(selector.equals(Constants.TIME_SELECTOR)){
			if (method.equals(Constants.MERGE_RECORDS) ||
				method.equals(Constants.DIFFERENTIAL) ){
				return true;
			}
		}
		return false;
	}
	
	public static boolean inputQueueRequiredForType(String inputQueueType){
		if(inputQueueType == null){
			return true;
		}
		if (isPersistanceInputQueueType(inputQueueType))
			return false;
		return true;
	}
	public static boolean isPersistanceInputQueueType(String inputQueueType){
		if (inputQueueType.equals(Constants.MAPRDB_INPUT_RECORD_QUEUE) ||
			inputQueueType.equals(Constants.LOCAL_FILE_INPUT_RECORD_QUEUE))
			return true;
		return false;
	}
	public static boolean isValidPersistor(String name){
		if(name == null)
			return false;
		if (name.equals(Constants.MAPRDB) ||
			name.equals(Constants.LOCAL_FILE_SYSTEM) )
			return true;
		return false;
	}


	@Override
	public RecordProducerStatus getRecordProducerStatus() {
		boolean isMfsGutsRecordProducerRunning = mfsGutsRecordProducer != null
				&& mfsGutsRecordProducer.isAlive();
		boolean isMfsGutsRecordProducerMetricsEnabled = mfsGutsRecordProducer != null
				&& mfsGutsRecordProducer.producerMetricsEnabled();

		boolean isProcRecordProducerRunning = procRecordProducer != null
				&& procRecordProducer.isAlive();
		boolean isProcRecordProducerMetricsEnabled = procRecordProducer != null
				&& procRecordProducer.producerMetricsEnabled();

		RecordProducerStatus producerStatus = new RecordProducerStatus(
				isMfsGutsRecordProducerRunning,
				isMfsGutsRecordProducerMetricsEnabled,
				isProcRecordProducerRunning,
				isProcRecordProducerMetricsEnabled,
				procRecordProducer.listEnabledMetrics());

		/*
		 * StringBuffer status = new StringBuffer();
		 * LOG.debug("\tMfsGutsRecordProducer is " + ((mfsGutsRecordProducer !=
		 * null) ? ((mfsGutsRecordProducer .isAlive()) ? "" : "not ") : "not ")
		 * + "running and producer metrics are " + ((mfsGutsRecordProducer !=
		 * null && mfsGutsRecordProducer .producerMetricsEnabled()) ? "enabled"
		 * : "disabled")); LOG.debug("\tProcRecordProducer is " +
		 * ((procRecordProducer != null) ? ((procRecordProducer .isAlive()) ? ""
		 * : "not ") : "not ") + "running and producer metrics are " +
		 * ((procRecordProducer != null && procRecordProducer
		 * .producerMetricsEnabled()) ? "enabled" : "disabled")); if
		 * (procRecordProducer != null) {
		 * LOG.debug("\tEnabled ProcRecordProducer metrics:");
		 * 
		 * for (String s : procRecordProducer.listEnabledMetrics()) {
		 * status.append("\t\t"); status.append(s); status.append("\n");
		 * LOG.debug("\t\t" + s); } }
		 */
		return producerStatus;
	}

	@Override
	public List<MetricActionStatus> getMetricActions() {
		LOG.debug("\tMetricAction details:");
		List<MetricActionStatus> metricActionStatuses = new ArrayList<MetricActionStatus>();

		MetricActionStatus metricActionStatus;

		for (String metricId : metricActionsIdMap.keySet()) {
			MetricAction metricAction = metricActionsIdMap.get(metricId);
			boolean isRunning = metricActionsIdFuturesMap
					.containsKey(metricAction.getId())
					&& !metricActionsIdFuturesMap.get(metricAction.getId())
							.isDone();
			boolean isScheduled = metricActionScheduler.contains(metricAction);
			metricActionStatus = new MetricActionStatus(metricId,
					metricAction.getMetricEnabled(), isRunning, isScheduled,
					metricAction.printSchedule());
			metricActionStatuses.add(metricActionStatus);
		}

		return metricActionStatuses;

		/*
		 * StringBuffer status = new StringBuffer();
		 * 
		 * Iterator<Map.Entry<String, MetricAction>> i = metricActionsIdMap
		 * .entrySet().iterator(); while (i.hasNext()) { Map.Entry<String,
		 * MetricAction> e = i.next(); status.append("\t\tID:" + e.getKey() +
		 * " enabled:" + e.getValue().getMetricEnabled() + " running:" +
		 * ((metricActionsIdFuturesMap.containsKey(e.getValue() .getId()) &&
		 * !metricActionsIdFuturesMap.get( e.getValue().getId()).isDone())) +
		 * " inSched:" + metricActionScheduler.contains(e.getValue()) +
		 * " sched:" + e.getValue().printSchedule() + "\n");
		 * 
		 * LOG.debug("\t\tID:" + e.getKey() + " enabled:" +
		 * e.getValue().getMetricEnabled() + " running:" +
		 * ((metricActionsIdFuturesMap.containsKey(e.getValue() .getId()) &&
		 * !metricActionsIdFuturesMap.get( e.getValue().getId()).isDone())) +
		 * " inSched:" + metricActionScheduler.contains(e.getValue()) +
		 * " sched:" + e.getValue().printSchedule()); } return
		 * status.toString();
		 */
	}
	
	@Override
	public MetricActionStatus getMetricAction(String metricActionName) throws Exception {
		if (metricActionsIdMap.containsKey(metricActionName)) {
			MetricAction metricAction = metricActionsIdMap
					.get(metricActionName);
			MetricActionStatus metricActionStatus;

			boolean isRunning = metricActionsIdFuturesMap
					.containsKey(metricAction.getId())
					&& !metricActionsIdFuturesMap.get(metricAction.getId())
							.isDone();
			boolean isScheduled = metricActionScheduler.contains(metricAction);
			metricActionStatus = new MetricActionStatus(metricAction.getId(),
					metricAction.getMetricEnabled(), isRunning, isScheduled,
					metricAction.printSchedule());
			return metricActionStatus;
		}
		
		throw new Exception("Not a valid MetricAction " + metricActionName);
	}

	@Override
	public boolean metricDisable(String metricName) throws Exception {
		try {
			disableMetric(metricName);
			return true;
		}

		catch (Exception e) {
			LOG.error("Metric " + metricName + " cannot be disabled - "
					+ e.getMessage());
			throw e;
		}
	}

	@Override
	public boolean metricEnable(String metricName) throws Exception {
		try {
			enableMetric(metricName);
			return true;
		}

		catch (Exception e) {
			LOG.error("Metric " + metricName + " cannot be enabled - "
					+ e.getMessage());
			throw e;
		}
	}

	@Override
	public boolean metricDelete(String metricName) throws Exception {
		try {
			deleteMetric(metricName);
			return true;
		}

		catch (Exception e) {
			LOG.error("Metric " + metricName + " cannot be deleted - "
					+ e.getMessage());
			throw e;
		}
	}

	@Override
	public List<RecordQueueStatus> getRecordQueues() {
		RecordQueue[] queues = recordQueueManager.getQueues();

		List<RecordQueueStatus> queueStatuses = new ArrayList<RecordQueueStatus>();
		RecordQueueStatus queueStatus;

		for (RecordQueue queue : queues) {
			queueStatus = new RecordQueueStatus(queue.getQueueName(),
					queue.getQueueType(), queue.getQueueRecordCapacity(),
					queue.queueSize(), queue.listProducers(),
					queue.listConsumers());
			queueStatuses.add(queueStatus);
		}
		/*
		 * StringBuffer status = new StringBuffer();
		 * 
		 * LOG.debug("\tRecordQueue details:"); for (int x = 0; x <
		 * queues.length; x++) { status.append("\t\tQueue " +
		 * queues[x].getQueueName() + " contains " + queues[x].queueSize() +
		 * " records with time capacity " + queues[x].getQueueTimeCapacity() +
		 * " and time usage " + (System.currentTimeMillis() - queues[x]
		 * .getOldestRecordTimestamp()) + "ms and record capacity " +
		 * queues[x].getQueueRecordCapacity() + " (" + (((double)
		 * queues[x].queueSize()) / ((double)
		 * queues[x].getQueueRecordCapacity()) * 100d) + "%)" + " cl:" +
		 * queues[x].listConsumers().length + " pl:" +
		 * queues[x].listProducers().length + "\n");
		 * 
		 * LOG.debug("\t\tQueue " + queues[x].getQueueName() + " contains " +
		 * queues[x].queueSize() + " records with time capacity " +
		 * queues[x].getQueueTimeCapacity() + " and time usage " +
		 * (System.currentTimeMillis() - queues[x] .getOldestRecordTimestamp())
		 * + "ms and record capacity " + queues[x].getQueueRecordCapacity() +
		 * " (" + (((double) queues[x].queueSize()) / ((double)
		 * queues[x].getQueueRecordCapacity()) * 100d) + "%)" + " cl:" +
		 * queues[x].listConsumers().length + " pl:" +
		 * queues[x].listProducers().length); //
		 * if(queues[x].getQueueName().equals("HighMfsThreadCpu-1s")){ //
		 * LOG.debug(queues[x].printNewestRecords(null, 5)); // } // if //
		 * (queues
		 * [x].getQueueName().equals(Constants.RAW_PRODUCER_STATS_QUEUE_NAME) //
		 * || //
		 * queues[x].getQueueName().equals(Constants.METRIC_ACTION_STATS_QUEUE_NAME
		 * )){ // LOG.debug(queues[x].printNewestRecords(null, 5)); // } }
		 * return status.toString();
		 */
		return queueStatuses;
	}

	@Override
	public RecordQueueStatus getQueueStatus(String queueName) throws Exception {
		if (recordQueueManager.queueExists(queueName)) {
			RecordQueue queue = recordQueueManager.getQueue(queueName);
			RecordQueueStatus queueStatus = new RecordQueueStatus(
					queue.getQueueName(), queue.getQueueType(),
					queue.getQueueRecordCapacity(), queue.queueSize(),
					queue.listProducers(), queue.listConsumers());
			return queueStatus;
		}
		
		throw new Exception("Not a valid queue " + queueName);
	}
	
	@Override
	public boolean requestShutdown(){
		shouldExit = true; 
		LOG.info("Coordinator received shutdown request.");
		return true;
	}

	@Override
	public Record[] getRecords(String queueName, int count) throws Exception {
		if (recordQueueManager.queueExists(queueName)) {
			SubscriptionRecordQueue queue = (SubscriptionRecordQueue) recordQueueManager
					.getQueue(queueName);
			return queue.dumpNewestRecords(count);
		}

		throw new Exception("Not a valid queue " + queueName);
	}

	@Override
	public boolean isScheduledMetricAction(String metricAction) throws Exception {
		if (metricActionsIdMap.containsKey(metricAction)) {
			MetricAction action = metricActionsIdMap.get(metricAction);
			return metricActionScheduler.contains(action);
		}
		
		throw new Exception("Not a valid MetricAction " + metricAction);
	}

	@Override
	public boolean isRunningMetricAction(String metricAction) throws Exception {
		if (metricActionsIdMap.containsKey(metricAction)) {
		  LOG.error("Found in Map "+metricAction);
			return metricActionsIdFuturesMap.containsKey(metricAction)
					&& !metricActionsIdFuturesMap.get(metricAction).isDone();
		}
		
		throw new Exception("Not a valid MetricAction " + metricAction);
	}

	public void init(String configLocation) {
		this.configFileLocation = configLocation;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		String configLocation = "/opt/mapr/conf/distiller.conf";
		if(args.length == 0){
			LOG.debug("Main: Using default configuration file location: " + configLocation);
		} else {
			configLocation = args[0];
			LOG.debug("Main: Using custom configuration file location: " + configLocation);
		}
		
		Coordinator coordinator = new Coordinator();
		coordinator.init(configLocation);
		Thread coordinatorThread = new Thread(coordinator, "Coordinator");
		coordinatorThread.start();

		LOG.info("Main: Shutting down.");
	}			

	@Override
	public void run() {
		shouldExit=false;
		long statusInterval = 3000l;
		long lastStatus = System.currentTimeMillis();

		File configFileHandle = new File(configFileLocation);
                if (!configFileHandle.exists()) {
                        LOG.error("FATAL: Config file not found: " + configFileLocation);
                        System.exit(1);
                }

		coordinatorLock = new Object();

		this.recordQueueManager = new RecordQueueManager();
		this.procRecordProducer = new ProcRecordProducer(Constants.PROC_RECORD_PRODUCER_NAME);
		this.metricActionsIdMap = new TreeMap<String, MetricAction>();
		this.metricActionsIdFuturesMap = new TreeMap<String, Future<MetricAction>>();
		this.metricActionsEnableMap = new TreeMap<String, Boolean>();
		this.metricConfigMap = new TreeMap<String, MetricConfig>();
		//this.maprdbPersistanceManager = null;
		this.maprdbSyncPersistanceManager = null;

		try {
			ProcessResourceRecord self = new ProcessResourceRecord("/proc/self/stat", "/proc/self/io", 100);
			this.myPid = self.get_pgrp();
			this.myStarttime = self.get_starttime();
		} catch (Exception e) {
			System.err.println("Failed to retrieve the PID and starttime of this process from /proc/self/stat");
			e.printStackTrace();
		}
		RandomAccessFile maprdbHostnameFile = null;
		try {
			maprdbHostnameFile = new RandomAccessFile("/opt/mapr/hostname", "r");
			this.maprdbHostname = maprdbHostnameFile.readLine();
		} catch (Exception e) {
			System.err.println("Failed to retrieve hostname from /opt/mapr/hostname, falling back to generic lookup.");
			BufferedReader r = null;
			try {
				Process p = (new ProcessBuilder("hostname", "-f")).start();
				r = new BufferedReader(new InputStreamReader(p.getInputStream()));
				this.maprdbHostname = r.readLine();
				p.waitFor();
			} catch (Exception e2) {
				System.err.println("Failed to retrieve hostname using \"hostname -f\"");
				e2.printStackTrace();
				System.exit(1);
			} finally {
				try {
					r.close();
				} catch (Exception e2){}
			}
		} finally {
			try {
				maprdbHostnameFile.close();
			} catch (Exception e) {}
		}


		// Hard coding a fuzzy window size of 20ms
		// This means that a metric may be triggered up to 20ms before it is
		// actually supposed to be gathered per schedule.
		// The reason for this is to avoid requiring extra code/waiting to run
		// in the scheduler when the precision of when a metric needs to be
		// processed is low.
		// E.g. since the records this scheduler will process are already
		// timestamped, we don't need to be worried about the exact time that
		// the processing happens.
		try {
			this.metricActionScheduler = new MetricActionScheduler(20);
		} catch (Exception e) {
			LOG.error("Failed to setup MetricActionScheduler" + e.toString());
			System.exit(1);
		}

		Config config = null;
		try {
			config = ConfigFactory.parseFile(configFileHandle);
		} catch (Exception e){
			LOG.error("Exception while parsing configuration file", e);
			config = null;
		}
		if (config == null) {
			LOG.error("Main: Failed to parse config file from " + configFileLocation);
			System.exit(1);
		}

		procRecordProducer.start();
		try {
			createMetricConfigs(config);
		} catch (Exception e) {
			LOG.error("Main: Failed to create metric configs due to exception:");
			e.printStackTrace();
			System.exit(1);
		}

		try {
			createMetricActions();
		} catch (Exception e) {
			LOG.error("Main: Failed to create metric actions due to exception:");
			e.printStackTrace();
			System.exit(1);
		}

		while (!shouldExit) {
			MetricAction nextAction = null;
			try {
				nextAction = metricActionScheduler
						.getNextScheduledMetricAction(true);
			} catch (Exception e) {
				LOG.error("Main: FATAL: Failed to retrieve next scheduled metric action");
				e.printStackTrace();
				System.exit(1);
			}
			synchronized (coordinatorLock) {
				if (metricActionsEnableMap.containsKey(nextAction.getId())
						&& metricActionsEnableMap.get(nextAction.getId())) {
					/**
					Future<?> future = metricActionsIdFuturesMap.get(nextAction
							.getId());
					
					if (future != null && !future.isDone()) {
						LOG.warn(System.currentTimeMillis() + " Metric "
								+ nextAction.getId()
								+ " is scheduled to run now but previous run is not done, advancing it's scheduled start time " 
								+ nextAction.printSchedule());
						nextAction.advanceSchedule();
						try {
							metricActionScheduler.schedule(nextAction);
							LOG.info(System.currentTimeMillis() + " Rescheduled " + nextAction.getId() + " to run " + nextAction.printSchedule());
						} catch (Exception e) {
							//We should never be here
							LOG.error("MetricAction-" + System.identityHashCode(this) + ": Unhandled exception", e);
							System.exit(1);
						}
					} else {
						future = executor.submit(nextAction);
						LOG.info(System.currentTimeMillis() + " Started " + nextAction.getId());
						metricActionsIdFuturesMap.put(nextAction.getId(),
								((Future<MetricAction>) future));
					}
					**/
					metricActionsIdFuturesMap.put(nextAction.getId(), ((Future<MetricAction>)executor.submit(nextAction)));
				} else {
					LOG.info("Main: dropping metric "
							+ nextAction.getId()
							+ " as it was disabled after retrieval from schedule.");
				}
			}

			
			//Some status stuff that is useful in debugging
			if(System.currentTimeMillis() >= lastStatus + statusInterval){
				lastStatus = System.currentTimeMillis();
				LOG.info("Main: Printing status at " + lastStatus);
				synchronized(coordinatorLock){
					RecordQueue[] queues = recordQueueManager.getQueues();
					LOG.info("\tMfsGutsRecordProducer is " + 
							((mfsGutsRecordProducer != null) ? ((mfsGutsRecordProducer.isAlive()) ? "" : "not ") : "not ") + 
							"running and producer metrics are " + 
							((mfsGutsRecordProducer != null && mfsGutsRecordProducer.producerMetricsEnabled()) ? "enabled" : "disabled"));
					LOG.info("\tProcRecordProducer is " + 
							((procRecordProducer != null) ? ((procRecordProducer.isAlive()) ? "" : "not ") : "not ") + 
							"running and producer metrics are " + 
							((procRecordProducer != null && procRecordProducer.producerMetricsEnabled()) ? "enabled" : "disabled"));
					if(procRecordProducer != null){
						LOG.info("\tEnabled ProcRecordProducer metrics:");
						for(String s : procRecordProducer.listEnabledMetrics()){
							LOG.debug("\t\t" + s);
						}
					}
					LOG.info("\tRecordQueue details:");
					for(int x=0; x<queues.length; x++){
						LOG.info("\t\tQueue " + queues[x].getQueueName() + " contains " + queues[x].queueSize() + " records with time capacity " + 
								queues[x].getQueueTimeCapacity() + " and time usage " + (System.currentTimeMillis() - queues[x].getOldestRecordTimestamp()) + 
								"ms and record capacity " + queues[x].getQueueRecordCapacity() + " (" + 
								(((double)queues[x].queueSize())/((double)queues[x].getQueueRecordCapacity())*100d) + "%)" + 
								" cl:" + queues[x].listConsumers().length +
								" pl:" + queues[x].listProducers().length);
						//if(queues[x].getQueueName().equals("HighMfsThreadCpu-1s")){
						//	LOG.debug(queues[x].printNewestRecords(null, 5));
						//}
						//if (queues[x].getQueueName().equals(Constants.RAW_PRODUCER_STATS_QUEUE_NAME) || 
						//	queues[x].getQueueName().equals(Constants.METRIC_ACTION_STATS_QUEUE_NAME)){
						//	LOG.debug(queues[x].printNewestRecords(null, 5));
						//}
					}
					LOG.info("\tMetricAction details:");
					Iterator<Map.Entry<String, MetricAction>> i = metricActionsIdMap.entrySet().iterator();
					while(i.hasNext()){
						Map.Entry<String, MetricAction> e = i.next();
						LOG.info("\t\tID:" + e.getKey() + " enabled:" + e.getValue().getMetricEnabled() + " running:" + ((metricActionsIdFuturesMap.containsKey(e.getValue().getId()) && !metricActionsIdFuturesMap.get(e.getValue().getId()).isDone())) + " inSched:" + metricActionScheduler.contains(e.getValue()) + " sched:" + e.getValue().printSchedule());
						if( e.getValue().getMetricEnabled() &&
						   !((metricActionsIdFuturesMap.containsKey(e.getValue().getId()) && !metricActionsIdFuturesMap.get(e.getValue().getId()).isDone())) &&
						   !metricActionScheduler.contains(e.getValue()) ){
							LOG.error("Metric is in inconsistent state.");
							System.exit(1);
						}
					}
					i = metricActionsIdMap.entrySet().iterator();
					LOG.info("\tMetricAction counters:");
					while(i.hasNext()){
						Map.Entry<String, MetricAction> e = i.next();
						long[] counters = e.getValue().getCounters();
						LOG.info("\t\tID:" + e.getKey() + " inRec:" + counters[0] + " outRec:" + counters[1] + " processingFail:" + counters[2] + " putFail:" + counters[3] + " otherFail:" + counters[4] + " runningTime:" + counters[5] + " startTime:" + counters[6]);
					}
					//if(maprdbPersistanceManager != null){
					//	LOG.info("\tMapRDBPersistanceManager details:");
					//	maprdbPersistanceManager.logPersistorStatus();
					if(maprdbSyncPersistanceManager != null){
						LOG.info("\tMapRDBSyncPersistanceManager details:");
						maprdbSyncPersistanceManager.logPersistorStatus();
					}
					if(localFileSystemPersistanceManager != null){
						LOG.info("\tLocalFileSystemPersistanceManager details:");
						localFileSystemPersistanceManager.logPersistorStatus();
					}
					
				}
			}
			
		}
		
		synchronized(coordinatorLock){
			LOG.info("Beginning shutdown");
			
			boolean cleanShutdown = true;
			
			//Step 1 of shutdown, stop all raw record producers that no new Records are being generate
			for(MetricConfig c : metricConfigMap.values()){
				if(isRawRecordType(c.getRecordType()))
					try {
						disableMetric(c.getId());
					} catch (Exception e) {
						LOG.error("Failed to disable ProcRecordProducer metric " + c.getId(), e);
						System.exit(1);
					}
			}
			if(procRecordProducer != null && procRecordProducer.isAlive()){
				LOG.info("Stopping ProcRecordProducer");
				procRecordProducer.requestExit();
			}
			//Just null check here because it's possible MfsGutsRecordProducer thread stopped but MfsGutsStdoutRecordProducer thread is still running
			if(mfsGutsRecordProducer != null){
				LOG.info("Stopping MfsGutsRecordProducer");
				mfsGutsRecordProducer.requestExit();
			}
			while
			( ( procRecordProducer != null && procRecordProducer.isAlive() )
			  ||
			  ( mfsGutsRecordProducer != null
			    &&
			    ( mfsGutsRecordProducer.isAlive()
			      ||
			      mfsGutsRecordProducer.mfsGutsStdoutRecordProducerIsAlive()
			    )
			  )
			)
			{
				try {
					Thread.sleep(1000);
				} catch (Exception e){}
			}
			
			LOG.info("Raw record producers have shut down.");
			
			//Step 2 of shutdown, kill running persistance input MetricActions, wait for other MetricActions to finish running
			Iterator<Map.Entry<String, Future<MetricAction>>> i = metricActionsIdFuturesMap.entrySet().iterator();
			while(i.hasNext()){
				Map.Entry<String, Future<MetricAction>> e = i.next();
				String metricId = e.getKey();
				Future<MetricAction> f = e.getValue();
				MetricAction ma = metricActionsIdMap.get(metricId);
				if( !f.isDone() && 
					!f.isCancelled() &&
					ma.getConfig().getInputQueueType() != null &&
					isPersistanceInputQueueType(ma.getConfig().getInputQueueType()) )
				{
					LOG.info("Attempting to stop running MetricAction " + 
							metricId + " from consuming from " + 
							ma.getConfig().getInputQueueType());
					f.cancel(true);
				}
			}
			LOG.info("Waiting for running MetricActions to complete.");
			i = metricActionsIdFuturesMap.entrySet().iterator();
			while(i.hasNext()){
				Map.Entry<String, Future<MetricAction>> e = i.next();
				Future<MetricAction> f = e.getValue();
				while(!f.isDone() && !f.isCancelled()){
					try {
						Thread.sleep(1000);
					} catch (Exception e2) {}
				}		
			}
			
			//Step 3, delete MetricActions that are disabled, delete/disable any MetricActions that read data from a persisted location
			LOG.info("Disabling MetricActions that use Persistance input queues");
			int numChecked=0;
			while(numChecked != metricActionsIdMap.size()){
				numChecked=0;
				Iterator<Map.Entry<String, MetricAction>> maI = metricActionsIdMap.entrySet().iterator();
				while(maI.hasNext()){
					Map.Entry<String, MetricAction> e = maI.next();
					MetricAction ma = e.getValue();
					if(!metricActionsEnableMap.get(ma.getId())){
						try {
							LOG.debug("Deleting disabled Metric: " + ma.getId());
							deleteMetric(ma.getId());
							break;
						} catch (Exception e2){}
					} 
					else if
					( ma.getConfig().getInputQueueType() != null &&
					  ( ma.getConfig().getInputQueueType().equals(Constants.LOCAL_FILE_INPUT_RECORD_QUEUE)
						||
						ma.getConfig().getInputQueueType().equals(Constants.MAPRDB_INPUT_RECORD_QUEUE)
					  )
					)
					{
						try {
							LOG.debug("Disabled and deleting Metric with persistance input queue: " + ma.getId());
							disableMetricAction(ma);
							deleteMetric(ma.getId());
							break;
						} catch (Exception e2){
							LOG.error("Failed to disable/delete MetricAction, clean shutdown is not possible. " + ma.getId(), e2);
							cleanShutdown = false;
						}
					}
					numChecked++;
				}
			}
			
			//Step 4, flush all data through the MetricActions.
			LOG.info("Flushing all in-memory Records through MetricActions.");
			
			/**
			 * The following code block calls run and flushRelatedSelection for each MetricAction until 
			 * the sum of record counters from all MetricActions returns the same after 2 consecutive 
			 * iterations through the loop.  This ensures all records are pushed out of the lowest 
			 * level of MetricAction, which are those MetricActions that have input queues with no
			 * producers.  flushLastRecords is called for those lowest level MetricActions, then the
			 * MetricActions are deleted and the loop is repeated until all MetricActions have been
			 * disabled/deleted.
			 */
			int consecutiveIterationsWithNoChanges=0;
			int maxIterationsWithNoChanges=3;
			while(metricActionsIdMap.size() != 0 && consecutiveIterationsWithNoChanges<maxIterationsWithNoChanges){
				LOG.debug("Beginning flush attempt with " + metricActionsIdMap.size() + " MetricActions left to disable/delete");
				long counterSum = 0, newCounterSum = 0;
				int consecutiveIterationsWithSameSum=0;
				int maxIterationsWithSameSum=2;
				while(consecutiveIterationsWithSameSum < maxIterationsWithSameSum){
					counterSum = newCounterSum;
					newCounterSum = 0;
					for(MetricAction ma : metricActionsIdMap.values()){
						metricActionScheduler.unschedule(ma);
						ma.run();
						ma.flushRelatedSelection();
						long[] counters = ma.getCounters();
						for(int x=0; x<5; x++) {
							newCounterSum += counters[x];
						}
					}
					if(counterSum == newCounterSum){
						consecutiveIterationsWithSameSum++;
					} else {
						consecutiveIterationsWithSameSum=0;
					}
					LOG.info("Finished running " + metricActionsIdMap.size() + " metric actions with ncs: " + newCounterSum + " and ocs: " + counterSum + 
							" ciwss: " + consecutiveIterationsWithSameSum + " miwss: " + maxIterationsWithSameSum);
					
				}
				Iterator<Map.Entry<String, MetricAction>> maI = metricActionsIdMap.entrySet().iterator();
				LinkedList<MetricAction> maToDelete = new LinkedList<MetricAction>();
				boolean disabledAMetric=false;
				while(maI.hasNext()){
					Map.Entry<String, MetricAction> e = maI.next();
					MetricAction ma = e.getValue();
					if ( recordQueueManager.getQueueProducers(ma.getConfig().getInputQueue()).length == 0 && 
						 ( !ma.getConfig().getRelatedSelectorEnabled() ||
						   recordQueueManager.getQueueProducers(ma.getConfig().getRelatedInputQueueName()).length == 0
					     )
					   )
					{
						maToDelete.add(ma);
					} else {
						LOG.info("Not deleting " + ma.getId() + 
								" inQ: " + ma.getConfig().getInputQueue() +
								" inQP: " + recordQueueManager.getQueueProducers(ma.getConfig().getInputQueue()).length + 
								" riQ: " + ma.getConfig().getRelatedInputQueueName() + 
								" riQP: " + ((ma.getConfig().getRelatedInputQueueName()==null) ? "null" : recordQueueManager.getQueueProducers(ma.getConfig().getRelatedInputQueueName()).length));
					}
				}
				for(MetricAction ma : maToDelete){
					ma.flushLastRecords();
					try {
						disableMetricAction(ma);
						deleteMetric(ma.getId());
						disabledAMetric=true;
						LOG.debug("Disabled and deleted MetricAction with no input queue producers: " + ma.getId());
					} catch (Exception e) {
						LOG.warn("Failed to disable/delete MetricAction " + ma.getId(), e);
					}
				}
				if(disabledAMetric)
					consecutiveIterationsWithNoChanges=0;
				else
					consecutiveIterationsWithNoChanges++;
			}
			
			//Step 5, shutdown any persistance managers, which will force them to flush persisted records.
			LOG.info("Shutting down persistance managers.");
			if(maprdbSyncPersistanceManager != null){
				LOG.info("Requesting MapRDBSyncPersistanceManager to shutdown");
				try {
					maprdbSyncPersistanceManager.requestShutdown();
				} catch (Exception e){}
			}
			if(localFileSystemPersistanceManager != null){
				LOG.info("Requesting LocalFileSystemPersistanceManager to shutdown");
				try {
					localFileSystemPersistanceManager.requestShutdown();
				} catch (Exception e){}
			}
			
			boolean threadsStillRunning=true;
			while(threadsStillRunning){
				threadsStillRunning = false;
				if(maprdbSyncPersistanceManager != null && maprdbSyncPersistanceManager.isAlive()){
					LOG.info("Waiting for MapRDBSyncPersistanceManager thread to exit");
					threadsStillRunning = true;
				}
				if(localFileSystemPersistanceManager != null && localFileSystemPersistanceManager.isAlive()){
					LOG.info("Waiting for LocalFileSystemPersistanceManager thread to exit");
					threadsStillRunning = true;
				}
				if(threadsStillRunning){
					try {
						Thread.sleep(1000);
					} catch (Exception e){}
				}
			}
			LOG.info("MapRDBSyncPersistors: " + ((maprdbSyncPersistanceManager == null) ? "null" : maprdbSyncPersistanceManager.getPersistors().size()) + 
					" LocalFileSystemPersistors: " + ((localFileSystemPersistanceManager == null) ? "null" : localFileSystemPersistanceManager.getPersistors().size()) + 
					" MetricConfigs: " + metricConfigMap.size() + 
					" MetricActions: " + metricActionsIdMap.size() + 
					" enabledActions: " + metricActionsEnableMap.size() + 
					" futureActions: " + metricActionsIdFuturesMap.size() + 
					" ProcRecordProducer: " + ((procRecordProducer == null) ? "null" : procRecordProducer.isAlive()) + 
					" MfsGutsRecordProducer: " + ((mfsGutsRecordProducer == null) ? "null" : mfsGutsRecordProducer.isAlive()) + 
					" MfsGutsStdoutRecordProducer: " + ((mfsGutsRecordProducer == null) ? "null" : mfsGutsRecordProducer.mfsGutsStdoutRecordProducerIsAlive()));
			for(MetricAction ma : metricActionsIdMap.values())
				LOG.info("MetricAction: " + ma.getId());
			LOG.info("Shutdown complete");
			
			System.exit(0);
		}
	}
}
