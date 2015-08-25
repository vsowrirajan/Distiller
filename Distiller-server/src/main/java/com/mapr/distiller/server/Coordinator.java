package com.mapr.distiller.server;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mapr.distiller.server.metricactions.MetricAction;
import com.mapr.distiller.server.producers.raw.ProcRecordProducer;
import com.mapr.distiller.server.producers.raw.MfsGutsRecordProducer;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.utils.MetricConfig;
import com.mapr.distiller.server.utils.MetricConfig.MetricConfigBuilder;
import com.mapr.distiller.server.scheduler.MetricActionScheduler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

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

	// Constructor
	public Coordinator() {
		// Nothing is done here because the work is really done in the main()
		// function as this is the primary class for distiller-server
		// That's not great but will do for now.
		LOG.info("Coordinator- {} : initialized", System.identityHashCode(this));
	}

	public void init(String configLocation) {
		this.configFileLocation = configLocation;
	}

	@Override
	public void run() {
		boolean shouldExit = false;
		long statusInterval = 3000l;
		long lastStatus = System.currentTimeMillis();
		
		File configFileHandle = new File(configFileLocation);
		if (!configFileHandle.exists()) {
			LOG.warn("Main: Config file not found: " + configFileLocation);
			System.exit(1);
		}

		coordinatorLock = new Object();

		this.recordQueueManager = new RecordQueueManager();
		this.procRecordProducer = new ProcRecordProducer(
				Constants.PROC_RECORD_PRODUCER_NAME);
		this.metricActionsIdMap = new TreeMap<String, MetricAction>();
		this.metricActionsIdFuturesMap = new TreeMap<String, Future<MetricAction>>();
		this.metricActionsEnableMap = new TreeMap<String, Boolean>();
		this.metricConfigMap = new TreeMap<String, MetricConfig>();

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

		Config config = ConfigFactory.parseFile(configFileHandle);
		if (config == null) {
			LOG.error("Main: Failed to parse config file from "
					+ configFileLocation);
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
				LOG.error("Main: FATAL: Failed to retrieve net scheduled metric action");
				e.printStackTrace();
				System.exit(1);
			}
			synchronized (coordinatorLock) {
				if (metricActionsEnableMap.containsKey(nextAction.getId())
						&& metricActionsEnableMap.get(nextAction.getId())) {
					Future<?> future = metricActionsIdFuturesMap.get(nextAction
							.getId());
					if (future != null && !future.isDone()) {
						LOG.info("Main: CRITICAL: Metric "
								+ nextAction.getId()
								+ " is scheduled to run now but previous run is not done");
					} else {
						future = executor.submit(nextAction);
						metricActionsIdFuturesMap.put(nextAction.getId(),
								((Future<MetricAction>) future));
					}
				} else {
					LOG.info("Main: dropping metric "
							+ nextAction.getId()
							+ " as it was disabled after retrieval from schedule.");
				}
			}

			if (System.currentTimeMillis() >= lastStatus + statusInterval) {
				lastStatus = System.currentTimeMillis();
				LOG.debug("Main: Printing status " + lastStatus);
				String recordProducerStatus = getRecordProducerStatus();

				LOG.debug("Record Queues");
				String recordQueues = getRecordQueues();

				LOG.debug("Metric Actions");
				String metricActions = getMetricActions();
			}
		}
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
		}
	}

	//Creates a single MetricConfig object from the block of configuration passed as argument
	public MetricConfig createMetricConfig(Config configBlock) throws Exception{
		//Generates a MetricConfig from a Config object while performing error checking.
		//If there is a problem with the content of the configuration then this will throw an exception.
		//If this returns succesfully, it doesn't imply the metric can be gathered, it just implies the values provided in the config are valid.
		//For instance, this function does not ensure that no other metric is already running with this name, nor does it try to resolve such a situation.
		boolean metricActionStatusRecordsEnabled=false;
		boolean metricEnabled=false;
		boolean rawProducerMetricsEnabled = false;
		boolean relatedSelectorEnabled = false;
		int outputQueueMaxProducers = -1;
		int outputQueueRecordCapacity = -1;
		int outputQueueTimeCapacity = -1;
		int relatedOutputQueueMaxProducers = -1;
		int relatedOutputQueueRecordCapacity = -1;
		int relatedOutputQueueTimeCapacity = -1;
		int periodicity = -1;
		long cumulativeSelectorFlushTime = -1;
		long metricActionStatusRecordFrequency=-1;
		long timeSelectorMaxDelta = -1;
		long timeSelectorMinDelta = -1;
		String id = null;
		String inputQueue = null;
		String method = null;
		String metricDescription = null;
		String outputQueue = null;
		String outputQueueType = null;
		String processor = null;
		String procRecordProducerMetricName = null;
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
		
		//Completely optional parameters
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
		} catch (Exception e) {
			if(!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("A value is required for " + Constants.OUTPUT_QUEUE_NAME +
									" when " + Constants.RECORD_TYPE + "=" + recordType);
		}
		try {
			outputQueueRecordCapacity = configBlock.getInt(Constants.OUTPUT_QUEUE_CAPACITY_RECORDS);
			if(outputQueueRecordCapacity < 1) throw new Exception();
		} catch (Exception e) {
			if(!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("A value is required for " + Constants.OUTPUT_QUEUE_CAPACITY_RECORDS +
									" when " + Constants.RECORD_TYPE + "=" + recordType);
		}
		try {
			outputQueueTimeCapacity = configBlock.getInt(Constants.OUTPUT_QUEUE_CAPACITY_SECONDS);
			if(outputQueueTimeCapacity < 1) throw new Exception();
		} catch (Exception e) {
			if(!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("A value is required for " + Constants.OUTPUT_QUEUE_CAPACITY_SECONDS +
									" when " + Constants.RECORD_TYPE + "=" + recordType);
		}
		try {
			outputQueueMaxProducers = configBlock.getInt(Constants.OUTPUT_QUEUE_MAX_PRODUCERS);
			if(outputQueueMaxProducers < 0) throw new Exception();
		} catch (Exception e) {
			if (!isRawRecordType(recordType) &&
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("All metrics require a value >= 0 for " + Constants.OUTPUT_QUEUE_MAX_PRODUCERS);
		}
		
		//Conditionally required parameters:
		try {
			inputQueue = configBlock.getString(Constants.INPUT_QUEUE_NAME);
			if(inputQueue.equals(""))
				throw new Exception("An empty string was provided for " + Constants.INPUT_QUEUE_NAME);
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
			processor = configBlock.getString(Constants.INPUT_RECORD_PROCESSOR_NAME);
			if(!isValidProcessorName(processor)) 
				throw new Exception("Invalid value for " + Constants.INPUT_RECORD_PROCESSOR_NAME +
									": " + processor);
		} catch (Exception e) {
			if (!isRawRecordType(recordType) &&
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
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
				!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
				throw new Exception("Use of non-raw " + Constants.RECORD_TYPE + "=" + recordType + 
									" requires a valid value for " + Constants.INPUT_RECORD_PROCESSOR_METHOD, e);
		}
		if (!isRawRecordType(recordType) &&
			!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD) &&
			!selectorSupportsMethod(selector, method))
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
			if(relatedOutputQueueRecordCapacity < 1) 
				throw new Exception("A value > 0 must be specified for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_RECORDS + 
				" - value: " + relatedOutputQueueRecordCapacity);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A value > 0 is required for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_RECORDS +
									" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedOutputQueueTimeCapacity = configBlock.getInt(Constants.RELATED_OUTPUT_QUEUE_CAPACITY_SECONDS);
			if(relatedOutputQueueTimeCapacity < 1)
				throw new Exception("A value > 0 must be specified for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_SECONDS + 
									" - value: " + relatedOutputQueueTimeCapacity);
		} catch (Exception e) {
			if(relatedSelectorEnabled)
				throw new Exception("A value > 0 is required for " + Constants.RELATED_OUTPUT_QUEUE_CAPACITY_SECONDS +
									" when " + Constants.RELATED_SELECTOR_ENABLED + "=true", e);
		}
		try {
			relatedOutputQueueMaxProducers = configBlock.getInt(Constants.RELATED_OUTPUT_QUEUE_MAX_PRODUCERS);
			if(relatedOutputQueueMaxProducers < 0)
				throw new Exception("A value > 0 must be specified for " + Constants.RELATED_OUTPUT_QUEUE_MAX_PRODUCERS + 
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
					relatedOutputQueueMaxProducers, relatedSelectorEnabled, metricEnabled);
		} catch (Exception e) {
			throw new Exception("Failed to construct MetricConfigBuilder", e);
		}
		return metricConfigBuilder.build();
	}
	//This method is called once during Distiller startup as the MetricConfig objects created from parsing the config file are built into MetricAction objects and scheduled for execution.
	//Creation of metrics after this method is called must be done using the "createMetric" method
	public void createMetricActions() throws Exception{
		LOG.info("Coordinator- {} : request to create metric actions" , System.identityHashCode(this));
		
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
								throw new Exception("Failed to call enableMetricAction for " + newAction.getId());
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
	//This method builds a MetricAction from a MetricConfig, e.g. a schedulable object (MetricAction) from a description of what should be done (MetricConfig)
	public MetricAction createMetricAction(MetricConfig config) throws Exception {
		MetricAction metricAction = null;
		boolean initialized=false;
		
		if(config.isInitialized() && !config.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
			throw new Exception("MetricConfig " + config.getId() + " is already initialized.");
		if(config.getMetricEnabled()){
			if( config.getInputQueue()!=null &&
				!recordQueueManager.queueExists(config.getInputQueue()) ) 
			{
				return null;
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
	//This method constructs, registers and schedules (if enabled) MetricConfig and MetricAction objects based on a provided block of config
	public void createMetric(Config configBlock) throws Exception{
		synchronized(coordinatorLock){
			MetricConfig metricConfig = null;
			MetricAction metricAction = null;
			try {
				metricConfig = createMetricConfig(configBlock);
				if(metricConfigMap.containsKey(metricConfig.getId()))
					throw new Exception("The value for " + Constants.METRIC_NAME + " is not unique in the configuration, " + 
										"duplicate value found: " + metricConfig.getId());
				
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
	//Call this method to enable a metric based on the id parameter of a MetricConfig

	public void enableMetric(String metricName) throws Exception{
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
	//Call this method to enable a single MetricAction (e.g. schedule it for execution)
	public void enableMetricAction(MetricAction metricAction) throws Exception {
		synchronized(coordinatorLock){
			metricAction.enableMetric();
			metricActionsEnableMap.put(metricAction.getId(), new Boolean(true));
			metricActionScheduler.schedule(metricAction);
		}
	}
	//Call this method to enable MfsGutsRecordProducer
	public boolean enableMfsGutsRecordProducer(MetricConfig config){
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
	public boolean enableProcRecordProducerMetric(MetricConfig config){
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
	public boolean enableRawRecordProducerStats(MetricConfig config){
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
	//Call this method to disable a metric based on the id parameter of a MetricConfig
	public void disableMetric(String metricName) throws Exception{
		synchronized(coordinatorLock){	
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
	//Call this method to disable a MetricAction
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
	//Call this method to disable MfsGutsRecordProducer
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
	public void disableProcRecordProducerMetric(MetricConfig config) throws Exception {
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
	//Call this method to disable a stats for a raw record producer (e.g. MfsGutsRecordProducer or ProcRecordProducer)
	public void disableRawRecordProducerStats(MetricConfig config) throws Exception{
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
		}
	}
	
	
	public static void main(String[] args) throws IOException,
			InterruptedException {
		String configLocation = "/opt/mapr/conf/distiller.conf";
		if (args.length == 0) {
			LOG.debug("Main: Using default configuration file location: "
					+ configLocation);
		} else {
			configLocation = args[0];
			LOG.debug("Main: Using custom configuration file location: "
					+ configLocation);
		}

		Coordinator coordinator = new Coordinator();
		coordinator.init(configLocation);
		Thread coordinatorThread = new Thread(coordinator, "Coordinator");
		coordinatorThread.start();

		LOG.info("Main: Shutting down.");
		// Do shutdown stuff here...
	}
	
	//Methods for error checking configuration
	public boolean isRawRecordProducerName(String name){
		if(name==null) return false;
		if (name.equals(Constants.PROC_RECORD_PRODUCER_NAME) ||
			name.equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)
		)
			return true;
		return false;
	}
	
	public boolean isRawRecordType(String name){
		if(name==null) return false;
		if (name.equals(Constants.PROC_RECORD_PRODUCER_RECORD) ||
			name.equals(Constants.MFS_GUTS_RECORD_PRODUCER_RECORD)
		)
			return true;
		return false;
	}
	
	public boolean processorMethodRequiresThresholdKey(String name){
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

	public boolean processorMethodRequiresThresholdValue(String name){
		if(name==null) return false;
		if (name.equals(Constants.IS_ABOVE) ||
			name.equals(Constants.IS_BELOW) ||
			name.equals(Constants.IS_EQUAL) ||
			name.equals(Constants.IS_NOT_EQUAL)
		)
			return true;
		return false;
	}

	public boolean selectorRequiresQualifierKey(String name){
		if(name==null) return false;
		if (name.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) ||
			name.equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR)
		)
			return true;
		return false;
	}

	public boolean methodRequiresQualifierKey(String name){
		if(name==null) return false;
		if(name.equals(Constants.DIFFERENTIAL))
			return true;
		return false;
	}

	public boolean selectorRequiresQualifierValue(String name){
		return false;
	}

	public boolean isValidProcessorMethod(String name){
		if(name==null) return false;
		if (name.equals(Constants.IS_ABOVE) ||
			name.equals(Constants.IS_BELOW) ||
			name.equals(Constants.IS_EQUAL) ||
			name.equals(Constants.IS_NOT_EQUAL) ||
			name.equals(Constants.MERGE_RECORDS) ||
			name.equals(Constants.DIFFERENTIAL)
		)
			return true;
		return false;
	}

	public boolean isValidProcessorName(String name){
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
			name.equals(Constants.DIFFERENTIAL_VALUE_RECORD_PROCESSOR)
		)
			return true;
		return false;
	}

	public boolean isValidRecordSelector(String name){
		if(name==null) return false;
		if (name.equals(Constants.SEQUENTIAL_SELECTOR) ||
			name.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) ||
			name.equals(Constants.CUMULATIVE_SELECTOR) ||
			name.equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR) ||
			name.equals(Constants.TIME_SELECTOR)
		)
			return true;
		return false;
	}

	public boolean isValidRecordType(String name){
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
			name.equals(Constants.DIFFERENTIAL_VALUE_RECORD)
		)
			return true;
		return false;
	}

	public boolean isValidRelatedSelectorName(String name){
		if(name==null) return false;
		if (name.equals(Constants.BASIC_RELATED_RECORD_SELECTOR)
		)
			return true;
		return false;
	}

	public boolean isValidRelatedSelectorMethod(String name){
		if(name==null) return false;
		if (name.equals(Constants.TIME_BASED_WINDOW)
		)
			return true;
		return false;
	}

	public boolean selectorSupportsMethod(String selector, String method){
		if(selector==null || method==null) return false;
		
		if (selector.equals(Constants.SEQUENTIAL_SELECTOR)){
			if (method.equals(Constants.IS_ABOVE) ||
				method.equals(Constants.IS_BELOW) ||
				method.equals(Constants.IS_EQUAL) ||
				method.equals(Constants.IS_NOT_EQUAL) ||
				method.equals(Constants.MERGE_RECORDS) ||
				method.equals(Constants.DIFFERENTIAL)
			){
				return true;
			}
		} else if(selector.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR)){
			if (method.equals(Constants.MERGE_RECORDS) ||
				method.equals(Constants.DIFFERENTIAL) ){
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
	
	@Override
	public String getRecordProducerStatus() {
		StringBuffer status = new StringBuffer();
		LOG.debug("\tMfsGutsRecordProducer is "
				+ ((mfsGutsRecordProducer != null) ? ((mfsGutsRecordProducer
						.isAlive()) ? "" : "not ") : "not ")
				+ "running and producer metrics are "
				+ ((mfsGutsRecordProducer != null && mfsGutsRecordProducer
						.producerMetricsEnabled()) ? "enabled" : "disabled"));
		LOG.debug("\tProcRecordProducer is "
				+ ((procRecordProducer != null) ? ((procRecordProducer
						.isAlive()) ? "" : "not ") : "not ")
				+ "running and producer metrics are "
				+ ((procRecordProducer != null && procRecordProducer
						.producerMetricsEnabled()) ? "enabled" : "disabled"));
		if (procRecordProducer != null) {
			LOG.debug("\tEnabled ProcRecordProducer metrics:");

			for (String s : procRecordProducer.listEnabledMetrics()) {
				status.append("\t\t");
				status.append(s);
				status.append("\n");
				LOG.debug("\t\t" + s);
			}
		}
		return status.toString();
	}

	@Override
	public String getMetricActions() {
		LOG.debug("\tMetricAction details:");
		StringBuffer status = new StringBuffer();

		Iterator<Map.Entry<String, MetricAction>> i = metricActionsIdMap
				.entrySet().iterator();
		while (i.hasNext()) {
			Map.Entry<String, MetricAction> e = i.next();
			status.append("\t\tID:"
					+ e.getKey()
					+ " enabled:"
					+ e.getValue().getMetricEnabled()
					+ " running:"
					+ ((metricActionsIdFuturesMap.containsKey(e.getValue()
							.getId()) && !metricActionsIdFuturesMap.get(
							e.getValue().getId()).isDone())) + " inSched:"
					+ metricActionScheduler.contains(e.getValue()) + " sched:"
					+ e.getValue().printSchedule() + "\n");

			LOG.debug("\t\tID:"
					+ e.getKey()
					+ " enabled:"
					+ e.getValue().getMetricEnabled()
					+ " running:"
					+ ((metricActionsIdFuturesMap.containsKey(e.getValue()
							.getId()) && !metricActionsIdFuturesMap.get(
							e.getValue().getId()).isDone())) + " inSched:"
					+ metricActionScheduler.contains(e.getValue()) + " sched:"
					+ e.getValue().printSchedule());
		}
		return status.toString();
	}


	@Override
	public boolean metricDisable(String metricName) {
		try {
			disableMetric(metricName);
			return true;
		}
		
		catch(Exception e) {
			LOG.error("Metric " + metricName + " cannot be disabled - "
					+ e.getMessage());
		}
		return false;
	}

	@Override
	public boolean metricEnable(String metricName) {
		try {
			enableMetric(metricName);
			return true;
		}
		
		catch(Exception e) {
			LOG.error("Metric " + metricName + " cannot be enabled - "
					+ e.getMessage());
		}
		return false;
	}

	@Override
	public boolean metricDelete(String metricName) {
		try {
			deleteMetric(metricName);
			return true;
		}
		
		catch(Exception e) {
			LOG.error("Metric " + metricName + " cannot be deleted - "
					+ e.getMessage());
		}
		return false;
	}
	
	@Override
	public String getRecordQueues() {
		RecordQueue[] queues = recordQueueManager.getQueues();
		StringBuffer status = new StringBuffer();

		LOG.debug("\tRecordQueue details:");
		for (int x = 0; x < queues.length; x++) {
			status.append("\t\tQueue "
					+ queues[x].getQueueName()
					+ " contains "
					+ queues[x].queueSize()
					+ " records with time capacity "
					+ queues[x].getQueueTimeCapacity()
					+ " and time usage "
					+ (System.currentTimeMillis() - queues[x]
							.getOldestRecordTimestamp())
					+ "ms and record capacity "
					+ queues[x].getQueueRecordCapacity()
					+ " ("
					+ (((double) queues[x].queueSize())
							/ ((double) queues[x].getQueueRecordCapacity()) * 100d)
					+ "%)" + " cl:" + queues[x].listConsumers().length + " pl:"
					+ queues[x].listProducers().length + "\n");

			LOG.debug("\t\tQueue "
					+ queues[x].getQueueName()
					+ " contains "
					+ queues[x].queueSize()
					+ " records with time capacity "
					+ queues[x].getQueueTimeCapacity()
					+ " and time usage "
					+ (System.currentTimeMillis() - queues[x]
							.getOldestRecordTimestamp())
					+ "ms and record capacity "
					+ queues[x].getQueueRecordCapacity()
					+ " ("
					+ (((double) queues[x].queueSize())
							/ ((double) queues[x].getQueueRecordCapacity()) * 100d)
					+ "%)" + " cl:" + queues[x].listConsumers().length + " pl:"
					+ queues[x].listProducers().length);
			// if(queues[x].getQueueName().equals("HighMfsThreadCpu-1s")){
			// LOG.debug(queues[x].printNewestRecords(null, 5));
			// }
			// if
			// (queues[x].getQueueName().equals(Constants.RAW_PRODUCER_STATS_QUEUE_NAME)
			// ||
			// queues[x].getQueueName().equals(Constants.METRIC_ACTION_STATS_QUEUE_NAME)){
			// LOG.debug(queues[x].printNewestRecords(null, 5));
			// }
		}
		return status.toString();
	}
	
	@Override
	public String getQueueStatus(String queueName) {
		RecordQueue queue = recordQueueManager.getQueue(queueName);
		StringBuffer status = new StringBuffer();
		status.append(" Name : "+queue.getQueueName()+"\n");
		status.append(" Type : "+queue.getQueueType()+"\n");
		status.append(" Record Capacity : "+queue.getQueueRecordCapacity()+"\n");
		status.append(" QueueSize : "+queue.queueSize()+"\n");
		status.append(" Producers : "+Arrays.toString(queue.listProducers())+"\n");
		status.append(" Consumers : "+Arrays.toString(queue.listConsumers())+"\n");
		return status.toString();
	}

	@Override
	public String getRecords(String queueName, int count) {
		SubscriptionRecordQueue queue = (SubscriptionRecordQueue) recordQueueManager.getQueue(queueName);
		StringBuffer queueRecords = new StringBuffer();
		
		int getCount = count > 100 ? count : 100;
		
		for(Record record : queue.dumpNewestRecords(getCount)) {
			queueRecords.append(record.toString());
			queueRecords.append("\n");
		}
		
		return queueRecords.toString();
	}

	@Override
	public boolean isScheduledMetricAction(String metricAction) {
		MetricAction action = metricActionsIdMap.get(metricAction);
		return metricActionScheduler.contains(action);
	}

	@Override
	public boolean isRunningMetricAction(String metricAction) {
		MetricAction action = metricActionsIdMap.get(metricAction);
		return metricActionsIdFuturesMap.containsKey(action) && !metricActionsIdFuturesMap.get(action).isDone();
	}
}