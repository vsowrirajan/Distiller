import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.mapr.distiller.server.metricactions.MetricAction;
import com.mapr.distiller.server.producers.raw.ProcRecordProducer;
import com.mapr.distiller.server.producers.raw.MfsGutsRecordProducer;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.utils.MetricConfig;
import com.mapr.distiller.server.utils.MetricConfig.MetricConfigBuilder;
import com.mapr.distiller.server.scheduler.MetricActionScheduler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

public class Coordinator {
	private static Config testBlock;
	private static Object coordinatorLock;
	private static boolean DEBUG_ENABLED=true;

	private static Map<String, MetricConfig> metricConfigMap;
	private static Map<String, MetricAction> metricActionsIdMap;
	private static Map<String, Boolean> metricActionsEnableMap;
	private static ExecutorService executor = Executors.newFixedThreadPool(5);

	private static Map<String, Future<MetricAction>> metricActionsIdFuturesMap;

	private static ProcRecordProducer procRecordProducer;
	private static MfsGutsRecordProducer mfsGutsRecordProducer;
	private static RecordQueueManager recordQueueManager;
	private static MetricActionScheduler metricActionScheduler;

	//Constructor
	public Coordinator() {
		//Nothing is done here because the work is really done in the main() function as this is the primary class for distiller-server
		//That's not great but will do for now.
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": initialized");
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
				if(DEBUG_ENABLED)
					System.err.println("Coordinator-" + System.identityHashCode(this) + ": Creating a MetricConfig for element " + metric);
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
				if(metricConfig.getId().equals("CumulativeSystemMemory-1sI"))
					testBlock = configBlock;
			}
		}
	}
	//Creates a single MetricConfig object from the block of configuration passed as argument
	public static MetricConfig createMetricConfig(Config configBlock) throws Exception{
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
			if(isThresholdableProcessorMethod(method))
				throw new Exception("A value must be provided for " + Constants.THRESHOLD_KEY + 
									" when " + Constants.INPUT_RECORD_PROCESSOR_METHOD +
									"=" + method, e);
		}
		try {
			thresholdValue = configBlock.getString(Constants.THRESHOLD_VALUE);
			if(thresholdKey.equals("")) throw new Exception();
		} catch (Exception e) {
			if(isThresholdableProcessorMethod(method))
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
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": request to create metric actions");
		
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
							if(DEBUG_ENABLED)
								System.err.println("Coordinator-" + System.identityHashCode(this) + ": MetricAction created for " + newAction.getId());
						} else if(isRawRecordType(config.getRecordType()) || config.getRecordType().equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
							config.setMetricActionCreated(true);
							System.err.println("Marking success for " + config.getId());
							initializationSuccesses++;
							if(DEBUG_ENABLED)
								System.err.println("Coordinator-" + System.identityHashCode(this) + ": Raw metric enabled for " + config.getRecordType() + " " + config.getProcRecordProducerMetricName());
						}
					}
					if(newAction!=null){
						if(config.getMetricEnabled()){
							try {
								enableMetricAction(newAction);
								if(DEBUG_ENABLED)
									System.err.println("Coordinator-" + System.identityHashCode(this) + ": Enabled MetricAction " + newAction.getId());
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
						System.err.println("Coordinator-" + System.identityHashCode(this) + 
								": Failed to create MetricAction for MetricConfig " + config.getId() + 
								" inputQueue:" + config.getInputQueue() + 
								" inputQExists:" + recordQueueManager.queueExists(config.getInputQueue()) + 
								( (config.getRelatedSelectorEnabled()) ? (" relatedQueue:" + config.getRelatedInputQueueName() + 
																		  " relatedQExists:" + recordQueueManager.queueExists(config.getRelatedInputQueueName())
																		 ) 
																	     : 
																		 ""
								  )
								);
				}
				throw new Exception("Failed to initialize " + (metricConfigMap.size() - initializationSuccesses) + " metric(s), initialized:" + initializationSuccesses + " size:" + metricConfigMap.size());
			}
		}
	}
	//This method builds a MetricAction from a MetricConfig, e.g. a schedulable object (MetricAction) from a description of what should be done (MetricConfig)
	private static MetricAction createMetricAction(MetricConfig config) throws Exception {
		MetricAction metricAction = null;
		boolean initialized=false;
		
		if(config.isInitialized())
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
				if(DEBUG_ENABLED)
					System.err.println("Coordinator: Enabled MfsGutsRecordProducer");
				initialized=true;
			}
			break;

		case Constants.PROC_RECORD_PRODUCER_RECORD:
			if(!enableProcRecordProducerMetric(config)){
				throw new Exception("Coordinator: Failed to enable ProcRecordProducer metric " + config.getProcRecordProducerMetricName());
			} else {
				if(DEBUG_ENABLED)
					System.err.println("Coordinator: Enabled ProcRecordProducer metric " + config.getProcRecordProducerMetricName());
				initialized=true;
			}
			break;

		case Constants.SYSTEM_MEMORY_RECORD:
			metricAction = null;
			try {
				metricAction = MetricAction.getInstance(config, recordQueueManager, metricActionScheduler);
			} catch (Exception e) {
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
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
				System.err.println("Failed to enable metric: " + config.toString());
				e.printStackTrace();
			}
			if(metricAction != null){
				metricActionsIdMap.put(metricAction.getId(), metricAction);
				initialized=true;
			}
			break;
			
		case Constants.RAW_RECORD_PRODUCER_STAT_RECORD:
			if(!enableRawRecordProducerStats(config)){
				throw new Exception("Coordinator: Failed to enable stats for raw record producer " + config.getRawRecordProducerName());
			} else {
				if(DEBUG_ENABLED)
					System.err.println("Coordinator: Enabled stats for raw record producer " + config.getRawRecordProducerName());
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
	public static void createMetric(Config configBlock) throws Exception{
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

	private static void enableMetric(String metricName) throws Exception{
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
	private static void enableMetricAction(MetricAction metricAction) throws Exception {
		synchronized(coordinatorLock){
			metricAction.enableMetric();
			metricActionsEnableMap.put(metricAction.getId(), new Boolean(true));
			metricActionScheduler.schedule(metricAction);
			System.err.println("Scheduled " + metricAction.getId() + " with schedule " + metricAction.printSchedule());
		}
	}
	//Call this method to enable MfsGutsRecordProducer
	private static boolean enableMfsGutsRecordProducer(MetricConfig config){
		boolean createdQueue=false;
		if(mfsGutsRecordProducer != null) {
			if(!mfsGutsRecordProducer.isAlive())
				System.err.println("Coordinator: MfsGutsRecordProducer was initialized but is no longer running.  Will attempt to recreate it.");
			else if(DEBUG_ENABLED){
				System.err.println("Coordinator: MfsGutsRecordProducer is already running.");
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
				System.err.println("Coordinator: Failed to enable metric because output RecordQueue \"" + 
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
				System.err.println("Coordinator: Failed to enable metric because producer \"" + 
						Constants.MFS_GUTS_RECORD_PRODUCER_NAME + "\" could not be registered with queue \"" + config.getOutputQueue() + "\"");
				if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
					System.err.println("Coordinator: Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
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
	private static boolean enableProcRecordProducerMetric(MetricConfig config){
		boolean createdQueue=false, registeredProducer=false;
		synchronized(coordinatorLock){
			if(!ProcRecordProducer.isValidMetricName(config.getProcRecordProducerMetricName())){
				System.err.println("Coordinator: Can not enable ProcRecordProducer metric due to invalid metric name: \"" + config.getProcRecordProducerMetricName() + "\"");
				return false;
			}
			if(procRecordProducer == null || !procRecordProducer.isAlive()){
				System.err.println("Coordinator: Failed to enable metric because ProdRecordProducer is not alive");
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
				System.err.println("Coordinator: Failed to enable metric because output RecordQueue \"" + 
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
				System.err.println("Coordinator: Failed to enable metric because producer \"" + 
						Constants.PROC_RECORD_PRODUCER_NAME + "\" could not be registered with queue \"" + config.getOutputQueue() + "\"");
				if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
					System.err.println("Coordinator: Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
				return false;
			}
			try {
				procRecordProducer.enableMetric(config.getProcRecordProducerMetricName(), recordQueueManager.getQueue(config.getOutputQueue()), config.getPeriodicity());
			} catch (Exception e) {
				System.err.println("Coordinator: Failed to enable ProcRecordProducer metric " + config.getProcRecordProducerMetricName() + " with exception:");
				e.printStackTrace();
				if(registeredProducer && !recordQueueManager.unregisterProducer(config.getOutputQueue(), Constants.PROC_RECORD_PRODUCER_NAME))
					System.err.println("Coordinator: Failed to unregister producer \"" + Constants.PROC_RECORD_PRODUCER_NAME + 
							"\" from queue \"" + config.getOutputQueue() + "\" while cleaning up");
				if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
					System.err.println("Coordinator: Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
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
	private static boolean enableRawRecordProducerStats(MetricConfig config){
		boolean createdQueue = false, registeredProducer = false;
		synchronized(coordinatorLock){
			if (!config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME) &&
					!config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)){
					System.err.println("Coordinator: Unknown raw.record.producer.name: " + config.getRawRecordProducerName());
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
					System.err.println("Coordinator: Failed to retrieve output queue for raw producer metrics: \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\"");
				} 
				else if(	!(	recordQueueManager.checkForQueueProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName()) ||
								(	recordQueueManager.registerProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName()) &&
										(registeredProducer=true)
								)
						 	 )
					   )
				{
					System.err.println("Coordinator: Failed to register \"" + config.getRawRecordProducerName() + 
								"\" as a producer with queue \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\"");
					if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
						System.err.println("Coordinator: Failed to delete queue \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + 
							"\" while cleaning up.");
				}
				else if (config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)) {
					if(mfsGutsRecordProducer==null){
						System.err.println("Failed to enable MfsGutsRecordProducer stats because it is not yet initialized");
						return false;
					}
					if (!mfsGutsRecordProducer.producerMetricsEnabled() && 
							!mfsGutsRecordProducer.enableProducerMetrics(recordQueueManager.getQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
						   )
						{
							System.err.println("Coordinator: Failed to enable raw producer metrics for ProcRecordProducer");
							if (registeredProducer && 
								!recordQueueManager.unregisterProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName())
							   )
								System.err.println("Coordinator: Failed to unregister producer \"" + 
										config.getRawRecordProducerName() + "\" while cleaning up.");
							if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
								System.err.println("Coordinator: Failed to delete queue \"" + 
										Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\" while cleaning up.");
						}
				}
				else if (config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME)) {
					if (!procRecordProducer.producerMetricsEnabled() && 
						!procRecordProducer.enableProducerMetrics(recordQueueManager.getQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
					   )
					{
						System.err.println("Coordinator: Failed to enable raw producer metrics for ProcRecordProducer");
						if (registeredProducer && 
							!recordQueueManager.unregisterProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName())
						   )
							System.err.println("Coordinator: Failed to unregister producer \"" + 
									config.getRawRecordProducerName() + "\" while cleaning up.");
						if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
							System.err.println("Coordinator: Failed to delete queue \"" + 
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
	private static void disableMetric(String metricName) throws Exception{
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
	private static void disableMetricAction(MetricAction metricAction) throws Exception {
		synchronized(coordinatorLock){
			if(!metricActionsEnableMap.containsKey(metricAction.getId()))
				throw new Exception("Current state for MetricAction " + metricAction.getId() + " not found");
			
			if (metricActionsIdFuturesMap.containsKey(metricAction.getId())) {
				Future<?> future = metricActionsIdFuturesMap.get(metricAction.getId());
				if(!future.isDone()){
					System.err.println(metricAction.getId() + " is still running, cancelling it");
					future.cancel(true);
					if(!future.isDone() && !future.isCancelled())
						throw new Exception("Bad state for " + metricAction.getId() + " isDone:" + future.isDone() + " isCancelled:" + future.isCancelled());
				}
				metricActionsIdFuturesMap.remove(metricAction.getId());
			}
			
			if(metricActionScheduler.contains(metricAction)){
				System.err.println("Removing " + metricAction.getId() + " from schedule");
				metricActionScheduler.unschedule(metricAction);
			}
			metricActionsEnableMap.put(metricAction.getId(), false);
			metricAction.disableMetric();
		}
	}
	//Call this method to disable MfsGutsRecordProducer
	private static void disableMfsGutsRecordProducer(MetricConfig config) throws Exception{
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
	private static void disableProcRecordProducerMetric(MetricConfig config) throws Exception {
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
	private static void disableRawRecordProducerStats(MetricConfig config) throws Exception{
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
	private static void deleteMetric(String metricName) throws Exception {
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
		boolean shouldExit = false;
		String configLocation = "/opt/mapr/conf/distiller.conf";
		if(args.length == 0){
			System.err.println("Main: Using default configuration file location: " + configLocation);
		} else {
			configLocation = args[0];
			System.err.println("Main: Using custom configuration file location: " + configLocation);
		}
		File f = new File(configLocation);
		if(!f.exists()){	
			System.err.println("Main: Config file not found: " + configLocation);
			System.exit(1);
		}
		
		coordinatorLock = new Object();
		Coordinator coordinator = new Coordinator();
		recordQueueManager = new RecordQueueManager();
		procRecordProducer = new ProcRecordProducer(Constants.PROC_RECORD_PRODUCER_NAME);
		procRecordProducer.start();
		metricActionsIdMap = new HashMap<String, MetricAction>();
		metricActionsIdFuturesMap = new HashMap<String, Future<MetricAction>>();
		metricActionsEnableMap = new HashMap<String, Boolean>();
		metricConfigMap = new HashMap<String, MetricConfig>();
		
		//Hard coding a fuzzy window size of 20ms
		//This means that a metric may be triggered up to 20ms before it is actually supposed to be gathered per schedule.
		//The reason for this is to avoid requiring extra code/waiting to run in the scheduler when the precision of when a metric needs to be processed is low.
		//E.g. since the records this scheduler will process are already timestamped, we don't need to be worried about the exact time that the processing happens.
		try {
			metricActionScheduler = new MetricActionScheduler(20);	
		} catch (Exception e){
			System.err.println("Failed to setup MetricActionScheduler" + e.toString());
			System.exit(1);
		}
		
		Config config = ConfigFactory.parseFile(new File(configLocation));
		if (config == null) {
			System.err.println("Main: Failed to parse config file from " + configLocation);
			System.exit(1);
		}
		
		try {
			coordinator.createMetricConfigs(config);
		} catch (Exception e) {
			System.err.println("Main: Failed to create metric configs due to exception:");
			e.printStackTrace();
			System.exit(1);
		}
		
		try {
			coordinator.createMetricActions();
		} catch (Exception e){
			System.err.println("Main: Failed to create metric actions due to exception:");
			e.printStackTrace();
			System.exit(1);
		}
		long statusInterval = 3000l;
		long lastStatus = System.currentTimeMillis();
		while(!shouldExit){
            MetricAction nextAction = null;
			try {
				nextAction = metricActionScheduler.getNextScheduledMetricAction(true);
			} catch (Exception e) {
				System.err.println("Main: FATAL: Failed to retrieve net scheduled metric action");
				e.printStackTrace();
				System.exit(1);
			}
			synchronized(coordinatorLock){
				if (metricActionsEnableMap.containsKey(nextAction.getId()) && 
					metricActionsEnableMap.get(nextAction.getId()))
				{
					Future<?> future = metricActionsIdFuturesMap.get(nextAction.getId());
					if(future!=null && !future.isDone()){
						System.err.println("Main: CRITICAL: Metric " + nextAction.getId() + " is scheduled to run now but previous run is not done");
					} else {
						future = executor.submit(nextAction);
						metricActionsIdFuturesMap.put(nextAction.getId(), ((Future<MetricAction>) future));
					}
				} else {
					System.err.println("Main: dropping metric " + nextAction.getId() + " as it was disabled after retrieval from schedule.");
				}
			}
			
		//This is where the while loop should end when there is an RPC interface to query for status
		//}

			if(System.currentTimeMillis() >= lastStatus + statusInterval){
				lastStatus = System.currentTimeMillis();
				System.err.println("Main: Printing status at " + lastStatus);
				synchronized(coordinatorLock){
					RecordQueue[] queues = recordQueueManager.getQueues();
					System.err.println("\tMfsGutsRecordProducer is " + 
							((mfsGutsRecordProducer != null) ? ((mfsGutsRecordProducer.isAlive()) ? "" : "not ") : "not ") + 
							"running and producer metrics are " + 
							((mfsGutsRecordProducer != null && mfsGutsRecordProducer.producerMetricsEnabled()) ? "enabled" : "disabled"));
					System.err.println("\tProcRecordProducer is " + 
							((procRecordProducer != null) ? ((procRecordProducer.isAlive()) ? "" : "not ") : "not ") + 
							"running and producer metrics are " + 
							((procRecordProducer != null && procRecordProducer.producerMetricsEnabled()) ? "enabled" : "disabled"));
					if(procRecordProducer != null){
						System.err.println("\tEnabled ProcRecordProducer metrics:");
						for(String s : procRecordProducer.listEnabledMetrics()){
							System.err.println("\t\t" + s);
						}
					}
					System.err.println("\tRecordQueue details:");
					for(int x=0; x<queues.length; x++){
						System.err.println("\t\tQueue " + queues[x].getQueueName() + " contains " + queues[x].queueSize() + " records with time capacity " + 
								queues[x].getQueueTimeCapacity() + " and time usage " + (System.currentTimeMillis() - queues[x].getOldestRecordTimestamp()) + 
								"ms and record capacity " + queues[x].getQueueRecordCapacity() + " (" + 
								(((double)queues[x].queueSize())/((double)queues[x].getQueueRecordCapacity())*100d) + "%)" + 
								" cl:" + queues[x].listConsumers().length +
								" pl:" + queues[x].listProducers().length);
						//if(queues[x].getQueueName().equals("HighMfsThreadCpu-1s")){
						//	System.err.println(queues[x].printNewestRecords(null, 5));
						//}
						//if (queues[x].getQueueName().equals(Constants.RAW_PRODUCER_STATS_QUEUE_NAME) || 
						//	queues[x].getQueueName().equals(Constants.METRIC_ACTION_STATS_QUEUE_NAME)){
						//	System.err.println(queues[x].printNewestRecords(null, 5));
						//}
					}
					System.err.println("\tMetricAction details:");
					Iterator<Map.Entry<String, MetricAction>> i = metricActionsIdMap.entrySet().iterator();
					while(i.hasNext()){
						Map.Entry<String, MetricAction> e = i.next();
						System.err.println("\t\tID:" + e.getKey() + " enabled:" + e.getValue().getMetricEnabled() + " running:" + ((metricActionsIdFuturesMap.containsKey(e.getValue().getId()) && !metricActionsIdFuturesMap.get(e.getValue().getId()).isDone())) + " inSched:" + metricActionScheduler.contains(e.getValue()) + " sched:" + e.getValue().printSchedule());
					}
				}
			}

		}

		System.err.println("Main: Shutting down.");
		//Do shutdown stuff here...
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
	public static boolean isThresholdableProcessorMethod(String name){
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
	public static boolean selectorRequiresQualifierValue(String name){
		return false;
	}
	public static boolean isValidProcessorMethod(String name){
		if(name==null) return false;
		if (name.equals(Constants.IS_ABOVE) ||
			name.equals(Constants.IS_BELOW) ||
			name.equals(Constants.IS_EQUAL) ||
			name.equals(Constants.IS_NOT_EQUAL) ||
			name.equals(Constants.MERGE_RECORDS)
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
			name.equals(Constants.SLIM_THREAD_RESOURCE_RECORD_PROCESSOR)
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
			name.equals(Constants.TIME_SELECTOR)
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
			name.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)
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
		if(selector==null || method==null) return false;
		
		if (selector.equals(Constants.SEQUENTIAL_SELECTOR)){
			if (method.equals(Constants.IS_ABOVE) ||
				method.equals(Constants.IS_BELOW) ||
				method.equals(Constants.IS_EQUAL) ||
				method.equals(Constants.IS_NOT_EQUAL) ||
				method.equals(Constants.MERGE_RECORDS)
			){
				return true;
			}
		} else if(selector.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR)){
			if(method.equals(Constants.MERGE_RECORDS)){
				return true;
			}
		} else if(selector.equals(Constants.CUMULATIVE_SELECTOR)){
			if(method.equals(Constants.MERGE_RECORDS)){
				return true;
			}
		} else if(selector.equals(Constants.CUMULATIVE_WITH_QUALIFIER_SELECTOR)){
			if(method.equals(Constants.MERGE_RECORDS)){
				return true;
			}
		} else if(selector.equals(Constants.TIME_SELECTOR)){
			if(method.equals(Constants.MERGE_RECORDS)){
				return true;
			}
		}
		return false;
	}
}
