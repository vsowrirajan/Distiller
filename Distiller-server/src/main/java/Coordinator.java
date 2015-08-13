import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Iterator;

import com.mapr.distiller.server.metricactions.MetricAction;
import com.mapr.distiller.server.producers.raw.ProcRecordProducer;
import com.mapr.distiller.server.producers.raw.MfsGutsRecordProducer;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.utils.Constants;
import com.mapr.distiller.server.utils.MetricConfig;
import com.mapr.distiller.server.utils.MetricConfig.MetricConfigBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

public class Coordinator {
	private boolean DEBUG_ENABLED=true;

	private List<MetricConfig> metricConfigs;
	private static Map<String, MetricAction> metricActionsIdMap;

	private ExecutorService executor = Executors.newCachedThreadPool();

	private static Map<String, Boolean> metricActionsStateMap;
	private static Map<String, Future<MetricAction>> metricActionsIdFuturesMap;

	private static ProcRecordProducer procRecordProducer;
	private static MfsGutsRecordProducer mfsGutsRecordProducer;
	private static RecordQueueManager recordQueueManager;

	public Coordinator() {
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": initialized");
	}

	public void start() {
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": started");
		metricActionsStateMap = new HashMap<String, Boolean>();
		metricActionsIdFuturesMap = new HashMap<String, Future<MetricAction>>();

		for (MetricAction metricAction : metricActionsIdMap.values()) {
			String id = metricAction.getId();
			metricActionsStateMap.put(id, true);
			Future<?> future = executor.submit(metricAction);
			metricActionsIdFuturesMap.put(id, ((Future<MetricAction>) future));
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": registered metric action " + id);
		}
	}

	public void stop() {
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": shutting down");
		executor.shutdown();
	}

	public void createMetricConfigs(Config config) throws Exception {
		metricConfigs = new ArrayList<MetricConfig>();

		ConfigObject configObject = config.getObject("distiller");
		Set<String> metrics = configObject.keySet();

		// Create metricActions
		LinkedList<String> metricNames = new LinkedList<String>();
		for (String metric : metrics) {
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Creating a MetricAction for element " + metric);
			MetricConfig configMetric;
			MetricConfigBuilder configMetricBuilder;
			Config distillerConfig = configObject.toConfig();
			ConfigObject distillerConfigObject = distillerConfig
					.getObject(metric);
			Config metricConfig = distillerConfigObject.toConfig();

			String id = null;
			String inputQueue = null;
			String outputQueue = null;
			int outputQueueRecordCapacity = -1;
			int outputQueueTimeCapacity = -1;
			int outputQueueMaxProducers = -1;
			int periodicity = -1;
			String recordType = null;
			String procRecordProducerMetricName = null;
			boolean rawProducerMetricsEnabled = false;
			String metricDescription = null;
			String rawRecordProducerName = null;
			String selector = null;
			String processor = null;
			String method = null;
			String thresholdKey = null;
			String thresholdValue = null;
			boolean metricActionStatusRecordsEnabled=false;
			long metricActionStatusRecordFrequency=-1;
			long timeSelectorMinDelta = -1;
			long timeSelectorMaxDelta = -1;
			String selectorQualifierKey = null;
			long cumulativeSelectorFlushTime = -1;
			String outputQueueType = null;
			
			try {
				id = metricConfig.getString(Constants.METRIC_NAME);
			} catch (Exception e) {}
			try {
				inputQueue = metricConfig.getString(Constants.INPUT_QUEUE_NAME);
			} catch (Exception e) {}
			try {
				outputQueue = metricConfig.getString(Constants.OUTPUT_QUEUE_NAME);
			} catch (Exception e) {}
			try {
				outputQueueRecordCapacity = metricConfig.getInt(Constants.OUTPUT_QUEUE_CAPACITY_RECORDS);
			} catch (Exception e) {}
			try {
				outputQueueTimeCapacity = metricConfig.getInt(Constants.OUTPUT_QUEUE_CAPACITY_SECONDS);
			} catch (Exception e) {}
			try {
				outputQueueMaxProducers = metricConfig.getInt(Constants.OUTPUT_QUEUE_MAX_PRODUCERS);
			} catch (Exception e) {}
			try {
				periodicity = metricConfig.getInt(Constants.PERIODICITY_MS);
			} catch (Exception e) {}
			try {
				recordType = metricConfig.getString(Constants.RECORD_TYPE);
			} catch (Exception e) {}
			try {
				procRecordProducerMetricName = metricConfig.getString(Constants.PROC_RECORD_PRODUCER_METRIC_NAME);
			} catch (Exception e) {}
			try {
				rawProducerMetricsEnabled = metricConfig.getBoolean(Constants.RAW_PRODUCER_METRICS_ENABLED);
			} catch (Exception e) {}
			try {
				metricDescription = metricConfig.getString(Constants.METRIC_DESCRIPTION);
			} catch (Exception e) {}
			try {
				rawRecordProducerName = metricConfig.getString(Constants.RAW_RECORD_PRODUCER_NAME);
			} catch (Exception e) {}
			try {
				selector = metricConfig.getString(Constants.INPUT_RECORD_SELECTOR);
			} catch (Exception e) {}
			try {
				processor = metricConfig.getString(Constants.INPUT_RECORD_PROCESSOR_NAME);
			} catch (Exception e) {}
			try {
				method = metricConfig.getString(Constants.INPUT_RECORD_PROCESSOR_METHOD);
			} catch (Exception e) {}
			try {
				metricActionStatusRecordsEnabled = metricConfig.getBoolean(Constants.METRIC_ACTION_STATUS_RECORDS_ENABLED);
			} catch (Exception e) {}
			try {
				metricActionStatusRecordFrequency = metricConfig.getLong(Constants.METRIC_ACTION_STATUS_RECORD_FREQUENCY);
			} catch (Exception e) {}
			try {
				thresholdKey = metricConfig.getString(Constants.THRESHOLD_KEY);
			} catch (Exception e) {}
			try {
				thresholdValue = metricConfig.getString(Constants.THRESHOLD_VALUE);
			} catch (Exception e) {}
			try {
				timeSelectorMaxDelta = metricConfig.getLong(Constants.TIME_SELECTOR_MAX_DELTA);
			} catch (Exception e) {}
			try {
				timeSelectorMinDelta = metricConfig.getLong(Constants.TIME_SELECTOR_MIN_DELTA);
			} catch (Exception e) {}
			try {
				selectorQualifierKey = metricConfig.getString(Constants.SELECTOR_QUALIFIER_KEY);
			} catch (Exception e) {}
			try {
				cumulativeSelectorFlushTime = metricConfig.getLong(Constants.SELECTOR_CUMULATIVE_FLUSH_TIME);
			} catch (Exception e) {}
			try {
				outputQueueType = metricConfig.getString(Constants.OUTPUT_QUEUE_TYPE);
			} catch (Exception e) {}
			
			if (outputQueueType!=null && outputQueueType.equals(Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE) && 
				(selectorQualifierKey==null || selectorQualifierKey.equals("")))
				throw new Exception("Use of " + Constants.OUTPUT_QUEUE_TYPE + "=" + Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE + 
									" requires a value for " + Constants.SELECTOR_QUALIFIER_KEY);
			
			if (selector!=null && selector.equals(Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR) && 
				( selectorQualifierKey==null || selectorQualifierKey.equals("") ) )
				throw new Exception(Constants.SELECTOR_QUALIFIER_KEY + " must be specified when " + Constants.INPUT_RECORD_SELECTOR + "=" + Constants.SEQUENTIAL_WITH_QUALIFIER_SELECTOR);
			
			if(id != null && !id.equals("")){
				if(metricNames.contains(id))
					throw new Exception("Parameter " + Constants.METRIC_NAME + " must be unique across all metrics. Found duplicate value: \"" + id + "\"");
				metricNames.add(id);
			}
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Building config for element " + metric);
			
			try {
				configMetricBuilder = new MetricConfigBuilder(id, inputQueue, outputQueue, outputQueueRecordCapacity, 
						outputQueueTimeCapacity, outputQueueMaxProducers, periodicity, recordType, procRecordProducerMetricName, 
						rawProducerMetricsEnabled, metricDescription, rawRecordProducerName, selector, processor, method,
						metricActionStatusRecordsEnabled, metricActionStatusRecordFrequency, thresholdKey, thresholdValue,
						timeSelectorMaxDelta, timeSelectorMinDelta, selectorQualifierKey, cumulativeSelectorFlushTime,
						outputQueueType);
			} catch (Exception e) {
				throw new Exception("Coordinator-" + System.identityHashCode(this) + ": Failed to construct MetricConfigBuilder for config element " + metric, e);
			}

			configMetric = configMetricBuilder.build();
			this.metricConfigs.add(configMetric);
		}
	}

	public void startMetric(String id) {
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": request to start metric: " + id);
		if (metricActionsIdMap.containsKey(id)) {
			MetricAction metricAction = metricActionsIdMap.get(id);
			metricAction.resume();
			metricActionsIdMap.put(id, metricAction);
			metricActionsStateMap.put(id, true);
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": started metric: " + id);
		}

		else {
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": could not start unknown metric: " + id);
		}
	}

	public void stopMetric(String id) throws InterruptedException {
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": request to stop metric: " + id);
		if (metricActionsIdMap.containsKey(id)) {
			MetricAction metricAction = metricActionsIdMap.get(id);
			metricAction.suspend();
			metricActionsIdMap.put(id, metricAction);
			metricActionsStateMap.put(id, false);
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": stopped metric: " + id);
		}

		else {
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": could not stop unknown metric: " + id);
		}
	}

	public void killMetric(String id) {
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": request to kill metric: " + id);
		if (metricActionsIdMap.containsKey(id)
				&& metricActionsIdFuturesMap.containsKey(id)) {
			MetricAction metricAction = metricActionsIdMap.get(id);
			metricAction.kill();
			Future<MetricAction> future = metricActionsIdFuturesMap.get(id);
			metricActionsStateMap.put(id, false);
			future.cancel(true);
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": killed metric: " + id);
		}

		else {
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": could not kill unknown metric: " + id);
		}
	}

	public void createMetricActions() throws Exception{
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": request to create metric actions");
		
		List<MetricConfig> metricConfigs = getMetricConfigs();
		
		int initializationSuccesses=0;
		int consecutiveIterationsWithNoSuccesses=0;
		int maxIterationsWithNoSuccesses=2;
		boolean initializationComplete=false;
		while(	!initializationComplete && 
				consecutiveIterationsWithNoSuccesses<maxIterationsWithNoSuccesses )
		{
			int ss = initializationSuccesses;
			for (MetricConfig config : metricConfigs) {
				if (!config.isInitialized() &&
					( config.getInputQueue()==null || 
					  config.getInputQueue().equals("") ||
					  recordQueueManager.queueExists(config.getInputQueue())
					)
				){
					int startingSuccesses=initializationSuccesses;
					if(DEBUG_ENABLED)
						System.err.println("Coordinator-" + System.identityHashCode(this) + ": processing config: " + config.getId() + 
								" with record type " + config.getRecordType());
					MetricAction metricAction;
					switch (config.getRecordType()) {

					case Constants.MFS_GUTS_RECORD_PRODUCER_RECORD:
						if(!enableMfsGutsRecordProducer(config)){
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable MfsGutsRecordProducer");
						} else {
							if(DEBUG_ENABLED)
								System.err.println("Coordinator-" + System.identityHashCode(this) + ": Enabled MfsGutsRecordProducer");
							initializationSuccesses++;
						}
						break;

					case Constants.PROC_RECORD_PRODUCER_RECORD:
						if(!enableProcRecordProducerMetric(config)){
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable ProcRecordProducer metric " + config.getProcRecordProducerMetricName());
						} else {
							if(DEBUG_ENABLED)
								System.err.println("Coordinator-" + System.identityHashCode(this) + ": Enabled ProcRecordProducer metric " + config.getProcRecordProducerMetricName());
							initializationSuccesses++;
						}
						break;

					case Constants.SYSTEM_MEMORY_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.SYSTEM_CPU_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.DISK_STAT_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.NETWORK_INTERFACE_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.THREAD_RESOURCE_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.PROCESS_RESOURCE_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.SLIM_THREAD_RESOURCE_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.SLIM_PROCESS_RESOURCE_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.TCP_CONNECTION_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;

					case Constants.MFS_GUTS_RECORD:
						metricAction = null;
						try {
							metricAction = MetricAction.getInstance(config, recordQueueManager);
						} catch (Exception e) {
							System.err.println("Failed to enable metric: " + config.toString());
							e.printStackTrace();
						}
						if(metricAction != null){
							metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;
						
					case Constants.RAW_RECORD_PRODUCER_STAT_RECORD:
						if(!enableRawRecordProducerStats(config)){
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable stats for raw record producer " + config.getRawRecordProducerName());
						} else {
							if(DEBUG_ENABLED)
								System.err.println("Coordinator-" + System.identityHashCode(this) + ": Enabled stats for raw record producer " + config.getRawRecordProducerName());
							initializationSuccesses++;
						}
						break;

					default:
						throw new IllegalArgumentException("Unknown record type \"" + config.getRecordType() + "\" specified for metric \"" + config.getId() + "\"");
					}
					if(startingSuccesses < initializationSuccesses){
						System.err.println("Marking initialized for " + config.getId());
						config.setInitialized(true);
					}
				} else if(!config.isInitialized() && DEBUG_ENABLED){
					System.err.println("Coordinator-" + System.identityHashCode(this) + ": Skipping metric \"" + config.getId() + "\" this iteration as input queue " + config.getInputQueue() + " is not yet available.");
				}
			}
			if(ss == initializationSuccesses)
				consecutiveIterationsWithNoSuccesses++;
			else
				consecutiveIterationsWithNoSuccesses=0;
			if(initializationSuccesses==metricConfigs.size())
				initializationComplete=true;
		} 
		if(!initializationComplete)
			throw new Exception("Failed to initialize " + (metricConfigs.size() - initializationSuccesses) + " metric(s), initialized:" + initializationSuccesses + " size:" + metricConfigs.size());
	}
	
	private boolean enableMfsGutsRecordProducer(MetricConfig config){
		boolean createdQueue=false;
		if(mfsGutsRecordProducer != null) {
			if(!mfsGutsRecordProducer.isAlive())
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": MfsGutsRecordProducer was initialized but is no longer running.");
			else if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": MfsGutsRecordProducer is already running.");
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
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable metric because output RecordQueue \"" + 
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
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable metric because producer \"" + 
						Constants.MFS_GUTS_RECORD_PRODUCER_NAME + "\" could not be registered with queue \"" + config.getOutputQueue() + "\"");
				if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
					System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
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
	
	private boolean enableProcRecordProducerMetric(MetricConfig config){
		boolean createdQueue=false, registeredProducer=false;
		if(!ProcRecordProducer.isValidMetricName(config.getProcRecordProducerMetricName())){
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Can not enable ProcRecordProducer metric due to invalid metric name: \"" + config.getProcRecordProducerMetricName() + "\"");
			return false;
		}
		if(procRecordProducer == null || !procRecordProducer.isAlive()){
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable metric because ProdRecordProducer is not alive");
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
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable metric because output RecordQueue \"" + 
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
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable metric because producer \"" + 
					Constants.PROC_RECORD_PRODUCER_NAME + "\" could not be registered with queue \"" + config.getOutputQueue() + "\"");
			if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
			return false;
		}
		try {
			procRecordProducer.enableMetric(config.getProcRecordProducerMetricName(), recordQueueManager.getQueue(config.getOutputQueue()), config.getPeriodicity());
		} catch (Exception e) {
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable ProcRecordProducer metric " + config.getProcRecordProducerMetricName() + " with exception:");
			e.printStackTrace();
			if(registeredProducer && !recordQueueManager.unregisterProducer(config.getOutputQueue(), Constants.PROC_RECORD_PRODUCER_NAME))
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to unregister producer \"" + Constants.PROC_RECORD_PRODUCER_NAME + 
						"\" from queue \"" + config.getOutputQueue() + "\" while cleaning up");
			if(createdQueue && !recordQueueManager.deleteQueue(config.getOutputQueue()))
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to delete queue \"" + config.getOutputQueue() + "\" while cleaning up");
			return false;
		}
		
		if(config.getRawProducerMetricsEnabled()){
			config.setRawRecordProducerName(Constants.PROC_RECORD_PRODUCER_NAME);
			enableRawRecordProducerStats(config);
		}
		
		return true;
	}
	
	private boolean enableRawRecordProducerStats(MetricConfig config){
		boolean createdQueue = false, registeredProducer = false;
		if (!config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME) &&
			!config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)){
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Unknown raw.record.producer.name: " + config.getRawRecordProducerName());
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
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to retrieve output queue for raw producer metrics: \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\"");
		} 
		else if(	!(	recordQueueManager.checkForQueueProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName()) ||
						(	recordQueueManager.registerProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName()) &&
								(registeredProducer=true)
						)
				 	 )
			   )
		{
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to register \"" + config.getRawRecordProducerName() + 
						"\" as a producer with queue \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\"");
			if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to delete queue \"" + Constants.RAW_PRODUCER_STATS_QUEUE_NAME + 
					"\" while cleaning up.");
		}
		else if (config.getRawRecordProducerName().equals(Constants.MFS_GUTS_RECORD_PRODUCER_NAME)) {
			if (!mfsGutsRecordProducer.producerMetricsEnabled() && 
					!mfsGutsRecordProducer.enableProducerMetrics(recordQueueManager.getQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
				   )
				{
					System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable raw producer metrics for ProcRecordProducer");
					if (registeredProducer && 
						!recordQueueManager.unregisterProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName())
					   )
						System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to unregister producer \"" + 
								config.getRawRecordProducerName() + "\" while cleaning up.");
					if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
						System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to delete queue \"" + 
								Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\" while cleaning up.");
				}
		}
		else if (config.getRawRecordProducerName().equals(Constants.PROC_RECORD_PRODUCER_NAME)) {
			if (!procRecordProducer.producerMetricsEnabled() && 
				!procRecordProducer.enableProducerMetrics(recordQueueManager.getQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
			   )
			{
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable raw producer metrics for ProcRecordProducer");
				if (registeredProducer && 
					!recordQueueManager.unregisterProducer(Constants.RAW_PRODUCER_STATS_QUEUE_NAME, config.getRawRecordProducerName())
				   )
					System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to unregister producer \"" + 
							config.getRawRecordProducerName() + "\" while cleaning up.");
				if(createdQueue && !recordQueueManager.deleteQueue(Constants.RAW_PRODUCER_STATS_QUEUE_NAME))
					System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to delete queue \"" + 
							Constants.RAW_PRODUCER_STATS_QUEUE_NAME + "\" while cleaning up.");
			}
		}
		return true;
	}

	public List<MetricConfig> getMetricConfigs() {
		return metricConfigs;
	}

	public Map<String, MetricAction> getMetricActionsIdMap() {
		return metricActionsIdMap;
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
		
		Coordinator coordinator = new Coordinator();
		recordQueueManager = new RecordQueueManager();
		procRecordProducer = new ProcRecordProducer(Constants.PROC_RECORD_PRODUCER_NAME);
		procRecordProducer.start();
		metricActionsIdMap = new HashMap<String, MetricAction>();
		
		
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
		
		coordinator.start();
		
		/**
		 * This next block of code just prints some status stuff out to stderr.
		 * We should remove this once the RPC/client interface is ready.
		 */
		long statusInterval = 10000l;
		long lastStatus = System.currentTimeMillis();
		while(!shouldExit){
			try{
				Thread.sleep(1000);
			} catch (Exception e){}
			if(System.currentTimeMillis() >= lastStatus + statusInterval){
				lastStatus = System.currentTimeMillis();
				System.err.println("Main: Printing status at " + lastStatus);
				synchronized(recordQueueManager){
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
					System.err.println("\tMetricAction details:");
					Iterator<Map.Entry<String, Future<MetricAction>>> fI = metricActionsIdFuturesMap.entrySet().iterator();
					while(fI.hasNext()){
						Map.Entry<String, Future<MetricAction>> e = fI.next();
						System.err.println("\t\t" + e.getKey() + " done:" + e.getValue().isDone());
					}

							
					System.err.println("\tRecordQueue details:");
					for(int x=0; x<queues.length; x++){
						System.err.println("\t\tQueue " + queues[x].getQueueName() + " contains " + queues[x].queueSize() + " records with time capacity " + 
								queues[x].getQueueTimeCapacity() + " and time usage " + (System.currentTimeMillis() - queues[x].getOldestRecordTimestamp()) + 
								"ms and record capacity " + queues[x].getQueueRecordCapacity() + " (" + 
								(((double)queues[x].queueSize())/((double)queues[x].getQueueRecordCapacity())*100d) + "%)" + 
								" cl:" + queues[x].listConsumers().length +
								" pl:" + queues[x].listProducers().length);
						if(queues[x].getQueueName().equals("DiffTcpConnections-60s")){
							System.err.println(queues[x].printNewestRecords(null, 5));
						}
						//if (queues[x].getQueueName().equals(Constants.RAW_PRODUCER_STATS_QUEUE_NAME) || 
						//	queues[x].getQueueName().equals(Constants.METRIC_ACTION_STATS_QUEUE_NAME)){
						//	System.err.println(queues[x].printNewestRecords(null, 5));
						//}
					}
				}
			}
		}
		
		System.err.println("Main: Shutting down.");
		//Do shutdown stuff here...
		coordinator.stop();
	}
	
}
