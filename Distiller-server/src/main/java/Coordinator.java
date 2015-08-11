import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
	private Map<String, MetricAction> metricActionsIdMap;

	private ExecutorService executor = Executors.newFixedThreadPool(5);

	private Map<String, Boolean> metricActionsStateMap;
	private Map<String, Future<MetricAction>> metricActionsIdFuturesMap;

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
		for (String metric : metrics) {
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Creating a MetricAction for " + metric);
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
			boolean isAggregation = false;
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
			
			try {
				id = metricConfig.getString("metric.name");
			} catch (Exception e) {}
			try {
				inputQueue = metricConfig.getString("input.queue.name");
			} catch (Exception e) {}
			try {
				outputQueue = metricConfig.getString("output.queue.name");
			} catch (Exception e) {}
			try {
				outputQueueRecordCapacity = metricConfig.getInt("output.queue.capacity.records");
			} catch (Exception e) {}
			try {
				outputQueueTimeCapacity = metricConfig.getInt("output.queue.capacity.seconds");
			} catch (Exception e) {}
			try {
				outputQueueMaxProducers = metricConfig.getInt("output.queue.max.producers");
			} catch (Exception e) {}
			try {
				periodicity = metricConfig.getInt("periodicity.ms");
			} catch (Exception e) {}
			try {
				procRecordProducerMetricName = metricConfig.getString("proc.record.producer.metric.name");
			} catch (Exception e) {}
			try {
				isAggregation = metricConfig.hasPath("aggregation");
			} catch (Exception e) {}
			try {
				rawProducerMetricsEnabled = metricConfig.getBoolean("raw.producer.metrics.enabled");
			} catch (Exception e) {}
			try {
				metricDescription = metricConfig.getString("metric.description");
			} catch (Exception e) {}
			try {
				rawRecordProducerName = metricConfig.getString("raw.record.producer.name");
			} catch (Exception e) {}
			try {
				selector = metricConfig.getString("input.record.selector");
			} catch (Exception e) {}
			try {
				processor = metricConfig.getString("input.record.processor.name");
			} catch (Exception e) {}
			try {
				method = metricConfig.getString("input.record.processor.method");
			} catch (Exception e) {}
			try {
				metricActionStatusRecordsEnabled = metricConfig.getBoolean("metric.action.status.records.enabled");
			} catch (Exception e) {}
			try {
				metricActionStatusRecordFrequency = metricConfig.getLong("metric.action.status.record.frequency");
			} catch (Exception e) {}
			try {
				thresholdKey = metricConfig.getString("threshold.key");
			} catch (Exception e) {}
			try {
				thresholdValue = metricConfig.getString("threshold.value");
			} catch (Exception e) {}
			
			if(DEBUG_ENABLED)
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": Building config for " + metric + " with parameters: " + 
						" metric.name:" + ((id == null) ? "null" : id) + 
						" input.queue.name:" + ((inputQueue == null) ? "null" : inputQueue) + 
						" output.queue.name:" + ((outputQueue == null) ? "null" : outputQueue) + 
						" output.queue.capacity.records:" + outputQueueRecordCapacity +
						" output.queue.capacity.seconds:" + outputQueueTimeCapacity + 
						" output.queue.max.producer:" + outputQueueMaxProducers +
						" periodicity:" + periodicity + 
						" proc.record.producer.metric.name:" + ((procRecordProducerMetricName == null) ? "null" : procRecordProducerMetricName) + 
						" raw.producer.metrics.enabled:" + rawProducerMetricsEnabled + 
						" raw.record.producer.name:" + ((rawRecordProducerName==null) ? "null" : rawRecordProducerName) + 
						" input.record.selector:" + ((selector==null) ? "null" : selector) + 
						" input.record.processor.name:" + ((processor==null) ? "null" : processor) + 
						" input.record.processor.method:" + ((method==null) ? "null" : method) + 
						" metric.action.status.records.enabled:" + metricActionStatusRecordsEnabled + 
						" metric.action.status.record.frequency:" + metricActionStatusRecordFrequency + 
						" threshold.key:" + ((thresholdKey==null) ? "null" : thresholdKey) + 
						" threshold.value:" + ((thresholdValue==null) ? "null" : thresholdValue)
						);
			
			
			try {
				configMetricBuilder = new MetricConfigBuilder(id, inputQueue, outputQueue, outputQueueRecordCapacity, 
						outputQueueTimeCapacity, outputQueueMaxProducers, periodicity, recordType, procRecordProducerMetricName, 
						rawProducerMetricsEnabled, metricDescription, rawRecordProducerName, selector, processor, method,
						metricActionStatusRecordsEnabled, metricActionStatusRecordFrequency, thresholdKey, thresholdValue);
			} catch (Exception e) {
				throw new Exception("Coordinator-" + System.identityHashCode(this) + ": Failed to construct MetricConfigBuilder", e);
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

	public void createMetricActions() {
		if(DEBUG_ENABLED)
			System.err.println("Coordinator-" + System.identityHashCode(this) + ": request to create metric actions");
		
		List<MetricConfig> metricConfigs = getMetricConfigs();
		this.metricActionsIdMap = new HashMap<String, MetricAction>();
		
		int initializationSuccesses=0;
		int initializationIterations=0;
		boolean initializationComplete=false;
		while(!initializationComplete && initializationIterations<metricConfigs.size()){
			initializationIterations++;
			for (MetricConfig config : metricConfigs) {
				if(!config.isInitialized()){
					System.err.println("Attempting initialization for config " + config.toString());
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
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": Enabled MfsGutsRecordProducer");
							initializationSuccesses++;
						}
						break;

					case Constants.PROC_RECORD_PRODUCER_RECORD:
						if(!enableProcRecordProducerMetric(config)){
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable ProcRecordProducer metric " + config.getProcRecordProducerMetricName());
						} else {
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
							this.metricActionsIdMap.put(metricAction.getId(), metricAction);
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
							this.metricActionsIdMap.put(metricAction.getId(), metricAction);
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
							this.metricActionsIdMap.put(metricAction.getId(), metricAction);
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
							this.metricActionsIdMap.put(metricAction.getId(), metricAction);
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
							this.metricActionsIdMap.put(metricAction.getId(), metricAction);
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
							this.metricActionsIdMap.put(metricAction.getId(), metricAction);
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
							this.metricActionsIdMap.put(metricAction.getId(), metricAction);
							initializationSuccesses++;
						}
						break;
						
					case Constants.RAW_RECORD_PRODUCER_STAT_RECORD:
						if(!enableRawRecordProducerStats(config)){
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": Failed to enable stats for raw record producer " + config.getRawRecordProducerName());
						} else {
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": Enabled stats for raw record producer " + config.getRawRecordProducerName());
							initializationSuccesses++;
						}
						break;

					default:
						if(DEBUG_ENABLED)
							System.err.println("Coordinator-" + System.identityHashCode(this) + ": invalid record type " + 
									config.getRecordType() + " specified in config " + config.getId());
						throw new IllegalArgumentException(
								"Not a supported recordType - "
										+ config.getRecordType());
					}
					if(startingSuccesses < initializationSuccesses){
						System.err.println("Marking intialized for config " + config.toString());
						config.setInitialized(true);
					}
				}
			}
			if(initializationSuccesses==metricConfigs.size()){
				System.err.println("Completed initialization of " + metricConfigs.size() + " metrics");
				initializationComplete=true;
			}
		}
	}
	
	private boolean enableMfsGutsRecordProducer(MetricConfig config){
		boolean createdQueue=false, registeredProducer=false;
		if(mfsGutsRecordProducer != null) {
			if(!mfsGutsRecordProducer.isAlive()){
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": MfsGutsRecordProducer was initialized but is no longer running.");
				return false;
			} else {
				System.err.println("Coordinator-" + System.identityHashCode(this) + ": MfsGutsRecordProducer is already running.");
				return false;
			}
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
						recordQueueManager.createQueue(config.getOutputQueue(), config.getOutputQueueRecordCapacity(), config.getOutputQueueTimeCapacity(), 1) &&
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
					(
						recordQueueManager.registerProducer(config.getOutputQueue(), Constants.MFS_GUTS_RECORD_PRODUCER_NAME) &&
						(registeredProducer=true)
					)
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
					recordQueueManager.createQueue(config.getOutputQueue(), config.getOutputQueueRecordCapacity(), config.getOutputQueueTimeCapacity(), 1) &&
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
													Constants.RAW_PRODUCER_STATS_QUEUE_TIME_CAPACITY, 0) && 
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
		String configLocation = "/path/to/default/config";
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
		
		recordQueueManager = new RecordQueueManager();
		System.err.println("Main: Created RecordQueueManager-" + System.identityHashCode(recordQueueManager));
		
		procRecordProducer = new ProcRecordProducer(Constants.PROC_RECORD_PRODUCER_NAME);
		System.err.println("Main: Created ProcRecordProducer-" + System.identityHashCode(procRecordProducer));
		
		procRecordProducer.start();
		System.err.println("Main: Started ProcRecordProducer-" + System.identityHashCode(procRecordProducer));
		
		Coordinator coordinator = new Coordinator();
		System.err.println("Main: Created Coordinator-" + System.identityHashCode(coordinator));
		
		Config config = ConfigFactory.parseFile(new File(configLocation));
		if (config == null) {
			System.err.println("Main: Failed to parse config file from " + configLocation);
			System.exit(1);
		}
		System.err.println("Main: Parsed config file into config-" + System.identityHashCode(config));
		
		try {
			coordinator.createMetricConfigs(config);
		} catch (Exception e) {
			System.err.println("Main: Failed to create metric configs due to exception:");
			e.printStackTrace();
			System.exit(1);
		}
		System.err.println("Main: Created metric configs for Coordinator-" + System.identityHashCode(coordinator));

		coordinator.createMetricActions();
		System.err.println("Main: Created metric actions for Coordinator-" + System.identityHashCode(coordinator));
		
		coordinator.start();
		System.err.println("Main: Started Coordinator-" + System.identityHashCode(coordinator));

		System.err.println("Main: Sleeping until shutdown.");
		
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
					System.err.println("\tRecordQueue details:");
					for(int x=0; x<queues.length; x++){
						System.err.println("\t\tQueue " + queues[x].getQueueName() + " contains " + queues[x].queueSize() + " records with time capacity " + 
								queues[x].getQueueTimeCapacity() + " and time usage " + (System.currentTimeMillis() - queues[x].getOldestRecordTimestamp()) + 
								"ms and record capacity " + queues[x].getQueueRecordCapacity() + " (" + 
								(((double)queues[x].queueSize())/((double)queues[x].getQueueRecordCapacity())*100d) + "%)");
						if (queues[x].getQueueName().equals(Constants.RAW_PRODUCER_STATS_QUEUE_NAME) || 
							queues[x].getQueueName().equals(Constants.METRIC_ACTION_STATS_QUEUE_NAME)){
							System.err.println(queues[x].printNewestRecords(null, 5));
						}
					}
				}
			}
		}
		
		System.err.println("Main: Shutting down.");
		//Do shutdown stuff here...
		coordinator.stop();
	}
	
}
