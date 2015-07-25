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
import com.mapr.distiller.server.metricactions.SystemCpuMetricAction;
import com.mapr.distiller.server.utils.MetricConfig;
import com.mapr.distiller.server.utils.MetricConfig.MetricConfigBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

public class Coordinator {

	private List<MetricConfig> metricConfigs;
	private Map<String, MetricAction> metricActionsIdMap;

	private ExecutorService executor = Executors.newFixedThreadPool(5);

	private Map<String, Boolean> metricActionsStateMap;
	private Map<String, Future<MetricAction>> metricActionsIdFuturesMap;

	public void start() {
		metricActionsStateMap = new HashMap<String, Boolean>();
		metricActionsIdFuturesMap = new HashMap<String, Future<MetricAction>>();

		for (MetricAction metricAction : metricActionsIdMap.values()) {
			String id = metricAction.getId();
			metricActionsStateMap.put(id, true);
			Future<?> future = executor.submit(metricAction);
			metricActionsIdFuturesMap.put(id, (Future<MetricAction>) future);
		}
	}

	public void stop() {

	}

	public void createMetricConfigs(Config config) {
		metricConfigs = new ArrayList<MetricConfig>();

		ConfigObject configObject = config.getObject("distiller");
		Set<String> metrics = configObject.keySet();

		// Create metricActions
		for (String metric : metrics) {
			MetricConfig configMetric;
			MetricConfigBuilder configMetricBuilder;
			Config distillerConfig = configObject.toConfig();
			ConfigObject distillerConfigObject = distillerConfig
					.getObject(metric);
			Config metricConfig = distillerConfigObject.toConfig();

			String id = metricConfig.getString("id");
			String inputQueue = metricConfig.getString("inputqueue");
			String outputQueue = metricConfig.getString("outputqueue");
			String recordType = metricConfig.getString("recordtype");

			configMetricBuilder = new MetricConfigBuilder(id, inputQueue,
					outputQueue, recordType);
			boolean isAggregation = metricConfig.hasPath("aggregation");

			if (isAggregation) {
				Map<String, String> aggregationMap = new HashMap<String, String>();
				ConfigObject aggregationConfigObject = metricConfig
						.getObject("aggregation");
				Config aggregationConfig = aggregationConfigObject.toConfig();

				for (String key : aggregationConfigObject.keySet()) {
					aggregationMap.put(key, aggregationConfig.getString(key));
				}

				configMetricBuilder.aggregation(
						aggregationConfig.getString("type"), aggregationMap);
			}

			// configMetricBuilder.persist(false);

			configMetric = configMetricBuilder.build();
			this.metricConfigs.add(configMetric);
		}
	}

	public void startMetric(String id) {
		Future<MetricAction> future = metricActionsIdFuturesMap.get(id);
		MetricAction metricAction = metricActionsIdMap.get(id);
		metricAction.resume();
		metricActionsIdMap.put(id, metricAction);
		// future.cancel(true);
		metricActionsStateMap.put(id, true);
	}

	public void stopMetric(String id) throws InterruptedException {
		Future<MetricAction> future = metricActionsIdFuturesMap.get(id);
		MetricAction metricAction = metricActionsIdMap.get(id);
		metricAction.suspend();
		metricActionsIdMap.put(id, metricAction);
		// future.cancel(true);
		metricActionsStateMap.put(id, false);
	}

	public void createMetricActions() {
		List<MetricConfig> metricConfigs = getMetricConfigs();
		this.metricActionsIdMap = new HashMap<String, MetricAction>();

		for (MetricConfig config : metricConfigs) {
			MetricAction metricAction;
			switch (config.getRecordType()) {
			case "SystemCpuRecord":
				metricAction = SystemCpuMetricAction.getInstance(config);
				this.metricActionsIdMap.put(metricAction.getId(), metricAction);
				break;

			case "SystemMemoryRecord":
				// Just a filler - Have to change this after implementing
				// SystemMemoryAction
				metricAction = SystemCpuMetricAction.getInstance(config);
				this.metricActionsIdMap.put(metricAction.getId(), metricAction);
				break;
			default:
				throw new IllegalArgumentException(
						"Not a supported recordType - "
								+ config.getRecordType());
			}
		}
	}

	public List<MetricConfig> getMetricConfigs() {
		return metricConfigs;
	}

	public Map<String, MetricAction> getMetricActionsIdMap() {
		return metricActionsIdMap;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		String configLocation = args[0];
		Config config = ConfigFactory.parseFile(new File(configLocation));
		Coordinator coordinator = new Coordinator();
		coordinator.createMetricConfigs(config);
		List<MetricConfig> metricConfigs = coordinator.getMetricConfigs();

		for (MetricConfig metricConfig : metricConfigs) {
			System.out.println(metricConfig.getId() + " - "
					+ metricConfig.getInputQueue() + " - "
					+ metricConfig.getOutputQueue() + " - "
					+ metricConfig.getRecordType() + " - "
					+ metricConfig.getAggregationType() + " - "
					+ metricConfig.getAggregationMap() + " - "
					+ metricConfig.isShouldPersist());
		}

		coordinator.createMetricActions();
		System.out.println("Metric actions size "
				+ coordinator.getMetricActionsIdMap().size() + " - "
				+ coordinator.getMetricActionsIdMap());
		coordinator.start();

		//TimeUnit.SECONDS.sleep(3);

		String id = "Metric 1";

		TimeUnit.SECONDS.sleep(3);
		System.out.println("Going to stop metric " + id);
		coordinator.stopMetric(id);
		TimeUnit.SECONDS.sleep(3);
		System.out.println("Going to resume metric " + id);
		coordinator.startMetric(id);
	}
}
