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
import com.mapr.distiller.server.metricactions.SystemMemoryMetricAction;
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
			metricActionsIdFuturesMap.put(id, ((Future<MetricAction>) future));
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
		if (metricActionsIdMap.containsKey(id)) {
			MetricAction metricAction = metricActionsIdMap.get(id);
			metricAction.resume();
			metricActionsIdMap.put(id, metricAction);
			metricActionsStateMap.put(id, true);
		}

		else {
			System.out.println("Not a valid id " + id);
		}
	}

	public void stopMetric(String id) throws InterruptedException {
		if (metricActionsIdMap.containsKey(id)) {
			MetricAction metricAction = metricActionsIdMap.get(id);
			metricAction.suspend();
			metricActionsIdMap.put(id, metricAction);
			metricActionsStateMap.put(id, false);
		}

		else {
			System.out.println("Not a valid id " + id);
		}
	}

	public void killMetric(String id) {
		if (metricActionsIdMap.containsKey(id)
				&& metricActionsIdFuturesMap.containsKey(id)) {
			MetricAction metricAction = metricActionsIdMap.get(id);
			metricAction.kill();
			Future<MetricAction> future = metricActionsIdFuturesMap.get(id);
			metricActionsStateMap.put(id, false);
			future.cancel(true);
		}

		else {
			System.out.println("Not a valid id future not available " + id);
		}
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
				metricAction = SystemMemoryMetricAction.getInstance(config);
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
		Coordinator coordinator = new Coordinator();
		String configLocation = "src/main/resources/distiller.conf";
		System.out.println(configLocation);
		Config config = ConfigFactory.parseFile(new File(configLocation));
		if (config == null) {
			System.out.println("Config not loaded properly");
		}
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

		// TimeUnit.SECONDS.sleep(3);

		String id = "Metric 1";

		TimeUnit.SECONDS.sleep(3);
		coordinator.stopMetric(id);
		TimeUnit.SECONDS.sleep(3);
		coordinator.startMetric(id);
		TimeUnit.SECONDS.sleep(3);
		coordinator.stopMetric("Metric 2");
		TimeUnit.SECONDS.sleep(3);
		coordinator.startMetric("Metric 2");
		TimeUnit.SECONDS.sleep(3);
		coordinator.killMetric("Metric 1");
		TimeUnit.SECONDS.sleep(3);
		coordinator.killMetric("Metric 2");
		coordinator.executor.shutdown();
	}
}
