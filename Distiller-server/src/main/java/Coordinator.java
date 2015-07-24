import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mapr.distiller.server.metricactions.MetricAction;
import com.mapr.distiller.server.metricactions.SystemCpuMetricAction;
import com.mapr.distiller.server.utils.MetricConfig;
import com.mapr.distiller.server.utils.MetricConfig.MetricConfigBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

public class Coordinator {

	private List<MetricConfig> metricConfigs;
	private List<MetricAction> metricActions;

	private ExecutorService executor = Executors.newFixedThreadPool(5);

	private Map<String, Boolean> metricActionsStateMap;

	public void start() {
		metricActionsStateMap = new HashMap<String, Boolean>();

		for (MetricAction metricAction : metricActions) {
			metricActionsStateMap.put(metricAction.getId(), true);
			executor.submit(metricAction);
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

	public void createMetricActions() {
		List<MetricConfig> metricConfigs = getMetricConfigs();
		this.metricActions = new ArrayList<MetricAction>();

		for (MetricConfig config : metricConfigs) {
			MetricAction metricAction;
			switch (config.getRecordType()) {
			case "SystemCpuRecord":
				metricAction = SystemCpuMetricAction.getInstance(config);
				this.metricActions.add(metricAction);
				break;

			case "SystemMemoryRecord":
				// Just a filler - Have to change this after implementing
				// SystemMemoryAction
				metricAction = SystemCpuMetricAction.getInstance(config);
				this.metricActions.add(metricAction);
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

	public List<MetricAction> getMetricActions() {
		return metricActions;
	}

	public static void main(String[] args) throws IOException {
		String configLocation = args[0];
		Config config = ConfigFactory.parseFile(new File(configLocation));
		Coordinator coordinator = new Coordinator();
		coordinator.createMetricConfigs(config);
		List<MetricConfig> metricConfigs = coordinator.getMetricConfigs();

		for (MetricConfig metricConfig : metricConfigs) {
			System.out.println(metricConfig.getInputQueue() + " - "
					+ metricConfig.getOutputQueue() + " - "
					+ metricConfig.getRecordType() + " - "
					+ metricConfig.getAggregationType() + " - "
					+ metricConfig.getAggregationMap() + " - "
					+ metricConfig.isShouldPersist());
		}

		coordinator.createMetricActions();
		System.out.println("Metric actions size "
				+ coordinator.getMetricActions().size());
		coordinator.start();

	}

}
