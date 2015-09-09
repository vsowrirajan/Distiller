package com.mapr.distiller.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.cli.base.CLICommandRegistry;
import com.mapr.distiller.cli.base.CLIRegistryInterface;

public class DistillerRegistry implements CLIRegistryInterface {

  private static final Logger LOG = LoggerFactory
      .getLogger(DistillerRegistry.class);

  private static DistillerRegistry registry = new DistillerRegistry();

  private DistillerRegistry() {

  }

  public static DistillerRegistry getInstance() {
    return registry;
  }

  @Override
  public void register() {
    registerQueueStatus();
    registerMetricActionStatus();
    registerProducerStatus();
  }

  public void registerQueueStatus() {
    CLICommandRegistry.getInstance().register(QueueCommands.queueCmds);
  }

  public void registerProducerStatus() {
    CLICommandRegistry.getInstance().register(ProducerCommands.producerCmds);
  }

  public void registerMetricActionStatus() {
    CLICommandRegistry.getInstance().register(
        MetricActionCommands.metricActionCmds);
  }

}
