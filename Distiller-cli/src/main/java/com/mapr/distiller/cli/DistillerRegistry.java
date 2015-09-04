package com.mapr.distiller.cli;

import com.mapr.distiller.cli.base.CLICommandRegistry;
import com.mapr.distiller.cli.base.CLIRegistryInterface;

public class DistillerRegistry implements CLIRegistryInterface {

  private static DistillerRegistry registry = new DistillerRegistry();

  private DistillerRegistry() {

  }

  public static DistillerRegistry getInstance() {
    return registry;
  }

  @Override
  public void register() {
    registerQueueStatus();
  }

  public void registerQueueStatus() {
    CLICommandRegistry.getInstance().register(QueueCommands.queueCmds);
  }

}
