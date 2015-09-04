package com.mapr.distiller.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.mapr.distiller.cli.base.CLICommand;
import com.mapr.distiller.cli.base.CLICommand.ExecutionTypeEnum;
import com.mapr.distiller.cli.base.CLIProcessingException;
import com.mapr.distiller.cli.base.CLIUsageOnlyCommand;
import com.mapr.distiller.cli.base.CommandOutput;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy;
import com.mapr.distiller.cli.base.ProcessedInput;
import com.mapr.distiller.cli.base.common.ListCommand;
import com.mapr.distiller.cli.base.inputparams.BaseInputParameter;
import com.mapr.distiller.client.CoordinatorClient;

public class QueueCommands extends ListCommand {

  private static final Logger LOG = LoggerFactory
      .getLogger(QueueCommands.class);
  
  private static final CoordinatorClient client = new CoordinatorClient("http://localhost","8080");

  public static final String VALUES_PARAM_NAME = "values";

  public static final CLICommand listCmd = new CLICommand("list",
      "list of queues available", QueueCommands.class, ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().build(), null)
      .setShortUsage("list");

  public static final CLICommand queueCmds = new CLICommand("queue", "",
      CLIUsageOnlyCommand.class, ExecutionTypeEnum.REST,
      new CLICommand[] { listCmd }).setShortUsage("queue [list]");

  public QueueCommands(ProcessedInput input, CLICommand cliCommand) {
    super(input, cliCommand);
    LOG.info("Starting queue commands");
  }

  @Override
  public CommandOutput executeRealCommand() throws CLIProcessingException {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandOutput();
    output.setOutput(out);

    String cmd = cliCommand.getCommandName();
    if (cmd.equalsIgnoreCase("list")) {
      LOG.info("Get list of queues available");
      getQueuesList(out);
      
    }
    return output;
  }
  
  private void getQueuesList(OutputHierarchy out) {
    String response = client.getRecordQueues();
    LOG.info("Queues list");
    System.out.println(response);
  }
}
