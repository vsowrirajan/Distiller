package com.mapr.distiller.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.mapr.distiller.cli.base.CLICommand;
import com.mapr.distiller.cli.base.CLICommand.ExecutionTypeEnum;
import com.mapr.distiller.cli.base.CLIBaseClass;
import com.mapr.distiller.cli.base.CLIProcessingException;
import com.mapr.distiller.cli.base.CLIUsageOnlyCommand;
import com.mapr.distiller.cli.base.CommandLineOutput;
import com.mapr.distiller.cli.base.CommandOutput;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputError;
import com.mapr.distiller.cli.base.ProcessedInput;
import com.mapr.distiller.cli.base.TextCommandOutput;
import com.mapr.distiller.cli.base.common.ListCommand;
import com.mapr.distiller.cli.base.inputparams.BaseInputParameter;
import com.mapr.distiller.cli.base.inputparams.TextInputParameter;
import com.mapr.distiller.client.CoordinatorClient;

public class QueueCommands extends ListCommand {

  private static final Logger LOG = LoggerFactory
      .getLogger(QueueCommands.class);

  private static CoordinatorClient client = null;
  private BufferedReader br;

  public static final String VALUES_PARAM_NAME = "values";
  public static final String QUEUE_PARAM_NAME = "queue";
  public static final String COUNT_PARAM_NAME = "count";

  public static final CLICommand listCmd = new CLICommand("list",
      "List of queues available", QueueCommands.class, ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().build(), null)
      .setShortUsage("list");

  public static final CLICommand getCmd = new CLICommand("get",
      "Get queue Status", QueueCommands.class, ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().put(
          QUEUE_PARAM_NAME,
          new TextInputParameter(QUEUE_PARAM_NAME, "queuenames",
              CLIBaseClass.REQUIRED, null)).build(), null)
      .setShortUsage("get [ -queue queuename ]");

  public static final CLICommand getRecordsCmd = new CLICommand("getRecords",
      "Get queue Records", QueueCommands.class, ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>()
          .put(
              QUEUE_PARAM_NAME,
              new TextInputParameter(QUEUE_PARAM_NAME, "queuenames",
                  CLIBaseClass.REQUIRED, null))
          .put(
              COUNT_PARAM_NAME,
              new TextInputParameter(COUNT_PARAM_NAME, "recordcount",
                  CLIBaseClass.NOT_REQUIRED, null)).build(), null)
      .setShortUsage("getRecords [ -queue queuename -count recordCount(100) ]");

  public static final CLICommand queueCmds = new CLICommand("queues", "",
      CLIUsageOnlyCommand.class, ExecutionTypeEnum.REST, new CLICommand[] {
          listCmd, getCmd, getRecordsCmd })
      .setShortUsage("queues [ list | get | getRecords ]");

  public QueueCommands(ProcessedInput input, CLICommand cliCommand) {
    super(input, cliCommand);
    LOG.info("Starting Queue commands");

    br = new BufferedReader(new InputStreamReader(this.getClass()
        .getResourceAsStream("/serverinfo.conf")));
    try {
      String line = br.readLine();
      String[] opts = line.split(":");
      client = new CoordinatorClient("http://" + opts[0], opts[1]);
      LOG.info("Connecting to server at http://" + opts[0] + ":" + opts[1]);
    } catch (IOException e) {
      LOG.error("serverinfo file is not found in classpath " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public CommandOutput executeRealCommand() throws CLIProcessingException {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandOutput();
    output.setOutput(out);

    String cmd = cliCommand.getCommandName();
    if (cmd.equalsIgnoreCase("list")) {
      LOG.info("Get list of queues available");
      return getQueuesList();
    }

    else if (cmd.equalsIgnoreCase("get")) {
      String queueName = getParamTextValue(QUEUE_PARAM_NAME, 0);
      LOG.info("Queues Info for " + queueName);
      return getQueueInfo(queueName);
    }

    else if (cmd.equalsIgnoreCase("getRecords")) {
      String queueName = getParamTextValue(QUEUE_PARAM_NAME, 0);
      int count = 100;

      if (input.getParameterByName(COUNT_PARAM_NAME) != null) {
        String countStr = getParamTextValue(COUNT_PARAM_NAME, 0).trim();
        count = Integer.parseInt(countStr);
      }
      return getQueueRecords(queueName, count);
    }

    else {
      out.addError(new OutputError(0, "Not a valid command " + cmd));
    }

    return output;
  }

  private CommandOutput getQueuesList() {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.getRecordQueues();
      if (response != null) {
        output = new TextCommandOutput(response);
      }
    }

    catch (Exception e) {
      out.addError(new OutputError(404, e.getMessage()));
      output.setOutput(out);
      return output;
    }
    return output;
  }

  private CommandOutput getQueueInfo(String queueName) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.getQueueStatus(queueName);
      if (response != null) {
        output = new TextCommandOutput(response);
      }
    }

    catch (Exception e) {
      out.addError(new OutputError(404, e.getMessage()));
      output.setOutput(out);
      return output;
    }
    return output;
  }

  private CommandOutput getQueueRecords(String queueName, int count) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.getRecords(queueName, count);
      if (response != null) {
        output = new TextCommandOutput(response);
      }
    }

    catch (Exception e) {
      out.addError(new OutputError(404, e.getMessage()));
      output.setOutput(out);
      return output;
    }
    return output;
  }
}
