package com.mapr.distiller.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.mapr.distiller.cli.base.CLIBaseClass;
import com.mapr.distiller.cli.base.CLICommand;
import com.mapr.distiller.cli.base.CLIProcessingException;
import com.mapr.distiller.cli.base.CLIUsageOnlyCommand;
import com.mapr.distiller.cli.base.CommandLineOutput;
import com.mapr.distiller.cli.base.TextCommandOutput;
import com.mapr.distiller.cli.base.CLICommand.ExecutionTypeEnum;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputError;
import com.mapr.distiller.cli.base.CommandOutput;
import com.mapr.distiller.cli.base.ProcessedInput;
import com.mapr.distiller.cli.base.common.ListCommand;
import com.mapr.distiller.cli.base.inputparams.BaseInputParameter;
import com.mapr.distiller.cli.base.inputparams.TextInputParameter;
import com.mapr.distiller.client.CoordinatorClient;

public class MetricActionCommands extends ListCommand {

  private static final Logger LOG = LoggerFactory
      .getLogger(MetricActionCommands.class);

  private static CoordinatorClient client = null;
  private BufferedReader br;

  public static final String VALUES_PARAM_NAME = "values";
  public static final String ID_PARAM_NAME = "id";
  public static final String COUNT_PARAM_NAME = "count";
  public static final String STATUS_TYPE_PARAM_NAME = "type";

  public MetricActionCommands(ProcessedInput input, CLICommand cliCommand) {
    super(input, cliCommand);
    
    LOG.info("Starting Metric Action commands");

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

  public static final CLICommand listCmd = new CLICommand("list",
      "List of Metric Actions available", MetricActionCommands.class,
      ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().build(), null)
      .setShortUsage("list");

  public static final CLICommand getCmd = new CLICommand("get",
      "Get Metric Action Status", MetricActionCommands.class,
      ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().put(
          ID_PARAM_NAME,
          new TextInputParameter(ID_PARAM_NAME, "MetricAction names",
              CLIBaseClass.REQUIRED, null)).build(), null)
      .setShortUsage("get [ -id metricActionId ]");

  public static final CLICommand statusCmd = new CLICommand("status",
      "Metric Action Status", MetricActionCommands.class,
      ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>()
          .put(
              STATUS_TYPE_PARAM_NAME,
              new TextInputParameter(STATUS_TYPE_PARAM_NAME, "status types",
                  CLIBaseClass.REQUIRED, null))
          .put(
              ID_PARAM_NAME,
              new TextInputParameter(ID_PARAM_NAME, "MetricAction names",
                  CLIBaseClass.REQUIRED, null)).build(), null)
      .setShortUsage("status [ isRunning | isScheduled ] -id metricActionId");

  public static final CLICommand enableCmd = new CLICommand("enable",
      "Enable a metric action", MetricActionCommands.class,
      ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().put(
          ID_PARAM_NAME,
          new TextInputParameter(ID_PARAM_NAME, "MetricAction names",
              CLIBaseClass.REQUIRED, null)).build(), null)
      .setShortUsage("enable [ -id metricActionId ]");

  public static final CLICommand disableCmd = new CLICommand("disable",
      "Disable a metric action", MetricActionCommands.class,
      ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().put(
          ID_PARAM_NAME,
          new TextInputParameter(ID_PARAM_NAME, "MetricAction names",
              CLIBaseClass.REQUIRED, null)).build(), null)
      .setShortUsage("disable [ -id metricActionId ]");

  public static final CLICommand deleteCmd = new CLICommand("delete",
      "Delete a metric action", MetricActionCommands.class,
      ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().put(
          ID_PARAM_NAME,
          new TextInputParameter(ID_PARAM_NAME, "MetricAction names",
              CLIBaseClass.REQUIRED, null)).build(), null)
      .setShortUsage("delete [ -id metricActionId ]");

  public static final CLICommand metricActionCmds = new CLICommand(
      "metricActions", "", CLIUsageOnlyCommand.class, ExecutionTypeEnum.REST,
      new CLICommand[] { listCmd, getCmd, statusCmd, enableCmd, disableCmd,
          deleteCmd })
      .setShortUsage("metricActions [ list | get | status | enable | disable | delete ]");

  @Override
  public CommandOutput executeRealCommand() throws CLIProcessingException {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandOutput();
    output.setOutput(out);

    String cmd = cliCommand.getCommandName();
    if (cmd.equalsIgnoreCase("list")) {
      LOG.info("Get list of Metric Actions available");
      return getMetricActionList();
    }

    else if (cmd.equalsIgnoreCase("get")) {
      String metricActionName = getParamTextValue(ID_PARAM_NAME, 0);
      LOG.info("Metric Actions Info for " + metricActionName);
      return getMetricAction(metricActionName);
    }

    else if (cmd.equalsIgnoreCase("status")) {
      String type = getParamTextValue(STATUS_TYPE_PARAM_NAME, 0);
      if (type.equalsIgnoreCase("isRunning")) {
        String id = getParamTextValue(ID_PARAM_NAME, 0);
        return isRunning(id);
      }

      else if (type.equalsIgnoreCase("isScheduled")) {
        String id = getParamTextValue(ID_PARAM_NAME, 0);
        return isScheduled(id);
      }

      else {
        out.addError(new OutputError(0, "Not a valid status type " + type));
      }
    }

    else if (cmd.equalsIgnoreCase("enable")) {
      String id = getParamTextValue(ID_PARAM_NAME, 0);
      return enableMetricAction(id);
    }

    else if (cmd.equalsIgnoreCase("disable")) {
      String id = getParamTextValue(ID_PARAM_NAME, 0);
      return disableMetricAction(id);
    }

    else if (cmd.equalsIgnoreCase("delete")) {
      String id = getParamTextValue(ID_PARAM_NAME, 0);
      deleteMetricAction(id);
    }

    else {
      out.addError(new OutputError(0, "Not a valid command " + cmd));
    }

    return output;
  }

  private CommandOutput getMetricActionList() {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.getMetricActions();
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

  private CommandOutput getMetricAction(String metricAction) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.getMetricAction(metricAction);
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

  private CommandOutput isRunning(String metricAction) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.isRunningMetricAction(metricAction) ? metricAction
          + " is running"
          : metricAction + " is not running";
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

  private CommandOutput isScheduled(String metricAction) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.isScheduledMetricAction(metricAction) ? metricAction
          + " is scheduled"
          : metricAction + " is not scheduled";
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

  private CommandOutput enableMetricAction(String metricAction) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.metricEnable(metricAction) ? metricAction
          + " is enabled" : metricAction + " is not enabled";
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

  private CommandOutput disableMetricAction(String metricAction) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.metricDisable(metricAction) ? metricAction
          + " is disabled" : metricAction + " is not disabled";
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

  private CommandOutput deleteMetricAction(String metricAction) {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.metricDelete(metricAction) ? metricAction
          + " is deleted" : metricAction + " is not deleted";
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
