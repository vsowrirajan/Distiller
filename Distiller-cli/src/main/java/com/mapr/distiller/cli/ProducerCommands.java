package com.mapr.distiller.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.mapr.distiller.cli.base.CLICommand;
import com.mapr.distiller.cli.base.CLIProcessingException;
import com.mapr.distiller.cli.base.CLIUsageOnlyCommand;
import com.mapr.distiller.cli.base.CommandLineOutput;
import com.mapr.distiller.cli.base.CommandOutput;
import com.mapr.distiller.cli.base.ProcessedInput;
import com.mapr.distiller.cli.base.TextCommandOutput;
import com.mapr.distiller.cli.base.CLICommand.ExecutionTypeEnum;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputError;
import com.mapr.distiller.cli.base.common.ListCommand;
import com.mapr.distiller.cli.base.inputparams.BaseInputParameter;
import com.mapr.distiller.client.CoordinatorClient;

public class ProducerCommands extends ListCommand {

  private static final Logger LOG = LoggerFactory
      .getLogger(ProducerCommands.class);

  private static CoordinatorClient client = null;
  private BufferedReader br;

  public static final String VALUES_PARAM_NAME = "values";
  public static final String ID_PARAM_NAME = "id";
  public static final String COUNT_PARAM_NAME = "count";
  public static final String STATUS_TYPE_PARAM_NAME = "type";

  public static final CLICommand listCmd = new CLICommand("list",
      "List of Metric Actions available", ProducerCommands.class,
      ExecutionTypeEnum.REST,
      new ImmutableMap.Builder<String, BaseInputParameter>().build(), null)
      .setShortUsage("list");

  public static final CLICommand producerCmds = new CLICommand("producers", "",
      CLIUsageOnlyCommand.class, ExecutionTypeEnum.REST,
      new CLICommand[] { listCmd }).setShortUsage("producers [ list ]");

  public ProducerCommands(ProcessedInput input, CLICommand cliCommand) {
    super(input, cliCommand);
    LOG.info("Starting Producer commands");
    
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
      return getRecordProducerStatus();
    }

    else {
      out.addError(new OutputError(0, "Not a valid command " + cmd));
    }

    return output;
  }

  private CommandOutput getRecordProducerStatus() {
    OutputHierarchy out = new OutputHierarchy();
    CommandOutput output = new CommandLineOutput();

    try {
      String response = client.getRecordProducerStatus();
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
