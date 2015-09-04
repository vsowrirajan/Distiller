package com.mapr.distiller.cli.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.cli.base.common.Errno;
import com.mapr.distiller.cli.base.CLICommand;
import com.mapr.distiller.cli.base.CLICommandFactory;
import com.mapr.distiller.cli.base.CLIInterface;
import com.mapr.distiller.cli.base.CLIProcessingException;
import com.mapr.distiller.cli.base.CLIRegistryInterface;
import com.mapr.distiller.cli.base.CommandLineOutput;
import com.mapr.distiller.cli.base.CommandOutput;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputError;
import com.mapr.distiller.cli.base.ProcessedInput;
import com.mapr.distiller.cli.base.TextCommandOutput;
import com.mapr.distiller.cli.base.ProcessedInput.Parameter;

public class CLIMainDriver {

  private static final Logger LOG = LoggerFactory
      .getLogger(CLIMainDriver.class);

  private static final String SPECIAL_JSON_OUTPUT_PARAM = "json";
  private static final String SPECIAL_PROPAGATE_ERROR_PARAM = "propagateerror";
  private static final String CLI_LOG_LEVEL_PARAM = "cli.loglevel";
  private static final String LONG_OUTPUT_PARAM = "long";
  private static final String NOHEADER_OUTPUT_PARAM = "noheader";
  private static final String ADMIN_USAGE_PARAM = "showusage";

  /**
   * @param args
   */
  public static void main(String[] args) throws CLIProcessingException {

    BufferedReader inputR = new BufferedReader(new InputStreamReader(
        CLIMainDriver.class.getResourceAsStream("/cliregistry")));
    String line;

    try {
      while ((line = inputR.readLine()) != null) {
        System.out.println(line);
        Class<CLIRegistryInterface> registryClass = (Class<CLIRegistryInterface>) Class
            .forName(line);
        Method getInstancemethod = registryClass.getMethod("getInstance");
        CLIRegistryInterface retObj = (CLIRegistryInterface) getInstancemethod
            .invoke(null);
        retObj.register();
      }
    } catch (SecurityException e1) {
      throw new CLIProcessingException(e1);
    } catch (IllegalArgumentException e1) {
      throw new CLIProcessingException(e1);
    } catch (IOException e1) {
      throw new CLIProcessingException(e1);
    } catch (ClassNotFoundException e1) {
      throw new CLIProcessingException(e1);
    } catch (NoSuchMethodException e1) {
      throw new CLIProcessingException(e1);
    } catch (IllegalAccessException e1) {
      throw new CLIProcessingException(e1);
    } catch (InvocationTargetException e1) {
      throw new CLIProcessingException(e1);
    }

    if ((args == null || args.length == 0)) {
      System.out.print(CLICommandFactory.getInstance().getUsage(false));
      LOG.info(CLICommandFactory.getInstance().getUsage(false));
      return;
    }

    // LOG.info(Arrays.asList(args));

    if (args[0].equalsIgnoreCase(ADMIN_USAGE_PARAM)) {
      LOG.info(Arrays.asList(args).toString());
      System.out.print(CLICommandFactory.getInstance().getUsage(true));
      LOG.info(CLICommandFactory.getInstance().getUsage(true));
      return;
    }

    ProcessedInput input = new ProcessedInput(args);

    Parameter logLevelParam = input.getParameterByName(CLI_LOG_LEVEL_PARAM);
    String logLevel = null;
    if (logLevelParam != null) {
      // need to set log level to provided value
      List<String> logValues = logLevelParam.getParamValues();
      if (logValues.isEmpty()) {
        LOG.warn(CLI_LOG_LEVEL_PARAM
            + " does not have any level set. Using default one from log4j.properties");
      } else {
        logLevel = logValues.get(0);
      }
      input.removeParameter(CLI_LOG_LEVEL_PARAM);
    }

    boolean isOverrideUsage = false;
    if (input.getParameterByName(ADMIN_USAGE_PARAM) != null) {
      input.removeParameter(ADMIN_USAGE_PARAM);
      isOverrideUsage = true;
    }

    boolean isToLogInput = true;
    Map<String, Parameter> params = input.getAllParameters();
    for (Parameter param : params.values()) {
      String paramName = param.getParamName();
      List<String> paramValues = param.getParamValues();
      if (paramName.indexOf(CLIInterface.PASSWORD_PREFIX) >= 0) {
        // don't log
        isToLogInput = false;
        break;
      }
      for (String paramValue : paramValues) {
        if (paramValue.indexOf(CLIInterface.PASSWORD_PREFIX) >= 0) {
          // don't log
          isToLogInput = false;
          break;
        }
      }
      if (!isToLogInput) {
        break;
      }
    }

    if (isToLogInput) {
      LOG.info(Arrays.asList(args).toString());
    } else {
      LOG.info(input.getCommandName()
          + (!input.getSubCommandNames().isEmpty() ? ","
              + input.getSubCommandNames() : "") + "****");
    }

    CLIInterface commandIFace = null;
    try {
      commandIFace = CLICommandFactory.getInstance().getCLI(input);
    } catch (CLIProcessingException e) {
      /**
       * <MAPR_ERROR> Message:Exception during search for command Interface.
       * Function:CLIMainDriver.main() Meaning:An internal error occurred.
       * Resolution:Check the command syntax. </MAPR_ERROR>
       */
      LOG.error("Exception during search for command Interface.", e);
    }

    if (commandIFace == null) {
      /**
       * <MAPR_ERROR> Message:Could not find Interface for a command: <input>
       * Function:CLIMainDriver.main() Meaning:The interface for the command
       * does not appear to exist. Resolution:Check the command syntax and try
       * again. </MAPR_ERROR>
       */
      LOG.error("Could not find Interface for a command: "
          + Arrays.asList(input.getRawInput()));
      // print usage
      System.out.print("Could not find Interface for a command: "
          + Arrays.asList(input.getRawInput()) + "\n");
      if (isToLogInput) {
        LOG.error("Could not find Interface for a command: "
            + Arrays.asList(input.getRawInput()) + "\n");
      } else {
        LOG.error("Could not find Interface for a command: "
            + input.getCommandName()
            + (!input.getSubCommandNames().isEmpty() ? ","
                + input.getSubCommandNames() : "") + "****");
      }
      System.out.print(CLICommandFactory.getInstance()
          .getUsage(isOverrideUsage));
      LOG.info(CLICommandFactory.getInstance().getUsage(isOverrideUsage));
      System.exit(1);
    }

    boolean isJsonParamPresent = false;
    boolean isPropagateErrorPresent = false;
    // see if asked to give JSON output
    if (input.getParameterByName(SPECIAL_JSON_OUTPUT_PARAM) != null) {
      // asked for JSON output format
      input.removeParameter(SPECIAL_JSON_OUTPUT_PARAM);
      isJsonParamPresent = true;
    }
    // see if asked to propagate error
    if (input.getParameterByName(SPECIAL_PROPAGATE_ERROR_PARAM) != null) {
      // asked to propagate error code in cli return
      input.removeParameter(SPECIAL_PROPAGATE_ERROR_PARAM);
      isPropagateErrorPresent = true;
    }
    boolean isLongOutput = false;
    if (input.getParameterByName(LONG_OUTPUT_PARAM) != null) {
      input.removeParameter(LONG_OUTPUT_PARAM);
      isLongOutput = true;
    }
    boolean isHeaderNeeded = true;
    if (input.getParameterByName(NOHEADER_OUTPUT_PARAM) != null) {
      input.removeParameter(NOHEADER_OUTPUT_PARAM);
      isHeaderNeeded = false;
    }

    CLICommand mainCommand = commandIFace.getCLICommand();
    if (isOverrideUsage) {
      mainCommand.setGlobalUsageInVisibility(false);
    }

    try {
      if (!commandIFace.validateInput()) {
        System.out.print(mainCommand.getUsageFromParametersOfCommandsTree());
        LOG.info(mainCommand.getUsageFromParametersOfCommandsTree());
        System.exit(1);
      }

      /*
       * if ( Security.IsSecurityEnabled(CLDBRpcCommonUtils.getInstance().
       * getCurrentClusterName())) { // If security is enabled get the ticket
       * String ticketFileLocation = Security.GetUserTicketAndKeyFileLocation();
       * if ( ticketFileLocation == null ) {
       * LOG.error("TicketKey file is not found"); System.exit(1); }
       * 
       * 
       * int errorCode = Security.SetTicketAndKeyFile(ServerKeyType.ServerKey,
       * ticketFileLocation); if ( errorCode != 0 ) {
       * LOG.error("Problem with TicketKey file: " + errorCode); System.exit(1);
       * } }
       */
      CommandOutput output = null;
      try {
        output = commandIFace.executeCommand();
      } catch (Throwable e) {
        boolean isSecurityException = false;
        Throwable t = e;

        if (isSecurityException) {
          output = new CommandOutput();
          OutputHierarchy out = new OutputHierarchy();
          output.setOutput(out);
          out.addError(new OutputError(Errno.EINVAL, t.getMessage()));
          LOG.error(
              "Security Exception processing "
                  + Arrays.asList(input.getRawInput()) + " exception", e);
        } else {
          throw e;
        }
      }

      if (isJsonParamPresent) {
        // json based output (based class one)
        System.out.println(CommandOutput.toPrettyStringStatic(output
            .toJSONString()));
        LOG.info(CommandOutput.toPrettyStringStatic(output.toJSONString()));
      } else {
        // output per command if defined
        // TODO Fix this terrible hack
        if (output instanceof TextCommandOutput) {
          System.out.println(output.toPrettyString());
          LOG.info(output.toPrettyString());
        } else {
          CommandOutput lineOutput = new CommandLineOutput(output.getOutput());
          lineOutput.setHeaderRequired(isHeaderNeeded);
          lineOutput.setLongOutput(isLongOutput);
          lineOutput.setNodeOrder(output.getNodeOrder());
          System.out.print(lineOutput.toPrettyString());
          LOG.info(lineOutput.toPrettyString());
        }
      }
      if (output.getOutput() == null) {
        System.exit(1);
      }
      if (output.getOutput().getOutputErrors() == null
          || output.getOutput().getOutputErrors().isEmpty()) {
        System.exit(0);
      } else {
        OutputError oe = output.getOutput().getOutputErrors().get(0);
        if (isPropagateErrorPresent && oe.isPropagateErrorSupported()) {
          System.exit(oe.getErrorCode());
        }
        System.exit(1);
      }
    } catch (CLIProcessingException e) {
      /**
       * <MAPR_ERROR> Message:Exception during command execution
       * Function:CLIMainDriver.main() Meaning:An internal error occurred during
       * execution of a command. Resolution: </MAPR_ERROR>
       */
      LOG.error("Exception during " + Arrays.asList(input.getRawInput())
          + " execution", e);
      System.exit(1);
    } catch (Throwable e) {
      /**
       * <MAPR_ERROR> Message:Exception during command execution
       * Function:CLIMainDriver.main() Meaning:An Unexpected Error Occurred
       * Resolution: </MAPR_ERROR>
       */
      LOG.error("Exception during " + Arrays.asList(input.getRawInput())
          + " execution ", e);
      System.exit(1);
    }
  }

}
