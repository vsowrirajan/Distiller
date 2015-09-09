package com.mapr.distiller.cli.base;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.cli.base.inputparams.BaseInputParameter;

/**
 * Class to keep together all the information that each command was registered
 * with
 */
public class CLICommand {

  private static final Logger LOG = LoggerFactory.getLogger(CLICommand.class);
  
  /**
   * Name of the command
   */
  private String commandName;
  /**
   * Class that implements a command
   */
  private Class<? extends CLIInterface> commandClass;
  /**
   * Type of the execution
   */
  private ExecutionTypeEnum executionTypeEnum;
  /**
   * Command Input parameters
   */
  private Map<String, BaseInputParameter> parameters = new HashMap<String, BaseInputParameter>();
  /**
   * Subcommands of current command
   */
  private CLICommand[] subcommands;
  /**
   * Command usage
   */
  private String commandUsage;

  /**
   * Parent command if any so far the only usage of this one to get visibility
   * on parent command name
   */
  private CLICommand parentCommand;

  /*
   * Print usage for this command only if it is visible
   */
  private boolean isUsageInVisible;
  /**
   * Short command usage used in list help
   */
  private String shortUsage;

  public CLICommand(String commandName) {
    this(commandName, "no usage provided", null, null, null);
  }

  public CLICommand(String commandName, String commandUsage,
      Class<? extends CLIInterface> commandClass,
      ExecutionTypeEnum executionTypeEnum) {
    this(commandName, commandUsage, commandClass, executionTypeEnum, null);
  }

  public CLICommand(String commandName, String commandUsage,
      Class<? extends CLIInterface> commandClass,
      ExecutionTypeEnum executionTypeEnum, CLICommand[] subcommands) {
    this(commandName, commandUsage, commandClass, executionTypeEnum, null,
        subcommands);
  }

  public CLICommand(String commandName, String commandUsage,
      Class<? extends CLIInterface> commandClass,
      ExecutionTypeEnum executionTypeEnum,
      Map<String, BaseInputParameter> params, CLICommand[] subcommands) {
    this.commandName = commandName;
    this.commandUsage = commandUsage;
    this.commandClass = commandClass;
    this.executionTypeEnum = executionTypeEnum;
    this.parameters = params;
    this.subcommands = subcommands;
    if (this.subcommands != null && this.subcommands.length > 0) {
      for (CLICommand subcommand : this.subcommands) {
        subcommand.parentCommand = this;
      }
    }

  }

  public CLICommand(String commandName, CLICommand[] subcommands) {
    this(commandName, "no usage provided", null, null, subcommands);
  }

  public CLICommand(String commandName, String usage, CLICommand[] subcommands) {
    this(commandName, usage, null, null, subcommands);
  }

  /**
   * Returns recursively usage of this and all subcommands NOT USED effectively
   * anymore
   * 
   * @return
   */
  public String getUsage() {
    StringBuilder usageB = new StringBuilder();
    if (commandUsage != null && !commandUsage.isEmpty()) {
      usageB.append(commandUsage);
      usageB.append("\n");
    }
    if (subcommands != null) {
      for (CLICommand command : subcommands) {
        usageB.append(command.getUsage());
        usageB.append("\n");
      }
    }

    return usageB.toString();
  }

  public Class<? extends CLIInterface> getCommandClass()
      throws CLIProcessingException {
    return commandClass;
  }

  public ExecutionTypeEnum getExecutionTypeEnum() {
    return executionTypeEnum;
  }

  public Map<String, BaseInputParameter> getParameters() {
    return parameters;
  }

  public CLICommand[] getSubcommands() {
    return subcommands;
  }

  public String getCommandName() {
    return commandName;
  }

  public CLICommand setShortUsage(String shortUsage) {
    this.shortUsage = shortUsage;
    return this;
  }

  public String getShortUsage() {
    return shortUsage;
  }

  public CLICommand getParentCommand() {
    return parentCommand;
  }

  /**
   * Get all parent hierarchy command names used for usage construction
   * 
   * @return
   */
  public String getParentCommandNames() {
    if (parentCommand != null) {
      return parentCommand.getParentCommandNames() + " " + commandName;
    }
    return commandName;
  }

  /**
   * Construct usage only for a particular command w/o taking into consideration
   * children
   * 
   * @return
   */
  public String getUsageOfOnlyThisCommand() {
    StringBuilder sb = new StringBuilder();
    sb.append(commandName);
    sb.append("\n");
    sb.append(getUsageFromParameters());
    return sb.toString();
  }

  /**
   * Construct usage only for a particular command with all children commands
   * usage and parent command names
   * 
   * @return
   */
  public String getUsageFromParametersOfCommandsTree() {
    if (isUsageInVisible) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    if (parentCommand != null) {
      sb.append(getParentCommandNames());
    } else {
      sb.append(commandName);
    }
    sb.append("\n");
    int outputLength = sb.length();

    if (subcommands == null || subcommands.length == 0) {
      sb.append(getUsageFromParameters(0));
      if (sb.length() == outputLength) {
        // no params and no subcommands better to give at least command's usage
        sb.append("Usage : " + getUsage());
      }
      return sb.toString();
    }
    for (CLICommand childCommand : subcommands) {
      sb.append(childCommand.getUsageFromParametersOfCommandsTree(1));
      sb.append("\n");
    }

    return sb.toString();
  }

  /**
   * Helper method to get usage with right number of spacing
   * 
   * @param tabCount
   * @return
   */
  String getUsageFromParametersOfCommandsTree(int tabCount) {
    if (isUsageInVisible) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tabCount; i++) {
      sb.append("\t");
    }
    sb.append(commandName);
    sb.append("\n");

    if (subcommands == null || subcommands.length == 0) {
      sb.append(getUsageFromParameters(tabCount));
      return sb.toString();
    }
    for (CLICommand childCommand : subcommands) {
      sb.append(childCommand.getUsageFromParametersOfCommandsTree(tabCount + 1));
      sb.append("\n");
    }

    return sb.toString();
  }

  /**
   * Helper method to get usage with right number of spacing
   * 
   * @param tabCount
   * @return
   */
  String getUsageFromParameters(int tabCount) {
    if (parameters == null || parameters.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, BaseInputParameter> entry : parameters.entrySet()) {
      BaseInputParameter inputParam = entry.getValue();
      if (inputParam.isInvisible()) {
        continue;
      }
      for (int i = 0; i < tabCount; i++) {
        sb.append("\t");
      }
      sb.append("\t");
      String paramName = "-" + inputParam.getName();
      boolean isRequired = inputParam.isRequired();
      boolean isValueRequired = inputParam.isValueRequired();
      String description = inputParam.getDescription();
      String lowBoundary = inputParam.getMinValueAsString();
      String upperBoundary = inputParam.getMaxValueAsString();
      boolean isBoundaryApplicable = inputParam.minAndMaxApplicable();
      String defaultValue = inputParam.getParameterDefaultValueAsString();
      // construct usage for a parameter
      if (!isRequired) {
        // sb.append("\"[<");
        sb.append("[ ");
      } else {
        // sb.append("\"< ");
        sb.append(" ");
      }
      sb.append(paramName);
      sb.append(" ");
      sb.append((description == null) ? entry.getKey() : description);
      if (!isValueRequired) {
        sb.append(" Parameter takes no value");
        sb.append(" ");
      } else {
        if (isBoundaryApplicable
            && (lowBoundary != null || upperBoundary != null)) {
          sb.append(" <");
          sb.append((lowBoundary != null) ? " Low Boundary: " + lowBoundary
              : "");
          sb.append((upperBoundary != null) ? ". Upper Boundary: "
              + upperBoundary : "");
          sb.append(">");
        }
        if (defaultValue != null) {
          sb.append(". default: ");
          sb.append(defaultValue);
        }
      }

      if (!isRequired) {
        // sb.append(">]\"");
        sb.append(" ]");
      } else {
        // sb.append(">\"");
        sb.append(" ");
      }
      sb.append("\n");
    }
    return sb.toString();

  }

  /**
   * Get usage based on parameters and their descriptions
   * 
   * @return
   */
  public String getUsageFromParameters() {
    return getUsageFromParameters(0);
  }

  public CLICommand setUsageInVisible(boolean isUsageVisible) {
    this.isUsageInVisible = isUsageVisible;
    return this;
  }

  public boolean isUsageInVisible() {
    return isUsageInVisible;
  }

  public void setGlobalUsageInVisibility(boolean isUsageVisible) {
    this.isUsageInVisible = isUsageVisible;
    if (parameters != null) {
      for (BaseInputParameter param : parameters.values()) {
        param.setInvisible(isUsageVisible);
      }
    }
    if (subcommands != null) {
      for (CLICommand command : subcommands) {
        command.setGlobalUsageInVisibility(isUsageVisible);
      }
    }
  }

  /**
   * Enum to identify whether execution of the command should be coming from the
   * code, or just SSH command
   * 
   * @author yufeldman
   *
   */
  public enum ExecutionTypeEnum {
    SSH, NATIVE, REST;
  }
}
