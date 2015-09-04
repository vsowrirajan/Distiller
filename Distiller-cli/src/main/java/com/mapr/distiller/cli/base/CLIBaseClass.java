package com.mapr.distiller.cli.base;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputError;
import com.mapr.distiller.cli.base.ProcessedInput.Parameter;
import com.mapr.distiller.cli.base.common.Errno;
import com.mapr.distiller.cli.base.inputparams.BaseInputParameter;

/**
 * Base abstract class in case common functionality is needed
 * 
 * @author yufeldman
 *
 */
public abstract class CLIBaseClass implements CLIInterface {

  private static final Logger LOG = Logger.getLogger(CLIBaseClass.class);

  // cache users info but need to invalidate cache from time to time, as groups
  // may change in between
  private static Map<String, UserInfo> usersMap = new HashMap<String, UserInfo>();

  public static final boolean REQUIRED = true;
  public static final boolean NOT_REQUIRED = false;
  public static final String HELP_PARAM = "help";

  protected ProcessedInput input;
  protected CLICommand cliCommand;
  protected UserInfo userInfo;
  protected CommandOutput output = new CommandOutput();

  public CLIBaseClass(ProcessedInput input, CLICommand cliCommand) {
    this.input = input;
    this.cliCommand = cliCommand;
    //setUserCredentials();
  }

  public ProcessedInput getInput() {
    return input;
  }

  @Override
  public CLICommand getCLICommand() {
    return cliCommand;
  }

  /**
   * Method that validates input Child classes may overwrite it and/or add some
   * functionality but it should basically cover all the cases if Input
   * Parameters during registration defined correctly
   */
  @Override
  public boolean validateInput() throws IllegalArgumentException {
    if (cliCommand == null || cliCommand.getParameters() == null) {
      // nothing to validate
      return true;
    }
    // getParameters from classes and validate against input
    Set<String> inputParamNames = new HashSet<String>(input.getAllParameters()
        .keySet());
    inputParamNames.removeAll(input.getSubCommandNames());
    for (Map.Entry<String, BaseInputParameter> entry : cliCommand
        .getParameters().entrySet()) {
      String paramName = entry.getKey();
      BaseInputParameter param = entry.getValue();
      Parameter inputParam = input.getAllParameters().get(paramName);
      if (null == inputParam) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Registered Parameter: " + param.getName()
              + " is missing. Using defaultValue");
        }
        // fill input from Default value of registration
        if (param.getParameterDefaultValueAsString() != null) {
          Parameter addInputParam = new Parameter(paramName);
          if (param.isValueRequired()) {
            addInputParam.addParamValue(param
                .getParameterDefaultValueAsString());
          }
          input.addParameter(addInputParam);
          inputParam = input.getAllParameters().get(paramName);
        } else {
          if (param.isRequired()) {
            /**
             * <MAPR_ERROR> Message:Required parameter <parameter> is missing.
             * No valid default value is available.
             * Function:CLIBaseClass.validateInput() Meaning:The reported
             * parameter is required for the command. Resolution:Check the
             * command syntax and make sure to provide valid values for all
             * required parameters. </MAPR_ERROR>
             */
            LOG.error("Required parameter " + param.getName()
                + " is missing. No valid default value is available.");
            output.getOutput().addError(
                new OutputError(Errno.EINVAL, "Required parameter: "
                    + param.getName()
                    + " is missing and no valid default value provided")
                    .setField(param.getName()));
            return false;
          } else {
            // just continue - don't care if not required param is missing
            continue;
          }
        }
      }
      inputParamNames.remove(inputParam.getParamName());
      if (param.isValueRequired() && inputParam.getParamValues().isEmpty()) {
        /**
         * <MAPR_ERROR> Message:A value is required for parameter <parameter>
         * Function:CLIBaseClass.validateInput() Meaning:The reported parameter
         * requires a value. Resolution:Check the command syntax and make sure
         * to provide valid values for all required parameters. </MAPR_ERROR>
         */
        LOG.error("A value is required for parameter "
            + inputParam.getParamName());
        output.getOutput().addError(
            new OutputError(Errno.EINVAL,
                "Value is required, but is missing for parameter: "
                    + inputParam.getParamName()).setField(inputParam
                .getParamName()));
        return false;
      }
      for (String paramValue : inputParam.getParamValues()) {
        if (null == param.valueOf(paramValue)) {
          /**
           * <MAPR_ERROR> Message:Unexpected value/type provided for parameter
           * <parameter> Function:CLIBaseClass.validateInput() Meaning:The
           * reported parameter was an invalid value or type for the command.
           * Resolution:Check the command syntax and make sure to provide valid
           * values for all required parameters. </MAPR_ERROR>
           */
          LOG.error("Unexpected value/type provided for parameter "
              + inputParam.getParamName());
          output.getOutput().addError(
              new OutputError(Errno.EINVAL,
                  "Unexpected value/type of parameter: "
                      + inputParam.getParamName()).setField(inputParam
                  .getParamName()));
          return false;
        }
        if (!param.isWithinBoundaries(paramValue)) {
          /**
           * <MAPR_ERROR> Message:Value is out of boundaries for parameter
           * <parameter> Function:CLIBaseClass.validateInput() Meaning:The
           * reported parameter was an invalid value. Resolution:Check the
           * command syntax and make sure to provide values within the valid
           * range. </MAPR_ERROR>
           */
          LOG.error("Value is out of boundaries for parameter "
              + param.getName());
          output.getOutput().addError(
              new OutputError(Errno.EINVAL,
                  "Value is out of boundaries for parameter: "
                      + param.getName()).setField(param.getName())
                  .setFieldValue(paramValue));
          return false;
        }
      }
    }
    if (!inputParamNames.isEmpty()) {
      /**
       * <MAPR_ERROR> Message:Invalid parameters: <parameters>
       * Function:CLIBaseClass.validateInput() Meaning:The reported parameters
       * are unknown or invalid. Resolution:Check the command syntax and make
       * sure to provide only the correct command parameters. </MAPR_ERROR>
       */
      LOG.error("Invalid parameters: " + inputParamNames);
      for (String invalidParam : inputParamNames) {
        output.getOutput().addError(
            new OutputError(Errno.EINVAL, "Invalid parameter is provided: "
                + inputParamNames).setField(invalidParam));
      }
      return false;
    }
    return true;
  }

  /**
   * return command Usage based on CLICommand registration usage
   * 
   * @return - command usage
   */
  public String getCommandUsage() {
    if (cliCommand != null) {
      return cliCommand.getUsageFromParametersOfCommandsTree();
    }
    return null;
  }

  @Override
  public CommandOutput executeCommand() throws CLIProcessingException {
    for (String subcommand : input.getSubCommandNames()) {
      if (HELP_PARAM.equalsIgnoreCase(subcommand)) {
        if (cliCommand != null) {
          return new TextCommandOutput(cliCommand
              .getUsageFromParametersOfCommandsTree().getBytes());
        }
      }
    }
    if (!validateInput()) {
      return output;
    }

    return executeRealCommand();
  }

  /**
   * Meat of the framework - each child class should implement this with it's
   * own implementation
   * 
   * @return
   * @throws CLIProcessingException
   */
  public abstract CommandOutput executeRealCommand()
      throws CLIProcessingException;

  /**
   * Helper method to execute ssh command based on provided string
   * 
   * @param timeout
   * @param inputString
   * @return
   * @throws CLIProcessingException
   */
  protected byte[] executeSimpleSHHCommand(long timeout, String inputString)
      throws CLIProcessingException {
    if (inputString == null) {
      return executeSimpleSHHCommandPrivate(timeout, input.getRawInput());
    }
    String[] command = inputString.split(" ");
    return executeSimpleSHHCommandPrivate(timeout, command);
  }

  /**
   * Just Helper method to allow executed simple ssh commands from children
   * classes
   * 
   * @param timeout
   *          - how long needs to wait till command will timeout
   * @return
   * @throws CLIProcessingException
   */
  protected byte[] executeSimpleSHHCommand(long timeout)
      throws CLIProcessingException {
    return executeSimpleSHHCommandPrivate(timeout, input.getRawInput());
  }

  private static byte[] executeSimpleSHHCommandPrivate(long timeout,
      String[] command) throws CLIProcessingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int retValue = -1;

    List<String> finalCommand = new ArrayList<String>();
    for (String comToken : command) {
      finalCommand.addAll(Arrays.asList(comToken.split(" ")));
    }
    try {
      ProcessBuilder pb = new ProcessBuilder(finalCommand);
      // may be we can specify directory from where to run as one of the
      // parameters
      pb.directory(null);
      pb.redirectErrorStream(true);
      Process process = pb.start();
      BufferedInputStream is = new BufferedInputStream(process.getInputStream());

      try {
        long startProcessingTime = System.currentTimeMillis();
        int len;
        while ((len = is.read(buf)) != -1
            && (System.currentTimeMillis() - startProcessingTime) < timeout) {
          baos.write(buf, 0, len);
        }
        if ((System.currentTimeMillis() - startProcessingTime) < timeout) {
          try {
            retValue = process.waitFor();
          } catch (InterruptedException e) {
            /**
             * <MAPR_ERROR> Message:Interrupted Exception during command:
             * <command> run
             * Function:CLIBaseClass.executeSimpleSHHCommandPrivate()
             * Meaning:The command was interrupted during execution.
             * Resolution:Try the request again. </MAPR_ERROR>
             */
            LOG.error("Interrupted Exception during command: " + finalCommand
                + " run", e);
          }
        }
      } finally {
        is.close();
        process.destroy();
      }
    } catch (IOException ioex) {
      /**
       * <MAPR_ERROR> Message: Function: Meaning: Resolution: </MAPR_ERROR>
       */
      String out = "IOException during process firing";
      LOG.error(out, ioex);
      throw new CLIProcessingException(out);
      // deal with it below along with unsuccessful processing
    }
    if (retValue != 0) {
      /**
       * <MAPR_ERROR> Message:Error while running command: <command>
       * Function:CLIBaseClass.executeSimpleSHHCommandPrivate() Meaning:The
       * command encountered an internal error. Resolution: </MAPR_ERROR>
       */
      String out = "Error while running command: " + finalCommand;
      LOG.error(out);
      throw new CLIProcessingException(out);
    }
    return baos.toByteArray();

  }

  /**
   * Helper method to get int value of a provided parameter at specified
   * position
   * 
   * @param paramName
   * @param position
   * @return
   * @throws CLIProcessingException
   */
  public int getParamIntValue(String paramName, int position)
      throws CLIProcessingException {
    try {
      Integer param = (Integer) getParamObjectValue(paramName, position);
      return param.intValue();
    } catch (NumberFormatException nfe) {
      /**
       * <MAPR_ERROR> Message:Parameter is not an Integer
       * Function:CLIBaseClass.getParamIntValue() Meaning:A non-integer value
       * was passed in a parameter of type Integer. Resolution:Check the command
       * syntax and make sure to provide valid values for all required
       * parameters. </MAPR_ERROR>
       */
      LOG.error("Parameter " + paramName + " is not Integer");
      throw new CLIProcessingException("Parameter " + paramName
          + " is not Integer");
    }

  }

  /**
   * Helper method to get long value of a provided parameter at specified
   * position
   * 
   * @param paramName
   * @param position
   * @return
   * @throws CLIProcessingException
   */
  public long getParamLongValue(String paramName, int position)
      throws CLIProcessingException {
    try {
      Long param = (Long) getParamObjectValue(paramName, position);
      return param.longValue();
    } catch (NumberFormatException nfe) {
      /**
       * <MAPR_ERROR> Message:Parameter is not Long
       * Function:CLIBaseClass.getParamLongValue() Meaning:A non-long value was
       * passed in a parameter of type Long. Resolution:Check the command syntax
       * and make sure to provide valid values for all required parameters.
       * </MAPR_ERROR>
       */
      LOG.error("Parameter " + paramName + " is not Long");
      throw new CLIProcessingException("Parameter " + paramName
          + " is not Long");
    }

  }

  /**
   * Helper method to get String value of a provided parameter at specified
   * position
   * 
   * @param paramName
   * @param position
   * @return
   * @throws CLIProcessingException
   */
  public String getParamTextValue(String paramName, int position)
      throws CLIProcessingException {
    return (String) getParamObjectValue(paramName, position);
  }

  /**
   * Helper method to get boolean value of a provided parameter at specified
   * position
   * 
   * @param paramName
   * @param position
   * @return
   * @throws CLIProcessingException
   */
  public boolean getParamBooleanValue(String paramName, int position)
      throws CLIProcessingException {
    try {
      Boolean boolO = (Boolean) getParamObjectValue(paramName, position);
      return boolO.booleanValue();
    } catch (ClassCastException ce) {
      /**
       * <MAPR_ERROR> Message:Parameter value is not of type Boolean
       * Function:CLIBaseClass.getParamBooleanValue() Meaning:A non-boolean
       * value was passed in a parameter of type Boolean. Resolution:Check the
       * command syntax and make sure to provide valid values for all required
       * parameters. </MAPR_ERROR>
       */
      LOG.error("Parameter " + paramName + " is not of type Boolean");
      throw new CLIProcessingException("Parameter " + paramName
          + " is not of type Boolean");
    }
  }

  /**
   * Helper method to get date value of a provided parameter at specified
   * position
   * 
   * @param paramName
   * @param position
   * @return
   * @throws CLIProcessingException
   */
  public Date getParamDateValue(String paramName, int position)
      throws CLIProcessingException {
    try {
      return (Date) getParamObjectValue(paramName, position);
    } catch (ClassCastException ce) {
      /**
       * <MAPR_ERROR> Message:Parameter value is not of type Date
       * Function:CLIBaseClass.getParamDateValue() Meaning:A non-date value was
       * passed in a parameter of type Date. Resolution:Check the command syntax
       * and make sure to provide valid values for all required parameters.
       * </MAPR_ERROR>
       */
      LOG.error("Parameter " + paramName + " is not of type Date");
      throw new CLIProcessingException("Parameter " + paramName
          + " is not of type Date");
    }
  }

  /**
   * Helper method to get Object value of a provided parameter at specified
   * position
   * 
   * @param paramName
   * @param position
   * @return
   * @throws CLIProcessingException
   */
  public Object getParamObjectValue(String paramName, int position)
      throws CLIProcessingException {
    Parameter intParam = input.getParameterByName(paramName);
    if (intParam == null) {
      /**
       * <MAPR_ERROR> Message:Parameter <parameter> is not found
       * Function:CLIBaseClass.getParamObjectValue() Meaning:The reported
       * parameter or its value is missing. Resolution:Check the command syntax
       * and make sure to provide valid values for all required parameters.
       * </MAPR_ERROR>
       */
      LOG.error("Parameter " + paramName + " is not found");
      throw new CLIProcessingException("Parameter " + paramName
          + " is not found");
    }
    try {
      Object paramI = cliCommand.getParameters().get(paramName)
          .valueOf(intParam.getParamValues().get(position));

      if (paramI == null) {
        /**
         * <MAPR_ERROR> Message:Parameter <parameter> is defined for command
         * <command> Function:CLIBaseClass.getParamObjectValue() Meaning:The
         * reported parameter or its value could not be found. Resolution:Check
         * the command syntax and make sure to provide valid values for all
         * required parameters. </MAPR_ERROR>
         */
        LOG.error("Parameter " + paramName + " is defined for command "
            + cliCommand.getCommandName());
        throw new CLIProcessingException("Parameter " + paramName
            + " is defined for command " + cliCommand.getCommandName());
      }
      return paramI;
    } catch (IndexOutOfBoundsException ae) {
      /**
       * <MAPR_ERROR> Message:Position <position> is out of bounds for parameter
       * <parameter> Function:CLIBaseClass.getParamObjectValue() Meaning:The
       * reported parameter was an invalid value. Resolution:Check the command
       * syntax and make sure to provide values within the valid range.
       * </MAPR_ERROR>
       */
      LOG.error("Position " + position + " is out of bounds for parameter "
          + paramName, ae);
      throw new CLIProcessingException("Position " + position
          + " is out of bounds for parameter " + paramName);
    }

  }

  /**
   * Helper method to check whether input has specified parameter
   * 
   * @param paramName
   * @return
   * @throws CLIProcessingException
   */
  public boolean isParamPresent(String paramName) throws CLIProcessingException {
    return (input.getParameterByName(paramName) != null) ? true : false;
  }

  /**
   * Get current user credentials from the system and set uid and gids from it
   */
  private void setUserCredentials() {
    throw new UnsupportedOperationException("Not a valid method in Distiller-cli");
  }

  @Override
  public void setUserCredentials(String userLoginName)
      throws CLIProcessingException {
    throw new UnsupportedOperationException("Not a valid method in Distiller-cli");
  }

  @Override
  public long getUserId() {
    throw new UnsupportedOperationException("Not a valid method in Distiller-cli");
  }

  @Override
  public String getUserLoginId() {
    throw new UnsupportedOperationException("Not a valid method in Distiller-cli");
  }

  @Override
  public Set<Long> getGIds() {
    throw new UnsupportedOperationException("Not a valid method in Distiller-cli");
  }

  /*
   * public CredentialsMsg getUserCredentials() { Builder msg =
   * CredentialsMsg.newBuilder().setUid((int)getUserId()); for ( Long gid:
   * userInfo.getUserGroupIds() ) { msg.addGids(gid.intValue()); } return
   * msg.build(); }
   */

  /**
   * Helper class to hold user loginID, uid, gids
   * 
   * @author yufeldman
   *
   */
  private static final class UserInfo {
    private long userId;
    private Set<Long> userGroupIds = new HashSet<Long>();
    private String userLoginId;

    public UserInfo(long userId, String userLoginId) {
      this.userId = userId;
      this.userLoginId = userLoginId;
    }

    public long getUserId() {
      return userId;
    }

    public void setUserId(long userId) {
      this.userId = userId;
    }

    public Set<Long> getUserGroupIds() {
      return userGroupIds;
    }

    public void setUserGroupIds(Set<Long> userGroupIds) {
      this.userGroupIds = userGroupIds;
    }

    public String getUserLoginId() {
      return userLoginId;
    }

    public void setUserLoginId(String userLoginId) {
      this.userLoginId = userLoginId;
    }

    public void addUserGroupId(long gid) {
      userGroupIds.add(gid);
    }
  }
}
