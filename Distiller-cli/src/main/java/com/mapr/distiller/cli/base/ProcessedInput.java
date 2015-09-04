package com.mapr.distiller.cli.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mapr.distiller.cli.base.CLICommand.ExecutionTypeEnum;
import com.mapr.distiller.cli.base.CLICommand;


/**
 * Input from command line broken into parameters and options
 * @author yufeldman
 *
 */
public class ProcessedInput {

	private static final Logger LOG = Logger.getLogger(ProcessedInput.class);
	private String commandName;
	private List<String> subCommandNames = new ArrayList<String>();
	
	private  Map<String,Parameter> parameters = new HashMap<String,Parameter>();
	
	private String[] rawInput;
	
	private ExecutionTypeEnum executionType;
		
	public ProcessedInput(String[] argOpts) throws IllegalArgumentException {
		rawInput = argOpts;
		parseOutInput();
	}

	private void parseOutInput() {
		if ( rawInput == null || rawInput.length < 1 ) {
      /**
       * <MAPR_ERROR>
       * Message:No input parameters were provided.
       * Function:ProcessedInput.parseOutInput()
       * Meaning:The command requires input parameters. 
       * Resolution:Check the command syntax, and make sure to pass the correct parameters.
       * </MAPR_ERROR>
       */
			LOG.error("No input parameters were provided.");
			throw new IllegalArgumentException("No command was provided");
		}
		commandName = rawInput[0];
		boolean isSubCommandsDone = false;
		Parameter param = null;
		for ( int i = 1; i < rawInput.length; i++ ) {
			String arg = rawInput[i];
			if ( arg.startsWith("-") && arg.length() > 1 && arg.charAt(1) != '-') {
				isSubCommandsDone = true;
				param = new Parameter(arg.replaceFirst("-*", ""));
				parameters.put(param.getParamName(), param);
			} else {
				if ( isSubCommandsDone ) {
					if ( param != null ) {
						param.getParamValues().add(arg);
					} else {
						// can we have param w/o value - probably yes
					}
				} else {
					// add subcommand
					subCommandNames.add(arg);
					param = new Parameter(arg, new ArrayList<String>());
					parameters.put(param.getParamName(), param);
				}

			}
			
		}
			
	}
		

	public String[] getRawInput() {
		return rawInput;
	}
	
	public Map<String,Parameter> getAllParameters() {
		return Collections.unmodifiableMap(parameters);
	}
	
	public void addParameter(Parameter param) {
		if ( param != null ) {
			parameters.put(param.getParamName(), param);
		}
	}

	public void removeParameter(String paramName) {
		if ( paramName != null ) {
			parameters.remove(paramName);
		}
	}

	public Parameter getParameterByName(String name) {
		return parameters.get(name);
	}

        public int  getIntValueParameter(CLICommand cmd, String name, int position) {
          Parameter p = parameters.get(name);
          Integer i = (Integer) cmd.getParameters().get(name)
            .valueOf(p.getParamValues().get(position));
          return i.intValue();
        }

        public String getStringValueParameter(CLICommand cmd, String name, int position) {
          Parameter p = parameters.get(name);
          return (String) cmd.getParameters().get(name)
            .valueOf(p.getParamValues().get(position));
        }

	public String getCommandName() {
		return commandName;
	}

	public List<String> getSubCommandNames() {
		return subCommandNames;
	}

	public ExecutionTypeEnum getExecutionType() {
		return executionType;
	}

	public void setExecutionType(ExecutionTypeEnum executionType) {
		this.executionType = executionType;
	}

	public static final class Parameter {
		private final String paramName;
		private List<String> paramValues = new ArrayList<String>();
		
		Parameter(String paramName, List<String> paramOptions) {
			this.paramName = paramName;
			this.paramValues = paramOptions;
		}

		Parameter(String paramName) {
			this.paramName = paramName;			
		}
		
		public String getParamName() {
			return paramName;
		}

		public List<String> getParamValues() {
			return paramValues;
		}
		
		public void addParamValue(String paramValue) {
			paramValues.add(paramValue);
		}
	
		@Override
		public String toString() {
			return new String("ParamName: " + paramName + " ParamValues: " + paramValues.toString());
		}
	}
}


