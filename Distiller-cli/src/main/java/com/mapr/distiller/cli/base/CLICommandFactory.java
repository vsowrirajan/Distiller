package com.mapr.distiller.cli.base;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * Factory to get CLIInterface from CLICommandRegistry 
 * @author yufeldman
 *
 */
public class CLICommandFactory {

	private static final Logger LOG = Logger.getLogger(CLICommandFactory.class);
	
	private static CLICommandFactory s_instance  = new CLICommandFactory();
	
	private CLICommandFactory() {
	}
	
	public static CLICommandFactory getInstance() {
		return s_instance;
	}
		
	/**
	 * Get all registered commands in this execution
	 * @return
	 */
	public Map<String, CLICommand> getAllRegisteredCommands() {
		return CLICommandRegistry.getInstance().getAllCommands();
	}
		
	/**
	 * Provide method to get list of all registered commands
	 * Needs to be enhanced to provide their usage as well
	 * @return
	 */
	public String getUsage(boolean isOverrideHiddenUsage) {
		Map<String, CLICommand> allCommands = getAllRegisteredCommands();
		TreeMap<String, String> allCommandsUsage = new TreeMap<String,String>();
		StringBuilder usageB = new StringBuilder("list of commands:");
		usageB.append("\n\n");
		int maxLength = -1;
		for ( CLICommand cliCommand : allCommands.values()) {
			if (cliCommand.isUsageInVisible() && !isOverrideHiddenUsage) {
				continue;
			}
			if ( cliCommand.getCommandName().length() > maxLength ) {
				maxLength = cliCommand.getCommandName().length();
			}
			allCommandsUsage.put(cliCommand.getCommandName(), (null == cliCommand.getShortUsage()) ? "short description is not available" : cliCommand.getShortUsage());
		}
		
		for ( Map.Entry<String, String> commandUsage : allCommandsUsage.entrySet()) {
			char[] asChars = Arrays.copyOf(commandUsage.getKey().toCharArray(), maxLength);
			Arrays.fill(asChars, commandUsage.getKey().length(), asChars.length, ' ');
			usageB.append("\t" + new String(asChars));
			usageB.append("\t" + commandUsage.getValue());
			usageB.append("\n");
		}
		return usageB.toString();
	}

	/**
	 * Figure out which CLIInterface corresponds best to input provided
	 * method is looking recursively if needed through all subcommands 
	 * @param input - ProcessedInput
	 * @return CLIInterface
	 * @throws CLIProcessingException
	 */
	public CLIInterface getCLI(ProcessedInput input) throws CLIProcessingException {
		if ( input == null ) {
			return null;
		}
		
		CLICommand cliCommand = CLICommandRegistry.getInstance().getCLICommand(input.getCommandName());
		if ( cliCommand == null ) {
      /**
       * <MAPR_ERROR>
       * Message:CLICommand is not found: <command>
       * Function:CLICommandFactory.getCLI()
       * Meaning:The command does not exist. 
       * Resolution:Check the command syntax and make sure the command is typed correctly.
       * </MAPR_ERROR>
       */
			LOG.error("CLICommand not found: " + input.getCommandName());
			return null;
		}
				
		CLICommand cliConcreteCommand = findConcreteCLICommand(cliCommand, input.getSubCommandNames());
		if ( cliConcreteCommand == null ) {
			return null;
		}
		
		Class<? extends CLIInterface> cliConcreteClass = cliConcreteCommand.getCommandClass();
		if ( cliConcreteClass != null ) {
			try {
				Constructor<? extends CLIInterface> ctor = cliConcreteClass.getConstructor(ProcessedInput.class, CLICommand.class);
				CLIInterface returnInterface = ctor.newInstance(input, cliConcreteCommand);
				return returnInterface;					
			} catch (Exception e) {
				throw new CLIProcessingException("Exception during CLIInterface instantiation", e);
			} 
		}
		return null;
	}

	/**
	 * Helper method to satisfy recursive search of concrete CLICommand
	 */	
	private CLICommand findConcreteCLICommand(CLICommand cliCommand, List<String> subcommandNames) throws CLIProcessingException {
		if ( cliCommand == null ) {
      /**
       * <MAPR_ERROR>
       * Message:CLICommand not found
       * Function:CLICommandFactory.findConcreteCLICommand()
       * Meaning:The command does not exist. 
       * Resolution:Check the command syntax and make sure the command is typed correctly.
       * </MAPR_ERROR>
       */
			LOG.error("CLICommand not found");
			return null;
		}
		
		CLICommand[] childCLICommands = cliCommand.getSubcommands();
		if ( childCLICommands != null && !subcommandNames.isEmpty() ) {
			for ( CLICommand childCommand : childCLICommands) {
				if ( childCommand.getCommandName().equalsIgnoreCase(subcommandNames.get(0))) {
					// let's explore further
					CLICommand cliChildCommand = findConcreteCLICommand(childCommand, subcommandNames.subList(1, subcommandNames.size()));
					if ( cliChildCommand != null ) {
						return cliChildCommand;
					}
				} 
			}
		} else {
			// if no subcommands were identified return current command
			return cliCommand;
		}
		// nothing was found
		return null;
	}
}
