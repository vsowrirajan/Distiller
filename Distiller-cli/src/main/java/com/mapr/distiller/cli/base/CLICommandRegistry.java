package com.mapr.distiller.cli.base;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry of all commands that were defined during runtime
 */
public class CLICommandRegistry {
	private static Map<String, CLICommand> commandsRegistry = new HashMap<String, CLICommand>();

	private static CLICommandRegistry s_instance = new CLICommandRegistry();
	
	private CLICommandRegistry() {
	}

	public static CLICommandRegistry getInstance() {
		return s_instance;
	}
	
	public CLICommand getCLICommand(String commandName) {
		return commandsRegistry.get(commandName);
	}
	
	public Map<String, CLICommand> getAllCommands() {
		return Collections.unmodifiableMap(commandsRegistry);
	}
	
	/**
	 * Registration of local commands comes here runtime
	 * @param cliCommand
	 */
	public synchronized void register(CLICommand cliCommand) {
		commandsRegistry.put(cliCommand.getCommandName(), cliCommand);
	}
}
