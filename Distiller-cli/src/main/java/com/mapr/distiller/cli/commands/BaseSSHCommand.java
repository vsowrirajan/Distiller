package com.mapr.distiller.cli.commands;


import com.mapr.distiller.cli.base.common.Errno;
import com.mapr.distiller.cli.base.CLIBaseClass;
import com.mapr.distiller.cli.base.CLICommand;
import com.mapr.distiller.cli.base.CLIInterface;
import com.mapr.distiller.cli.base.CLIProcessingException;
import com.mapr.distiller.cli.base.CommandOutput;
import com.mapr.distiller.cli.base.ProcessedInput;
import com.mapr.distiller.cli.base.TextCommandOutput;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputError;


/**
 * Base ssh command execution logic, can be overriden if needed by children
 * @author yufeldman
 *
 */
public class BaseSSHCommand extends CLIBaseClass implements CLIInterface {

	public static long TIMEOUT = 3600000L;
	

	public BaseSSHCommand(ProcessedInput input, CLICommand cliCommand) {
		super(input, cliCommand);
	}

	@Override
	public CommandOutput executeRealCommand() throws CLIProcessingException {
		try {
			byte [] output = executeSimpleSHHCommand(TIMEOUT);
			return new TextCommandOutput(output);
		} catch( CLIProcessingException ex) {
			return new TextCommandOutput(ex.getMessage().getBytes());
		}
	}

	@Override
	public boolean validateInput() throws IllegalArgumentException {
		if (input.getSubCommandNames().isEmpty()) {
			output.getOutput().addError(new OutputError(Errno.EINVAL,"No ssh command is provided"));
			return false;
		}
		return true;
	}

	@Override
	public String getCommandUsage() {
		return "Base SSH Command: " + cliCommand.getParameters().values().toString();
	}
}
