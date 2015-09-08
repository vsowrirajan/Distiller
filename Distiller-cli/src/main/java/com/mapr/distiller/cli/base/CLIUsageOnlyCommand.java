package com.mapr.distiller.cli.base;

/**
 * Helper command to define when there is no real implementation as it belongs to child commands, but need to have usage separately
 */
public class CLIUsageOnlyCommand extends CLIBaseClass implements CLIInterface {

	public CLIUsageOnlyCommand(ProcessedInput input, CLICommand cliCommand) {
		super(input, cliCommand);
	}

	@Override
	public CommandOutput executeRealCommand() throws CLIProcessingException {
		return new TextCommandOutput(getCommandUsage());
	}

	@Override
	public String getCommandUsage() {
		if ( cliCommand != null ) {
			return cliCommand.getUsageFromParametersOfCommandsTree();
		}
		return null;
	}

}
