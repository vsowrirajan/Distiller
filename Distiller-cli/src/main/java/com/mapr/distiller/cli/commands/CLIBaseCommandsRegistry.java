package com.mapr.distiller.cli.commands;

import com.mapr.distiller.cli.base.CLICommand;
import com.mapr.distiller.cli.base.CLICommandRegistry;
import com.mapr.distiller.cli.base.CLIRegistryInterface;
import com.mapr.distiller.cli.base.CLICommand.ExecutionTypeEnum;

/**
 * Local registry to keep commands local to CLIFramework - so far we have only one which is SSH based
 * @author yufeldman
 *
 */
public class CLIBaseCommandsRegistry implements CLIRegistryInterface {

	public static final CLICommand sshCommand = new CLICommand("ssh", "ssh <script_name>", BaseSSHCommand.class, ExecutionTypeEnum.SSH, null, null).setUsageInVisible(true).setShortUsage("ssh <script_name>");

	private static CLIBaseCommandsRegistry s_instance = new CLIBaseCommandsRegistry();
	
	private CLIBaseCommandsRegistry() {
		
	}
	
	public static CLIBaseCommandsRegistry getInstance() {
		return s_instance;
	}
	
	public void register() {
		CLICommandRegistry.getInstance().register(sshCommand);
	}	
}
