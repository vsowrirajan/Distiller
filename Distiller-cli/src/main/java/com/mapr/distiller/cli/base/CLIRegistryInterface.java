package com.mapr.distiller.cli.base;

/**
 * Common interface to register local commands into global CLICommandRegistry
 * @author yufeldman
 *
 */
public interface CLIRegistryInterface {

	/**
	 * Register local command into base CLICommandRegistry
	 */
	public void register();
}
