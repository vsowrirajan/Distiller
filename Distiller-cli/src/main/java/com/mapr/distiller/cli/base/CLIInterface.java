package com.mapr.distiller.cli.base;

import java.util.Set;

/**
 * Base CLI Interface to execute any command
 */
public interface CLIInterface {

  public final static String PASSWORD_PREFIX = "passw";
  
	/**
	 * Method to validate input according to particular command specification
	 * @return true if input is valid, false otherwise
	 */
	public boolean validateInput() throws IllegalArgumentException;
	
	/**
	 * Main method to execute Command
	 * @return CommandOutput object to be used by clients of this to display output 
	 */
	public CommandOutput executeCommand() throws CLIProcessingException;	
	
	/**
	 * get CLI command for the interface
	 * @return CLICommand corresponding to the interface
	 */
	public CLICommand getCLICommand();
	
	/**
	 * Method that allows to set uid, gids from loginusername
	 * @param userId
	 */
	public void setUserCredentials(String userId) throws CLIProcessingException;
	
	/**
	 * Method to get uid of current user
	 * @return
	 */
	public long getUserId();
	
	/**
	 * Method to get user login Id of current user
	 * @return
	 */
	public String getUserLoginId();
	
	/**
	 * Method to get gids of current user
	 * @return
	 */
	public Set<Long> getGIds();
	
}
