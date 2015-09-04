package com.mapr.distiller.cli.base;

/**
 * CLI Framework Exception
 * @author yufeldman
 *
 */
public class CLIProcessingException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6996812210597443030L;

	public CLIProcessingException(String msg) {
		super(msg);
	}
	
	public CLIProcessingException(String msg, Throwable t) {
		super(msg, t);
	}
	
	public CLIProcessingException(Throwable t) {
		super(t);
	}
}
