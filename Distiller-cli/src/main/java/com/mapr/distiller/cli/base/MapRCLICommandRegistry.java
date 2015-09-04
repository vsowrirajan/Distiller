package com.mapr.distiller.cli.base;


public class MapRCLICommandRegistry implements CLIRegistryInterface {
	
	private static MapRCLICommandRegistry s_instance = 
	                                      new MapRCLICommandRegistry();
	
	private MapRCLICommandRegistry() {
	}
	
	public static MapRCLICommandRegistry getInstance() {
		return s_instance;
	}
	
	
	@Override
	public void register() {
	}	

}
