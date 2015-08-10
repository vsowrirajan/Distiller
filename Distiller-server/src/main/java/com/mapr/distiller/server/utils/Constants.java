package com.mapr.distiller.server.utils;

//Everything here is in lower case, so it is better to call lower case on every string and check if they are equals to the another string.

//Constants of Distiller
public interface Constants {
	// All processing type names goes here
	public static final String MOVING_AVERAGE = "movingaverage";
	public static final String IS_BELOW = "isbelow";
	public static final String IS_ABOVE = "isabove";
	public static final String IS_EQUAL = "isequal";
	public static final String IS_NOT_EQUAL = "isnotequal";
	public static final String THRESHOLD = "threshold";

	// All record type names goes here
	public static final String SYSTEMCPURECORD = "systemcpurecord";
	public static final String SYSTEMMEMORYRECORD = "systemmemoryrecord";
	public static final String TCPCONNECTIONRECORD = "tcpconnectionrecord";
	public static final String THREADRESOURCERECORD = "threadresourcerecord";
	public static final String DISKSTATRECORD = "diskstatrecord";
	public static final String NETWORKINTERFACERECORD = "networkinterfacerecord";
	public static final String PROCESSRESOURCERECORD = "processresourcerecord";

	public static final String MFSGUTSRECORD = "mfsgutsrecord";

}
