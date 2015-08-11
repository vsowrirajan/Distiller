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

	// All record type names goes here, procrecordproducer and mfsgutsrecordproducer
	public static final String SYSTEM_CPU_RECORD = "SystemCpu";
	public static final String SYSTEM_MEMORY_RECORD = "SystemMemory";
	public static final String TCP_CONNECTION_RECORD = "TcpConneciton";
	public static final String THREAD_RESOURCE_RECORD = "ThreadResource";
	public static final String DISK_STAT_RECORD = "Diskstat";
	public static final String NETWORK_INTERFACE_RECORD = "NetworkInterface";
	public static final String PROCESS_RESOURCE_RECORD = "ProcessResource";
	public static final String MFS_GUTS_RECORD = "MfsGuts";
	public static final String PROC_RECORD_PRODUCER_RECORD = "ProcRecordProducer";
	public static final String MFS_GUTS_RECORD_PRODUCER_RECORD = "MfsGutsRecordProducer";
	public static final String RAW_RECORD_PRODUCER_STAT_RECORD = "RawRecordProducerStat";
	
	// RecordProcessor names
	public static final String DISKSTAT_RECORD_PROCESSOR = "DiskstatRecordProcessor";
	public static final String MFS_GUTS_RECORD_PROCESSOR = "MfsGutsRecordProcessor";
	public static final String NETWORK_INTERFACE_RECORD_PROCESSOR = "NetworkInterfaceRecordProcessor";
	public static final String PROCESS_RESOURCE_RECORD_PROCESSOR = "ProcessResourceRecordProcessor";
	public static final String SYSTEM_CPU_RECORD_PROCESSOR = "SystemCpuRecordProcessor";
	public static final String SYSTEM_MEMORY_RECORD_PROCESSOR = "SystemMemoryRecordProcessor";
	public static final String TCP_CONNECTION_STAT_RECORD_PROCESSOR = "TcpConnectionStatRecordProcessor";
	public static final String THREAD_RESOURCE_RECORD_PROCESSOR = "ThreadResourceRecordProcessor";
	public static final String SLIM_PROCESS_RESOURCE_RECORD_PROCESSOR = "SlimProcessResourceRecordProcessor";
	public static final String SLIM_THREAD_RESOURCE_RECORD_PROCESSOR = "SlimThreadResourceRecordProcessor";
	
	// Raw record producer names (e.g. names that will show up as producers in output record queues)
	public static final String PROC_RECORD_PRODUCER_NAME = "ProcRecordProducer";
	public static final String MFS_GUTS_RECORD_PRODUCER_NAME = "MfsGutsRecordProducer";
	
	//Parameters for the internal metrics from raw record producers
	public static final String RAW_PRODUCER_STATS_QUEUE_NAME = "Raw Producer Stats";
	public static final int RAW_PRODUCER_STATS_QUEUE_RECORD_CAPACITY = 1000;
	public static final int RAW_PRODUCER_STATS_QUEUE_TIME_CAPACITY = 600;
	
	//Parameters for the internal metrics from MetricAction objects
	public static final String METRIC_ACTION_STATS_QUEUE_NAME = "MetricAction Stats";
	public static final int METRIC_ACTION_STATS_QUEUE_RECORD_CAPACITY = 1000;
	public static final int METRIC_ACTION_STATS_QUEUE_TIME_CAPACITY = 600;

}
