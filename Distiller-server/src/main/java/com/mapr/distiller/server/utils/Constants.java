package com.mapr.distiller.server.utils;

public interface Constants {
	// Allowable configuration parameter names in the configuration file
	public static final String INPUT_QUEUE_NAME	 						= "input.queue.name";
	public static final String INPUT_RECORD_PROCESSOR_METHOD		 	= "input.record.processor.method";
	public static final String INPUT_RECORD_PROCESSOR_NAME	 			= "input.record.processor.name";
	public static final String INPUT_RECORD_SELECTOR	 				= "input.record.selector";
	public static final String TIME_SELECTOR_MIN_DELTA					= "time.selector.min.delta";
	public static final String TIME_SELECTOR_MAX_DELTA					= "time.selector.max.delta";
	public static final String METRIC_ACTION_STATUS_RECORD_FREQUENCY	= "metric.action.status.record.frequency";
	public static final String METRIC_ACTION_STATUS_RECORDS_ENABLED	 	= "metric.action.status.records.enabled";
	public static final String METRIC_DESCRIPTION	 					= "metric.description";
	public static final String METRIC_NAME	 							= "metric.name";
	public static final String OUTPUT_QUEUE_CAPACITY_RECORDS	 		= "output.queue.capacity.records";
	public static final String OUTPUT_QUEUE_CAPACITY_SECONDS	 		= "output.queue.capacity.seconds";
	public static final String OUTPUT_QUEUE_MAX_PRODUCERS	 			= "output.queue.max.producers";
	public static final String OUTPUT_QUEUE_NAME	 					= "output.queue.name";
	public static final String PERIODICITY_MS	 						= "periodicity.ms";
	public static final String PROC_RECORD_PRODUCER_METRIC_NAME	 		= "proc.record.producer.metric.name";
	public static final String RAW_PRODUCER_METRICS_ENABLED	 			= "raw.producer.metrics.enabled";
	public static final String RAW_RECORD_PRODUCER_NAME	 				= "raw.record.producer.name";
	public static final String RECORD_TYPE	 							= "record.type";
	public static final String THRESHOLD_KEY	 						= "threshold.key";
	public static final String THRESHOLD_VALUE	 						= "threshold.value";
	
	// Allowable values for input.record.processor.method
	public static final String MERGE_RECORDS 	= "merge";
	public static final String IS_BELOW 		= "isBelow";
	public static final String IS_ABOVE 		= "isAbove";
	public static final String IS_EQUAL 		= "isEqual";
	public static final String IS_NOT_EQUAL 	= "isNotEqual";
	
	// Allowable values for input.record.selector
	public static final String SEQUENTIAL_SELECTOR		= "sequential";
	public static final String CUMULATIVE_SELECTOR		= "cumulative";
	public static final String TIME_SELECTOR			= "time";
	
	// Allowable values for record.type
	public static final String SYSTEM_CPU_RECORD 				= "SystemCpu";
	public static final String SYSTEM_MEMORY_RECORD 			= "SystemMemory";
	public static final String TCP_CONNECTION_RECORD 			= "TcpConnectionStat";
	public static final String THREAD_RESOURCE_RECORD 			= "ThreadResource";
	public static final String SLIM_THREAD_RESOURCE_RECORD 		= "SlimThreadResource";
	public static final String DISK_STAT_RECORD 				= "Diskstat";
	public static final String NETWORK_INTERFACE_RECORD 		= "NetworkInterface";
	public static final String PROCESS_RESOURCE_RECORD 			= "ProcessResource";
	public static final String SLIM_PROCESS_RESOURCE_RECORD 	= "SlimProcessResource";
	public static final String MFS_GUTS_RECORD 					= "MfsGuts";
	public static final String PROC_RECORD_PRODUCER_RECORD 		= "ProcRecordProducer";
	public static final String MFS_GUTS_RECORD_PRODUCER_RECORD 	= "MfsGutsRecordProducer";
	public static final String RAW_RECORD_PRODUCER_STAT_RECORD 	= "RawRecordProducerStat";
	
	// Allowable values for input.record.processor.name
	public static final String DISKSTAT_RECORD_PROCESSOR 				= "DiskstatRecordProcessor";
	public static final String MFS_GUTS_RECORD_PROCESSOR 				= "MfsGutsRecordProcessor";
	public static final String NETWORK_INTERFACE_RECORD_PROCESSOR 		= "NetworkInterfaceRecordProcessor";
	public static final String PROCESS_RESOURCE_RECORD_PROCESSOR 		= "ProcessResourceRecordProcessor";
	public static final String SYSTEM_CPU_RECORD_PROCESSOR 				= "SystemCpuRecordProcessor";
	public static final String SYSTEM_MEMORY_RECORD_PROCESSOR 			= "SystemMemoryRecordProcessor";
	public static final String TCP_CONNECTION_STAT_RECORD_PROCESSOR 	= "TcpConnectionStatRecordProcessor";
	public static final String THREAD_RESOURCE_RECORD_PROCESSOR 		= "ThreadResourceRecordProcessor";
	public static final String SLIM_PROCESS_RESOURCE_RECORD_PROCESSOR 	= "SlimProcessResourceRecordProcessor";
	public static final String SLIM_THREAD_RESOURCE_RECORD_PROCESSOR 	= "SlimThreadResourceRecordProcessor";
	
	// Raw record producer names (e.g. names that will show up as producers in output record queues)
	public static final String PROC_RECORD_PRODUCER_NAME 		= "ProcRecordProducer";
	public static final String MFS_GUTS_RECORD_PRODUCER_NAME 	= "MfsGutsRecordProducer";
	
	//Parameters for the internal metrics from raw record producers
	public static final String RAW_PRODUCER_STATS_QUEUE_NAME 			= "Raw Producer Stats";
	public static final int RAW_PRODUCER_STATS_QUEUE_RECORD_CAPACITY 	= 1000;
	public static final int RAW_PRODUCER_STATS_QUEUE_TIME_CAPACITY 		= 600;
	
	//Parameters for the internal metrics from MetricAction objects
	public static final String METRIC_ACTION_STATS_QUEUE_NAME 			= "MetricAction Stats";
	public static final int METRIC_ACTION_STATS_QUEUE_RECORD_CAPACITY 	= 1000;
	public static final int METRIC_ACTION_STATS_QUEUE_TIME_CAPACITY 	= 600;

}
