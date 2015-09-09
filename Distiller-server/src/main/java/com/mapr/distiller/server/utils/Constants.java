package com.mapr.distiller.server.utils;

public interface Constants {
	// Allowable configuration parameter names in the configuration file
	public static final String INPUT_QUEUE_NAME	 						= "input.queue.name";
	public static final String INPUT_QUEUE_TYPE	 						= "input.queue.type";
	public static final String INPUT_RECORD_PROCESSOR_METHOD		 	= "input.record.processor.method";
	public static final String INPUT_RECORD_PROCESSOR_NAME	 			= "input.record.processor.name";
	public static final String INPUT_RECORD_SELECTOR	 				= "input.record.selector";
	public static final String TIME_SELECTOR_MIN_DELTA					= "time.selector.min.delta";
	public static final String TIME_SELECTOR_MAX_DELTA					= "time.selector.max.delta";
	public static final String MAPRDB_CREATE_TABLES						= "maprdb.create.tables";
	public static final String MAPRDB_INPUT_QUEUE_SCANNER				= "maprdb.input.queue.scanner";
	public static final String MAPRDB_INPUT_QUEUE_SCAN_START_TIME 		= "maprdb.input.queue.scan.start.time";
	public static final String MAPRDB_INPUT_QUEUE_SCAN_END_TIME			= "maprdb.input.queue.scan.end.time";
	public static final String MAPRDB_PUT_TIMEOUT						= "maprdb.put.timeout";
	public static final String MAPRDB_LOCAL_WORK_DIR_PATH				= "maprdb.local.work.dir.path";
	public static final String MAPRDB_LOCAL_WORK_DIR_BYTE_LIMIT			= "maprdb.local.work.dir.byte.limit";
	public static final String MAPRDB_ENABLE_WORK_DIR					= "maprdb.enable.work.dir";
	public static final String MAPRDB_WORK_DIR_BATCH_SIZE				= "maprdb.work.dir.batch.size";
	public static final String LFSP_OUTPUT_DIR							= "lfsp.output.dir";
	public static final String LFSP_MAX_OUTPUT_DIR_SIZE					= "lfsp.max.output.dir.size";
	public static final String LFSP_WRITE_BATCH_SIZE					= "lfsp.write.batch.size";
	public static final String LFSP_FLUSH_FREQUENCY						= "lfsp.flush.frequency";
	public static final String LFSP_RECORDS_PER_FILE					= "lfsp.records.per.file";
	public static final String LOCAL_FILE_INPUT_QUEUE_SCANNER			= "local.file.input.queue.scanner";
	public static final String LOCAL_FILE_INPUT_QUEUE_START_TIMESTAMP	= "local.file.input.queue.start.timestamp";
	public static final String LOCAL_FILE_INPUT_QUEUE_END_TIMESTAMP		= "local.file.input.queue.end.timestamp";
	public static final String LOCAL_FILE_INPUT_METRIC_NAME				= "local.file.input.metric.name";
	public static final String METRIC_ACTION_STATUS_RECORD_FREQUENCY	= "metric.action.status.record.frequency";
	public static final String METRIC_ACTION_STATUS_RECORDS_ENABLED	 	= "metric.action.status.records.enabled";
	public static final String METRIC_DESCRIPTION	 					= "metric.description";
	public static final String METRIC_NAME	 							= "metric.name";
	public static final String METRIC_ENABLED 							= "metric.enabled";
	public static final String OUTPUT_QUEUE_CAPACITY_RECORDS	 		= "output.queue.capacity.records";
	public static final String OUTPUT_QUEUE_CAPACITY_SECONDS	 		= "output.queue.capacity.seconds";
	public static final String OUTPUT_QUEUE_MAX_PRODUCERS	 			= "output.queue.max.producers";
	public static final String OUTPUT_QUEUE_NAME	 					= "output.queue.name";
	public static final String OUTPUT_QUEUE_TYPE	 					= "output.queue.type";
	public static final String PERIODICITY_MS	 						= "periodicity.ms";
	public static final String PERSISTOR_NAME							= "persistor.name";
	public static final String PROC_RECORD_PRODUCER_METRIC_NAME	 		= "proc.record.producer.metric.name";
	public static final String RAW_PRODUCER_METRICS_ENABLED	 			= "raw.producer.metrics.enabled";
	public static final String RAW_RECORD_PRODUCER_NAME	 				= "raw.record.producer.name";
	public static final String RECORD_TYPE	 							= "record.type";
	public static final String RELATED_OUTPUT_QUEUE_CAPACITY_RECORDS	= "related.output.queue.record.capacity";
	public static final String RELATED_OUTPUT_QUEUE_CAPACITY_SECONDS	= "related.output.queue.second.capacity";
	public static final String RELATED_OUTPUT_QUEUE_MAX_PRODUCERS		= "related.output.queue.max.producers";
	public static final String RELATED_SELECTOR_ENABLED					= "related.selector.enabled";
	public static final String THRESHOLD_KEY	 						= "threshold.key";
	public static final String THRESHOLD_VALUE	 						= "threshold.value";
	public static final String SELECTOR_QUALIFIER_KEY					= "selector.qualifier.key";
	public static final String SELECTOR_QUALIFIER_VALUE					= "selector.qualifier.value";
	public static final String SELECTOR_RELATED_INPUT_QUEUE_NAME		= "selector.related.input.queue.name";
	public static final String SELECTOR_RELATED_OUTPUT_QUEUE_NAME		= "selector.related.output.queue.name";
	public static final String SELECTOR_RELATED_NAME					= "selector.related.name";
	public static final String SELECTOR_RELATED_METHOD					= "selector.related.method";
	public static final String SELECTOR_CUMULATIVE_FLUSH_TIME			= "selector.cumulative.flush.time";
	public static final String UPDATING_SUBSCRIPTION_QUEUE_KEY			= "updating.subscription.queue.key";
	public static final String PRP_GENERATE_JAVA_STACK_TRACES			= "prp.generate.java.stack.traces";
	
	//Allowable values for maprdb.input.queue.scanner
	public static final String TIMESTAMP_SCANNER						= "scanByTimestamp";
	public static final String PREVIOUS_TIMESTAMP_SCANNER				= "scanByPreviousTimestamp";
	
	// Allowable values for persistor.name
	public static final String MAPRDB									= "MapRDB";
	public static final String LOCAL_FILE_SYSTEM						= "LocalFileSystem";
	
	// Allowable values for selector.related.method
	public static final String TIME_BASED_WINDOW						= "timeBasedWindow";
	
	// Allowable values for selector.related.name
	public static final String BASIC_RELATED_RECORD_SELECTOR			= "BasicRelatedRecordSelector";
	
	// Allowable values for output.queue.type
	public static final String SUBSCRIPTION_RECORD_QUEUE				= "SubscriptionRecordQueue";
	public static final String UPDATING_SUBSCRIPTION_RECORD_QUEUE		= "UpdatingSubscriptionRecordQueue";
	
	// Allowable values for input.queue.type
	public static final String LOCAL_FILE_INPUT_RECORD_QUEUE			= "LocalFileInputRecordQueue";
	public static final String MAPRDB_INPUT_RECORD_QUEUE				= "MapRDBInputRecordQueue";
	public static final String DEFAULT_INPUT_RECORD_QUEUE				= "DefaultInputRecordQueue";
	

	// Allowable values for input.record.processor.method
	public static final String MERGE_RECORDS 						= "merge";
	public static final String IS_BELOW 							= "isBelow";
	public static final String IS_ABOVE 							= "isAbove";
	public static final String IS_EQUAL 							= "isEqual";
	public static final String IS_NOT_EQUAL 						= "isNotEqual";
	public static final String DIFFERENTIAL 						= "differentialValue";
	public static final String CONVERT								= "convert";
	public static final String MERGE_CHRONOLOGICALLY_CONSECUTIVE 	= 	"mergeChronologicallyConsecutive";

	
	// Allowable values for input.record.selector
	public static final String SEQUENTIAL_SELECTOR					= "sequential";
	public static final String SEQUENTIAL_WITH_QUALIFIER_SELECTOR 	= "sequentialWithQualifier";
	public static final String CUMULATIVE_SELECTOR					= "cumulative";
	public static final String CUMULATIVE_WITH_QUALIFIER_SELECTOR 	= "cumulativeWithQualifier";
	public static final String TIME_SELECTOR						= "time";
	public static final String PERSISTING_SELECTOR					= "persist";
	
	// Allowable values for record.type
	public static final String SYSTEM_CPU_RECORD 					= "SystemCpu";
	public static final String SYSTEM_MEMORY_RECORD 				= "SystemMemory";
	public static final String TCP_CONNECTION_RECORD 				= "TcpConnectionStat";
	public static final String THREAD_RESOURCE_RECORD 				= "ThreadResource";
	public static final String SLIM_THREAD_RESOURCE_RECORD 			= "SlimThreadResource";
	public static final String DISK_STAT_RECORD 					= "Diskstat";
	public static final String NETWORK_INTERFACE_RECORD 			= "NetworkInterface";
	public static final String PROCESS_RESOURCE_RECORD 				= "ProcessResource";
	public static final String SLIM_PROCESS_RESOURCE_RECORD 		= "SlimProcessResource";
	public static final String MFS_GUTS_RECORD 						= "MfsGuts";
	public static final String PROC_RECORD_PRODUCER_RECORD 			= "ProcRecordProducer";
	public static final String MFS_GUTS_RECORD_PRODUCER_RECORD 		= "MfsGutsRecordProducer";
	public static final String RAW_RECORD_PRODUCER_STAT_RECORD 		= "RawRecordProducerStat";
	public static final String DIFFERENTIAL_VALUE_RECORD	 		= "DifferentialValue";
	public static final String METRIC_ACTION_STATUS_RECORD			= "MetricActionStatus";
	public static final String MAPRDB_PERSISTED_META_INFO_RECORD	= "MapRDBPersistedMetaInfo";
	public static final String LOAD_AVERAGE_RECORD					= "LoadAverage";
	public static final String PROCESS_EFFICIENCY_RECORD			= "ProcessEfficiency";
	public static final String RECORD								= "Record";
	
	// serialVersionUID values for each Record type
	// Pay attention:
	// http://docs.oracle.com/javase/7/docs/platform/serialization/spec/version.html#6678
	public static final long DATA_MODEL_VERSION							= 0;
	public static final long SVUID_BASE									= 1835102322l + ( 65536l * DATA_MODEL_VERSION );
	public static final long SVUID_SYSTEM_CPU_RECORD 					= SVUID_BASE + 1;
	public static final long SVUID_SYSTEM_MEMORY_RECORD 				= SVUID_BASE + 2;
	public static final long SVUID_TCP_CONNECTION_RECORD 				= SVUID_BASE + 3;
	public static final long SVUID_THREAD_RESOURCE_RECORD 				= SVUID_BASE + 4;
	public static final long SVUID_SLIM_THREAD_RESOURCE_RECORD 			= SVUID_BASE + 5;
	public static final long SVUID_DISK_STAT_RECORD 					= SVUID_BASE + 6;
	public static final long SVUID_NETWORK_INTERFACE_RECORD 			= SVUID_BASE + 7;
	public static final long SVUID_PROCESS_RESOURCE_RECORD 				= SVUID_BASE + 8;
	public static final long SVUID_SLIM_PROCESS_RESOURCE_RECORD 		= SVUID_BASE + 9;
	public static final long SVUID_MFS_GUTS_RECORD 						= SVUID_BASE + 10;
	public static final long SVUID_PROC_RECORD_PRODUCER_RECORD 			= SVUID_BASE + 11;
	public static final long SVUID_MFS_GUTS_RECORD_PRODUCER_RECORD 		= SVUID_BASE + 12;
	public static final long SVUID_RAW_RECORD_PRODUCER_STAT_RECORD 		= SVUID_BASE + 13;
	public static final long SVUID_DIFFERENTIAL_VALUE_RECORD	 		= SVUID_BASE + 14;
	public static final long SVUID_METRIC_ACTION_STATUS_RECORD			= SVUID_BASE + 15;
	public static final long SVUID_RECORD								= SVUID_BASE + 16;
	public static final long SVUID_MAPRDB_PERSISTED_META_INFO_RECORD	= SVUID_BASE + 17;
	public static final long SVUID_MAPRDB_COORDINATOR_META_RECORD		= SVUID_BASE + 18;
	public static final long SVUID_MAPRDB_DISK_BUFFERED_RECORD_META_INFO= SVUID_BASE + 19;
	public static final long SVUID_LOAD_AVERAGE_RECORD					= SVUID_BASE + 20;
	public static final long SVUID_PROCESS_EFFICIENCY_RECORD			= SVUID_BASE + 21;
	
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
	public static final String DIFFERENTIAL_VALUE_RECORD_PROCESSOR 		= "DifferentialValueRecordProcessor";
	public static final String PASSTHROUGH_RECORD_PROCESSOR 			= "PassthroughRecordProcessor";
	public static final String LOAD_AVERAGE_RECORD_PROCESSOR			= "LoadAverageRecordProcessor";
	public static final String PROCESS_EFFICIENCY_RECORD_PROCESSOR		= "ProcessEfficiencyRecordProcessor";
	
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
