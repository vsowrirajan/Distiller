package com.mapr.distiller.server.recordtypes;

import com.mapr.distiller.server.utils.Constants;

public class MapRDBMetricMetaRecord extends Record
{
	//For serialization
	private static final long serialVersionUID = Constants.SVUID_MAPRDB_PERSISTED_META_INFO_RECORD;
	
	//These Records should be inserted into a table using a row key of:
	// <metricId>/<coordinatorId>/t<timestamp>/p<previousTimestamp>
	//And:
	// <metricId>/<coordinatorId>/p<previousTimestamp>/t<timestamp>
	//ID of parent process for this Object (coordinatorId) and ID of the metric for which status is being reported.
	//String coordinatorId;			//The ID of the Coordinator instance that generated the MetricAction that generated this Record
	//String metricId;				//The ID of the MetricAction 
	String tablePath;				//Table path where Records were put
	
	/**
	 * Counters for work done by the MapRDBPersistor
	 * 
	 * recordsPut 	- 	Number of times a success Callback was called for an asynchronous Put
	 * putFailures 	- 	Number of times a failure Callback was called for an asynchronous Put
	 * putTimeouts  -	Number of times an inflight asynchronous Put was cancelled by the MapRDBPersistor 
	 * 					because no Callback was called within maprdb.async.put.timeout ms of Put creation.
	 * 					When maprdb.enable.work.dir is enabled, these Puts will be written to disk
	 * inflightPuts - 	The size of the queue holding Deferred objects for inflight Puts
	 * replayedPuts - 	The number of async Put requests issued to the HBaseClient for Records read back from the local OS work dir
	 * svcTime		- 	The aggregated number of milliseconds that Deferred objects spent in the queue.  
	 * 					This can be used to calculate average service time.
	 * 					This counter is incremented for all Puts, regardless of whether those Puts succeed, fail or timeout, 
	 * 					and regardless of whether the Puts are the first attempt or replayedom from the work dir.

	 */
	int recordsPut;
	int putFailures;
	int putTimeouts;
	int inflighPuts;
	int replayedPuts;
	long svcTime;

}
