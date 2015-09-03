package com.mapr.distiller.server.persistance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;

import java.io.ObjectOutput;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.File;

import com.mapr.distiller.server.persistance.MapRDBPersistor;
import com.mapr.distiller.server.recordtypes.Record;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.Callback;

public class MapRDBPersistanceManager extends Thread{
	private static final Logger LOG = LoggerFactory.getLogger(MapRDBPersistanceManager.class);

	//The path to the table that MapRDBPersistanceManager will use to persist meta info about metrics
	//If this table doesn't exist 
	public static final String DISTILLER_META_TABLE_PATH				= "/var/mapr/cluster/distiller/.meta";
	public static final String DISTILLER_META_COLUMN_FAMILY				= "m";
	private byte[] distillerMetaTablePathBytes							= DISTILLER_META_TABLE_PATH.getBytes();
	private byte[] distillerMetaColumnFamilyBytes						= DISTILLER_META_COLUMN_FAMILY.getBytes();
		
	//Used to specify the column family and qualifier for Put operations for Records
	public static final String RECORD_BYTES_COLUMN_FAMILY	 	= "Record bytes";
	public static final String RECORD_BYTES_COLUMN_QUALIFIER	= "r";
	private byte[] columnFamilyBytes = RECORD_BYTES_COLUMN_FAMILY.getBytes();
    private byte[] columnQualifierBytes = RECORD_BYTES_COLUMN_QUALIFIER.getBytes();
    
    //List of all the MapRDBPersistors that this manages
    private List<MapRDBPersistor> persistors;
    
    //Maps to indicate whether a Persistor is checking for it's table/column family and the last time at which a check was started
    private Map<MapRDBPersistor, Boolean> persistorTableCheckCompleteMap;
    private Map<MapRDBPersistor, Long> persistorTableCheckStartTimeMap;
    
    //Counters...
  	//First we serialize the records...
  	private long serializationFailures;
  	//Then we try to persist (e.g. HBaseClient.put)..
  	private long persistedRecords;
  	private long persistanceFailures;
  	//If the persist attempt fails, the record is buffered to disk for later replay or dropped entirely
  	private long diskBufferedRecords;
  	private long droppedRecords;
  	//And to count the number of times we unfortunately need to reconstruct the HBaseClient object
  	private long hbaseClientInitCount;
  	
  	//Use this lock anytime HBaseClient needs to be checked/changed and anytime the list of persistors needs to be checked/changed
  	private Object lock = new Object();
  	
  	//To be used to issue Put requests to MapRDB
  	private HBaseClient hbaseClient;
  	
  	//In conjunction, the following three objects are used to decide when to reinit HBaseClient based on configured Put timeout
	//Map of timestamps at which Puts were issued to HBaseClient to the Deferred returned by HBaseClient
	private Map<Long, List<Deferred<Object>>> deferredPutTimestampMap;
  	//To hold the time at which a Deferred callback for a successful put was called most recently
  	private long lastSuccessfulPutTime;
    //The minimum elapsed time from submission of the oldest put to now and from completion of the most recent put to now at which the HBaseClient instance will be reinitialized
  	private int putTimeout;
  	
	//Used to uniquely identify an instance of the Coordinator application/MapRDBPersistanceManager
    private int pid;
    private long startTime;
    private String hostname;
    
    //Used to control where Records are flushed to disk for buffering when the put timeout is exceeded and how much space can be consumed
    private long maprdbLocalWorkDirByteLimit;
	private String maprdbLocalWorkDirPath;
	
	//The cluster that should be used to initialize the HBaseClient instance.  This should generally be "maprfs:///"
	private String clusterPath;
	
	//Use this to track the last time we attempted to initHBaseClient()
	private long lastInitTime;
	
	//Minimum amount of time required to elapse between calls to initHbaseClient()
	private long minInitFrequency = 10000;
	
	//Used to indicate to the MapRDBPersistanceManager thread that it should shut down
	private boolean shouldExit;
	
	//The most recent time at which we logged an error about serialization failure.
	//Track this so we don't flood the log, will only print it 1 time per minute
	private long lastSerializationFailureStatusTime;
	//The number of serialization failures since last status;
	private int numSerializationFailuresSinceLastStatus;
	
	//The most recent time at which we logged an error about a put failure
	//Track this so we don't flood the log, will only print it 1 time per minute
	private long lastPutFailureStatusTime;
	//The number of serialization failures since last status;
	private int numPutFailuresSinceLastStatus;

	private int numDiskBufferedFiles;

	public MapRDBPersistanceManager(String clusterPath, int putTimeout, int pid, long startTime, String hostname,
									long maprdbLocalWorkDirByteLimit, String maprdbLocalWorkDirPath ) throws Exception{
		if(clusterPath == null || !clusterPath.startsWith("maprfs://")){
			throw new Exception("MapRDBMonitor-" + System.identityHashCode(this) + 
					": Failed to construct MapRDBMonitor due to invalid cluster path: " + 
					((clusterPath==null) ? "null" : "\"" + clusterPath + "\""));
		}
		if(putTimeout < 60000) {
			throw new Exception("MapRDBMonitor-" + System.identityHashCode(this) + 
					": Failed to construct MapRDBMonitor due to invalid put timeout < 60000, value: " + 
					putTimeout);
		}
		File testFile = new File(maprdbLocalWorkDirPath);
		boolean checkResult = false;
		try {
			checkResult = testFile.exists();
		} catch (Exception e) {
			throw new Exception("Failed to check whether MapRDB local work dir exists at path " + maprdbLocalWorkDirPath, e);
		}
		//Try to create it
		if(!checkResult){
			try {
				checkResult = testFile.mkdirs();
			} catch (Exception e) {
				throw new Exception("MapRDB local work dir does not exist and mkdirs threw exception for path " + maprdbLocalWorkDirPath, e);
			}
			if(!checkResult){
				throw new Exception("MapRDB local work dir does not exist and mkdirs failed for path " + maprdbLocalWorkDirPath);
			}
		}
		//Check that the work dir path is actually a directory
		checkResult = false;
		try {
			checkResult = testFile.isDirectory();
		} catch (Exception e) {
			throw new Exception("Caught exception while checking whether MapRDB local work dir path is a directory", e);
		}
		if(!checkResult){
			throw new Exception("Something exists at the MapRDB local work dir path but it is not a directory");
		}
		//Check that we can create and write to files in the work dir
		long now = System.currentTimeMillis();
		String testFilePath = maprdbLocalWorkDirPath + "/" + pid + "." + startTime + "." + hostname + "." + now;
		while (true) {
			testFile = new File(testFilePath);
			try {
				if(!testFile.exists()){
					break;
				}
				testFilePath = testFilePath + "_";
			} catch (Exception e) {
				throw new Exception("Caught exception while testing read/write access to MapRDB local work dir", e);
			}
		}
		
		checkResult = false;
		try {
			checkResult = testFile.createNewFile();
		} catch (Exception e) {
			throw new Exception("Caught exception while trying to create test file in MapRDB local work dir at path " + testFilePath, e);
		}
		if(!checkResult){
			throw new Exception("Failed to create test file in MapRDB local work dir at path " + testFilePath);
		}
		
		checkResult = false;
		try {
			checkResult = testFile.delete();
		} catch (Exception e) {
			throw new Exception("Caught an exception while trying to delete test file in MapRDB local work dir at path " + testFilePath, e);
		}
		if(!checkResult){
			throw new Exception("Failed to delete test file in MapRBD local work dir at path " + testFilePath);
		}

		this.persistors = new LinkedList<MapRDBPersistor>();
		this.persistorTableCheckCompleteMap = new HashMap<MapRDBPersistor, Boolean>();
		this.persistorTableCheckStartTimeMap = new HashMap<MapRDBPersistor, Long>();
		this.deferredPutTimestampMap = new TreeMap<Long, List<Deferred<Object>>>();
	  	this.serializationFailures = 0;
	  	this.persistedRecords = 0;
	  	this.persistanceFailures = 0;
	  	this.diskBufferedRecords = 0;
	  	this.droppedRecords = 0;
	  	this.hbaseClientInitCount = 0;
	  	this.lastSuccessfulPutTime = -1;
	  	this.shouldExit = false;
	  	this.hbaseClient = null;
		this.lastSerializationFailureStatusTime = 0;
		this.numSerializationFailuresSinceLastStatus = 0;
		this.lastPutFailureStatusTime = 0;
		this.numPutFailuresSinceLastStatus = 0;
		this.numDiskBufferedFiles = 0;

	  	
		this.clusterPath = clusterPath;
		this.putTimeout = putTimeout;
		this.pid = pid;
		this.startTime = startTime;
		this.hostname = hostname;
		this.maprdbLocalWorkDirByteLimit = maprdbLocalWorkDirByteLimit;
		this.maprdbLocalWorkDirPath = maprdbLocalWorkDirPath;
		initHBaseClient();
	}

	public boolean hasPersistor(MapRDBPersistor persistor) {
		synchronized(lock){
			return persistors.contains(persistor);
		}
	}
	//Call this each time a MapRDBPersistor is constructed and should be managed by this
	public void registerPersistor(MapRDBPersistor persistor) throws Exception{
		synchronized(lock){
			persistors.add(persistor);
			try {
				persistor.setMapRDBPersistanceManager(this);
			} catch (Exception e) {
				persistors.remove(persistor);
				throw new Exception("MapRDBPersistanceManager-" + System.identityHashCode(this) + ": Failed to set self as manager for MapRDBPersistor-" + System.identityHashCode(persistor), e);
			}
			persistorTableCheckCompleteMap.put(persistor,  new Boolean(false));
			persistorTableCheckStartTimeMap.put(persistor, new Long(-1));
			startTableFamilyCheck(persistor);
		}
	}
	//Call this to initialize HBaseClient
	//Also call this whenever we need to reinit the HBaseClient to handle timeouts
	public void initHBaseClient(){
		for(MapRDBPersistor persistor : persistors){
			synchronized(persistor){
				persistor.setTableFamilyExists(false);
			}
		}
		synchronized(lock){
			//Increment the counter number of inits
			//Do this first because Deferred callbacks for table family check RPCs will only update status of whether
			//the table/family exists if the value of the init counter at the time of the callback matches the value
			//form the time when the RPC was first created.
			hbaseClientInitCount++;
			//Shut down the HBaseClient since we are going to re-init it
			shutdownHBaseClient();
			//At this point, no more Put RPCs should complete from old HBaseClient so lets move all Records in Put maps to disk destined Record lists
			queueRecordsToDisk();
			//Attempt to reinit the HBase client
			try {
				hbaseClient = new HBaseClient(clusterPath);
				final CountDownLatch latch = new CountDownLatch(1);
				final AtomicBoolean res = new AtomicBoolean(false);
				hbaseClient.ensureTableFamilyExists(distillerMetaTablePathBytes, distillerMetaColumnFamilyBytes).addCallbacks(
			    		new Callback<Object, Object>() {
			    			@Override
			    			public Object call(Object arg) throws Exception {
			    				res.set(true);
			    				latch.countDown();
			    				return null;
			    			}
			   			},
			    		new Callback<Object, Object>() {
			    			@Override
			   				public Object call(Object arg) throws Exception {
			    				res.set(false);
			    				latch.countDown();
			    				return null;
			    			}
			   			}
			    );	
				while(!shouldExit && latch.getCount()!=0){
					try {
						latch.await();
					} catch (Exception e){};
				}
				if(!shouldExit){
					if(!res.get()){
						throw new Exception("Failed to find Distiller meta table at path " + DISTILLER_META_TABLE_PATH + 
											" with column family " + DISTILLER_META_COLUMN_FAMILY);
					}
				}
			} catch (Exception e) {
				LOG.error("MapRDBMonitor-" + System.identityHashCode(this) + 
						  ": Exception while trying to initialize HBaseClient", e);
				try {
					hbaseClient.shutdown();
				} catch (Exception e2){}
				hbaseClient = null;
			}
			//Check for required tables/column families for each registered MapRDBPersistor
			//If HBaseClient is null (e.g. reinit failed), this will set tableFamilyExists to false on all MapRDBPersistors
			//If HBaseClient is not null, this will check for the table/family and set tableFamilyExists to true is found
			//If HBaseClient is not null and the table/family is not found and the flag is set to create them, then an attempt to create will be attempted.
			//If the creation attempt fails, tableFamilyExists will be set to false.  If it succeeds, then set to true.
			lastInitTime = System.currentTimeMillis();
			if(hbaseClient != null){
				LOG.info("Initialized HBaseClient");
				Iterator<MapRDBPersistor> i = persistors.iterator();
				while(i.hasNext()){
					MapRDBPersistor p = i.next();
					persistorTableCheckStartTimeMap.put(p,  lastInitTime);
					startTableFamilyCheck(p);
				}
			}

		}
	}
	
	//Call this to start a check on whether table/family exists for a given MapRDBPersistor
	//This is called for all registered MapRDBPersistors when initHBaseClient() is called
	//This is called for the MapRDBPersistor provided for each call to registerPersistor(MapRDBPersistor)
	private void startTableFamilyCheck(MapRDBPersistor p){
		final MapRDBPersistor persistor = p;
		synchronized(lock){
			final long startingInitCount = hbaseClientInitCount;;
			if(hbaseClient==null){
				persistor.setTableFamilyExists(false);
				persistorTableCheckCompleteMap.put(persistor, new Boolean(true));
				return;
			}
			persistorTableCheckCompleteMap.put(persistor, new Boolean(false));
			persistor.setTableFamilyExists(false);
			hbaseClient.ensureTableFamilyExists(persistor.getTablePathBytes(), columnFamilyBytes).addCallbacks(
		    		new Callback<Object, Object>() {
		    			@Override
		    			public Object call(Object arg) throws Exception {
		    				synchronized(lock){
		    					if(hbaseClientInitCount==startingInitCount){
		    						persistorTableCheckCompleteMap.put(persistor, new Boolean(true));
		    						persistor.setTableFamilyExists(true);
		    						//LOG.error("Calling doPutsForBacklog for " + persistor.getId());
		    						//persistor.doPutsForBacklog();
		    						//LOG.error("Finished calling doPutsForBacklog for " + persistor.getId());
		    						
		    					}
		    				}
		   					return null;
		    			}
		   			},
		    		new Callback<Object, Object>() {
		    			@Override
		   				public Object call(Object arg) throws Exception {
		    				synchronized(lock){
		    					if(hbaseClientInitCount==startingInitCount){
			    					persistorTableCheckCompleteMap.put(persistor, new Boolean(true));
			    					persistor.setTableFamilyExists(false);
		    					}
		    				}
		    				return null;
		    			}
		   			}
		    );	
		}
	}
	
	//Call this to discard all in-flight/failed Puts and assign their target Records to be bufferred to disk (or dropped if work dir is disabled)
	//This is called each time initHBaseClient is called.
	//In turn initHBaseClient is called once timeout conditions occur
	private void queueRecordsToDisk(){
		synchronized(lock){
			Iterator<MapRDBPersistor> i = persistors.iterator();
			while(i.hasNext()){
				persistanceFailures += i.next().queueRecordsToDisk();
			}
		}
	}
	
	//This destroys the existing HBaseClient object (and thus destroys all in-flight RPCs)
	private void shutdownHBaseClient(){
		synchronized(lock){
			if(hbaseClient!= null){
				try{
					hbaseClient.shutdown();
					Thread.sleep(5000);
				} catch (Exception e){}
				hbaseClient = null;
			}
		}
	}
	
	private void writeRecordsToWorkDirFile(String tablePath, Object[] records, String filePath) throws Exception {
		File outputFile = new File(filePath);
		boolean outputFileExists = false;
		try {
			outputFileExists = outputFile.exists();
		} catch (Exception e) {
			throw new Exception("Failed to check existance of path " + filePath, e);
		}
		if(outputFileExists){
			throw new Exception("Something already exists at path " + filePath);
		}
		try {
			ObjectOutput output = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
			output.writeObject(tablePath);
			for(Object o : records){
				output.writeObject(o);
			}
			output.close();
		} catch (Exception e) {
			try {
				outputFile.delete();
			} catch (Exception e2){}
			throw new Exception("Failed to write records to disk fail at path " + filePath, e);
		}
	}
	
	private boolean writeBatchOfDiskDestinedRecordsToWorkDirFile(MapRDBPersistor persistor, boolean allowPartialBatch){
		synchronized(persistor){
			Object[] recordsToWrite = null;
			try {
				if(allowPartialBatch){
					recordsToWrite = persistor.removeDiskDestinedRecords( ((persistor.getNumDiskDestinedRecords() < persistor.getMapRDBWorkDirBatchSize()) ? persistor.getNumDiskDestinedRecords() : persistor.getMapRDBWorkDirBatchSize()));
				} else {
					recordsToWrite = persistor.removeDiskDestinedRecords(persistor.getMapRDBWorkDirBatchSize());
				}
			} catch (Exception e) {
				LOG.error("Failed to retrieve Records from MapRDBPersistor-" + System.identityHashCode(persistor), e);
				return false;
			}
			String filePath = maprdbLocalWorkDirPath + "/" + pid + "." + startTime + "." + hostname + "." + numDiskBufferedFiles;
			numDiskBufferedFiles++;
			try {
				writeRecordsToWorkDirFile(persistor.getTablePath(), 
										  recordsToWrite, 
										  filePath );
				persistor.incDiskBufferredRecords(recordsToWrite.length);
				diskBufferedRecords += recordsToWrite.length;
			} catch (Exception e) {
				LOG.error("Dropped records due to write failure for a batch of " + persistor.getMapRDBWorkDirBatchSize() + " Records to the work dir at path " + filePath, e);
				persistor.incDroppedRecords(recordsToWrite.length);
				droppedRecords += recordsToWrite.length;
				return false;
			}
			return true;
		}
	}
	
	private boolean writeBatchOfUnstableRecordsToWorkDirFile(MapRDBPersistor persistor){
		synchronized(persistor){
			Object[] recordsToWrite = new Record[persistor.getMapRDBWorkDirBatchSize()];
			int pos=0;
			int numDiskDestinedRecordsRemoved = 0;
			try {
				Object[] diskDestinedRecordsToWrite = persistor.removeDiskDestinedRecords(((persistor.getNumDiskDestinedRecords() > persistor.getMapRDBWorkDirBatchSize()) ? persistor.getMapRDBWorkDirBatchSize() : persistor.getNumDiskDestinedRecords()));
				numDiskDestinedRecordsRemoved = diskDestinedRecordsToWrite.length;
				for(Object r : diskDestinedRecordsToWrite){
					recordsToWrite[pos++] = (Record)r;
				}
			} catch (Exception e) {
				LOG.error("Failed to retrieve Records from MapRDBPersistor-" + System.identityHashCode(persistor), e);
				return false;
			}
			try {
				Object[] unputRecordsToWrite = persistor.removeRecordsToBePut(persistor.getMapRDBWorkDirBatchSize() - pos);
				for(Object r : unputRecordsToWrite){
					recordsToWrite[pos++] = (Record)r;
				}
			} catch (Exception e) {
				droppedRecords += numDiskDestinedRecordsRemoved;
				persistor.incDroppedRecords(numDiskDestinedRecordsRemoved);
				LOG.error("Dropped " + numDiskDestinedRecordsRemoved + " records after failing to retrieve Records from MapRDBPersistor-" + System.identityHashCode(persistor), e);
				return false;
			}
			String filePath = maprdbLocalWorkDirPath + "/" + pid + "." + startTime + "." + hostname + "." + numDiskBufferedFiles;
			numDiskBufferedFiles++;
			try {
				writeRecordsToWorkDirFile(persistor.getTablePath(), 
										  recordsToWrite, 
										  filePath );
				persistor.incDiskBufferredRecords(persistor.getMapRDBWorkDirBatchSize());
				diskBufferedRecords += persistor.getMapRDBWorkDirBatchSize();
			} catch (Exception e) {
				LOG.error("Failed to write a batch of " + persistor.getMapRDBWorkDirBatchSize() + " Records to the work dir at path " + filePath, e);
				persistor.incDroppedRecords(persistor.getMapRDBWorkDirBatchSize());
				droppedRecords += persistor.getMapRDBWorkDirBatchSize();
				return false;
			}
			return true;
		}
	}
	
	//This flushes records to files on disk until the number of records in memory falls below thresholds
	private void writeRecordsToDisk(MapRDBPersistor persistor, boolean allowPartialBatch){
		synchronized(persistor){
			if(persistor.getMapRDBWorkDirEnabled()){
				if(allowPartialBatch){
					while(persistor.getNumDiskDestinedRecords()!=0 && 
							writeBatchOfDiskDestinedRecordsToWorkDirFile(persistor, allowPartialBatch) )
					{
						//Dummy loop
					}
					if(persistor.getNumDiskDestinedRecords() != 0){
						//Something went wrong while dumping records to disk, so now we need to discard records
						long numDroppedRecords =  persistor.truncateDiskDestinedRecords(0);
						droppedRecords += numDroppedRecords;
						LOG.error("Dropping " + numDroppedRecords + " records following failure to write Records to disk for " + persistor.getId());
					}
				} else {
					while ( persistor.getNumDiskDestinedRecords() >= persistor.getMapRDBWorkDirBatchSize() &&
						    writeBatchOfDiskDestinedRecordsToWorkDirFile(persistor, allowPartialBatch) )
					{
						//Dummy loop
					}
					if(persistor.getNumDiskDestinedRecords() > persistor.getMapRDBWorkDirBatchSize()){
						//Something went wrong while dumping records to disk, so now we need to discard records
						long numDroppedRecords =  persistor.truncateDiskDestinedRecords(persistor.getMapRDBWorkDirBatchSize());
						droppedRecords += numDroppedRecords;
						LOG.error("Dropping " + numDroppedRecords + " records following failure to write Records to disk for " + persistor.getId());
					}
					
					while
					( !persistor.getTableFamilyExists() &&
					   persistor.getNumRecordsToBePut() + persistor.getNumDiskDestinedRecords() >= 2 * persistor.getMapRDBWorkDirBatchSize() &&
					   writeBatchOfUnstableRecordsToWorkDirFile(persistor)
					)
					{
						//Dummy loop
					} 
					if(persistor.getNumRecordsToBePut() + persistor.getNumDiskDestinedRecords() >= 2 * persistor.getMapRDBWorkDirBatchSize()){
						//Something went wrong while dumping records to disk, so now we need to discard records
						long numDroppedRecords = 0;
						if(persistor.getNumRecordsToBePut() >= persistor.getMapRDBWorkDirBatchSize()){
							numDroppedRecords += persistor.truncateDiskDestinedRecords(0);
							numDroppedRecords += persistor.truncateRecordsToBePut(persistor.getMapRDBWorkDirBatchSize());
						} else {
							numDroppedRecords += persistor.truncateDiskDestinedRecords(persistor.getMapRDBWorkDirBatchSize() - persistor.getNumRecordsToBePut());
						}
						droppedRecords += numDroppedRecords;
						LOG.error("Dropping " + numDroppedRecords + " records following failure to write Records to disk for " + persistor.getId());
					}
				}
			} else {
				long numDroppedRecords = persistor.clearDiskDestinedRecordList();
				if(persistor.getNumRecordsToBePut() > persistor.getMapRDBWorkDirBatchSize()){
					numDroppedRecords += persistor.truncateRecordsToBePut(persistor.getMapRDBWorkDirBatchSize());
				}
				droppedRecords += numDroppedRecords;
				LOG.warn("Dropped " + numDroppedRecords + " records from " + persistor.getId());
			}
		}
	}
	
	public void persist(MapRDBPersistor persistor, Record record) {
		synchronized(persistor){
			if(persist(persistor, record, ("t" + record.getTimestamp() + "p" + record.getPreviousTimestamp()).getBytes())){
	    		if(!persist(persistor, null, ("p" + record.getPreviousTimestamp() + "t" + record.getTimestamp()).getBytes())){
	    			persistor.addRecordToBePut(record);
	    			serializationFailures++;
	    			persistor.incSerializationaFailures();
	    		}
	    	} else {
	    		serializationFailures++;
	    		persistor.incSerializationaFailures();
	    	}
			if(persistor.getNumDiskDestinedRecords() + persistor.getNumRecordsToBePut() >= 2 * persistor.getMapRDBWorkDirBatchSize() ||
					persistor.getNumDiskDestinedRecords() >= persistor.getMapRDBWorkDirBatchSize())
			{
				writeRecordsToDisk(persistor, false);
			}
			if (persistor.getNumDiskDestinedRecords() != 0 &&
				persistor.getLastDiskDestinedRecordAddTime() != -1 &&
				System.currentTimeMillis() >  persistor.getLastDiskDestinedRecordAddTime() + putTimeout) 
			{
				writeRecordsToDisk(persistor, true);
			}

		}
	}
	
	/**
	 * This method should be called when a Record needs to be Put to MapRDB
	 * 
	 * In the simple/common case, if this is called while the table/column family is known to exist, 
	 * the Record will immediately be Put to the HBaseClient.
	 * 
	 * In any other scenario, issuing a Put to the HBaseClient would fail outright or sit in memory
	 * for an indefinite period of time and thus should never be done.  
	 * 
	 * So, in any other scenario, the Record will be placed in a queue of Records for which Puts
	 * will be issued to the HBaseClient as soon as a table/column family check returns success 
	 * for this particular Persistor.  
	 * 
	 * Since we can not let Records back up in memory indefinitely, the Thread.run() for 
	 * MapRDBPersistanceManager (e.g. this class) periodically checks the number of Records in 
	 * the queue of Records-to-be-Put and number in the queue of disk bound Records.  If the sum of
	 * those numbers exceeds 2X the configured batch size, then a batch of Records is written to
	 * disk if the work dir is enabled, and then discarded from memory.  Records to be written to
	 * disk are first selected from the disk bound Record queue, and then from the to-be-Put queue.
	 * 
	 * This method returns false only if the Record can not be serialized.  In all other cases,
	 * the Record is either attempted to be Put or queued for Putting and thus the method returns
	 * true.  That this method returns true does not indicate the Record was persisted and in fact
	 * it is still entirely possible that the record gets dropped entirely.
	 */
	public boolean persist(MapRDBPersistor persistor, Record record, byte[] rowKey){
		synchronized(persistor){
			if(persistor.getTableFamilyExists()){
				synchronized(lock){
		   			byte[] serializedObject = null;
		   			final MapRDBPersistor fPersistor = persistor;
					final Record tRecord = record;
					boolean serializationFailed=false;
		    		if(record!=null){
		    			ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    			try {
		    				ObjectOutput oo = new ObjectOutputStream(baos);
		    				oo.writeObject(tRecord);
		    				serializedObject = baos.toByteArray();
		    			} catch (Exception e) {
		    				//This results in the Record being dropped, we aren't going to try to re-serialize it later.
		    				if(System.currentTimeMillis() > lastSerializationFailureStatusTime + 60000){
		    					LOG.error("MapRDBPersistor-" + System.identityHashCode(this) + ": Failed to serialize a record, dropping it.  This has occurred " + 
		    								numSerializationFailuresSinceLastStatus + " times in the last 1 minute", e);
		    					lastSerializationFailureStatusTime = System.currentTimeMillis();
		    					numSerializationFailuresSinceLastStatus = 0;
		    				}
		    				serializationFailed = true;
		    				serializationFailures++;
		    				return false;
		    			}
		    		} else {
		    			//We do two puts per Record in MapRDBPersistor.
		    			//The first put uses timestamp as first part of row key and writes the serialized Record bytes to the row
		    			//The second put uses the previous timestamp as first part of the row key and writes empty data.
		    			//When empty data is retrieved by MapRDBInputRecordQueue (or any other component reading back our tables)
		    			//the requestor should swap the timestamps in the keys and do a new get against the row key with the data.
		    			//This architecture allows scans ordered by timestamp or by previous timestamp.
		    			serializedObject = new byte[0];
		    		}
		    		if(!serializationFailed){
			    		PutRequest p = new PutRequest( persistor.getTablePathBytes(),
			    										rowKey,
			    										columnFamilyBytes,
			    										columnQualifierBytes, 
			    										serializedObject );
			    		final Deferred<Object> d = hbaseClient.put(p);
						persistor.addDeferredEntry(d, record);
						final Long tsForPut = new Long(System.currentTimeMillis());
						if(deferredPutTimestampMap.containsKey(tsForPut)){
							deferredPutTimestampMap.get(tsForPut).add(d);
						} else {
							List<Deferred<Object>> l = new LinkedList<Deferred<Object>>();
							l.add(d);
							deferredPutTimestampMap.put(tsForPut, l);
						}
						d.addCallbacks(
				    		new Callback<Object, Object>() {
				    			@Override
				    			public Object call(Object arg) throws Exception {
				    				synchronized(lock){
					    				fPersistor.removeDeferredEntry(d);
					    				deferredPutTimestampMap.get(tsForPut).remove(d);
					    				if(deferredPutTimestampMap.get(tsForPut).size() == 0)
					    					deferredPutTimestampMap.remove(tsForPut);
					    				lastSuccessfulPutTime = System.currentTimeMillis();
					    				persistedRecords++;
					    				fPersistor.incPersistedRecords();
					    				return null;
				    				}
				    			}
				   			},
				    		new Callback<Object, Object>() {
				    			@Override
				   				public Object call(Object arg) throws Exception {
				    				synchronized(lock){
					    				fPersistor.removeDeferredEntry(d);
					    				deferredPutTimestampMap.get(tsForPut).remove(d);
					    				if(deferredPutTimestampMap.get(tsForPut).size() == 0)
					    					deferredPutTimestampMap.remove(tsForPut);
					    				persistanceFailures++;
					    				fPersistor.incPersistanceFailures();
				    					if(System.currentTimeMillis() > lastPutFailureStatusTime + 60000){
				    						LOG.error("MapRDBPersistor-" + System.identityHashCode(this) + ": Put failed. This message has been suppressed " + numPutFailuresSinceLastStatus + " times in the last 1 minute", (Exception)arg);
				    						lastPutFailureStatusTime = System.currentTimeMillis();
				    						numPutFailuresSinceLastStatus=0;
				    					}
				    					if(fPersistor.getMapRDBWorkDirEnabled()){
					    					fPersistor.addDiskBoundRecord(tRecord);
					    				} else {
					    					droppedRecords++;
					    					fPersistor.incDroppedRecords();
					    				}
					    				return null;
				    				}
				    			}
				   			}
				    	);
		    		}
		   		} 
			} else {
				if(record!=null){
					persistor.addRecordToBePut(record);
				}
		   	}
			return true;
		}
	}
	
	public void logPersistorStatus(){
		for (MapRDBPersistor p : persistors){
			synchronized(p){
				LOG.info("Persistor status: " + p.getId() + 
						" tableAvailable:" + p.getTableFamilyExists() + 
						" queuedPuts: " + p.getNumRecordsToBePut() + 
						" inFlightPuts: " + p.getNumInFlightPuts() + 
						" putSuc:" + p.getPersistedRecords() + 
						" putFail:" + p.getPersistanceFailures() + 
						" recsForDisk:" + p.getNumDiskDestinedRecords() + 
						" recsToDisk:" + p.getDiskBufferredRecords() + 
						" serialF:" + p.getSerializationFailures() + 
						" drops:" + p.getDroppedRecords() + 
						" HBaseClient:" + ((hbaseClient==null) ? "dead" : System.identityHashCode(hbaseClient))
						);
			}
		}
	}

	public void run(){
		while(!shouldExit){
			synchronized(lock){
				long now = System.currentTimeMillis();
				//If the last successful put happened longer ago than the timeout AND
				//	 there are 1 or more in-flight puts AND
				// 	 the start time of the oldest put from longer ago than the timeout AND
				//	 either	the HBaseClient is not connected OR
				//			the HBaseClient has been connected for at least as long as the timeout
				//then we need to check trigger a timeout here
				if
				(	now > lastSuccessfulPutTime + putTimeout &&
					deferredPutTimestampMap.size() > 0 &&
					now > deferredPutTimestampMap.keySet().iterator().next() + putTimeout &&
					( hbaseClient == null || 
					  now - lastInitTime > putTimeout
					)
				){
					LOG.error("Attempting to reinitialize HBaseClient due to put timeout, lastSuccessfulPut: " + lastSuccessfulPutTime +
							  " oldestPutRequest: " + deferredPutTimestampMap.keySet().iterator().next() + " putTimeout: " + 
							  putTimeout + " now: " + now);
					initHBaseClient();
				}
				
				//Regardless of put timeouts, if HBaseClient is not connected and we haven't tried to connect recently
				if(hbaseClient == null && now > lastInitTime + minInitFrequency ){
					LOG.info("Attempting to initialize HBaseClient");
					initHBaseClient();
					now = System.currentTimeMillis();
				}
			}
				
			//We need to do a check for files in the work dir and replay them here
			//System.err.println("IMPLEMENT WORK DIR REPLAY HERE");

			//Check each persistor to see if records need to be written to disk
			
			for (MapRDBPersistor p : persistors){
    			synchronized(p){
    				if(p.getNumRecordsToBePut()!=0){
    					p.doPutsForBacklog();
    				}
    				if(p.getNumDiskDestinedRecords() + p.getNumRecordsToBePut() >= 2 * p.getMapRDBWorkDirBatchSize() ||
    					p.getNumDiskDestinedRecords() >= p.getMapRDBWorkDirBatchSize()){
    					writeRecordsToDisk(p, false);
    				}
    				if (p.getNumDiskDestinedRecords() != 0 &&
    					p.getLastDiskDestinedRecordAddTime() != -1 &&
    					System.currentTimeMillis() >  p.getLastDiskDestinedRecordAddTime() + putTimeout) 
    				{
    					writeRecordsToDisk(p, true);
    				}
    			}
			}
			try {
				Thread.sleep(10000);
			} catch (Exception e){}
		}
	}
}
