package com.mapr.distiller.server.persistance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.util.zip.GZIPOutputStream;


import com.mapr.distiller.server.persistance.MapRDBSyncPersistor;
import com.mapr.distiller.server.recordtypes.Record;

public class MapRDBSyncPersistanceManager extends Thread{
	private static final Logger LOG = LoggerFactory.getLogger(MapRDBSyncPersistanceManager.class);
	
	//Some things to reduce logging when lots of failures start happening.
	private Object logFrequencyLock = new Object();
	private long lastSerializationFailureLogTime=-1;
	private int numSerializationFailuresSinceLastLog=0;
	private long lastPersistFailureLogTime=-1;
	private int numPersistFailuresSinceLastLog=0;
	private int minimumLogInterval = 10000;
	
	//MapRDBPutters will wait on this object for MapRDBSyncPersistors to notify them that there are Records to be put
	public final Object newRecordToBePut = new Object();

	//The path to the table that MapRDBSyncPersistanceManager will use to persist meta info about metrics
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
    
    //List of all the MapRDBSyncPersistors that this manages
    private List<MapRDBSyncPersistor> persistors;

    //Counters...
  	//First we serialize the records...
  	private AtomicLong serializationFailures;
  	//Then we try to persist (e.g. HTable.Put(put))..
  	private AtomicLong persistedRecords;
  	//Or send them to disk queue if table is not available
  	private AtomicLong straightToDiskRecords;
  	//If the operation times out or returns a hard error
  	private AtomicLong persistanceFailures;
  	//If the persist attempt fails, the record is buffered to disk for later replay or dropped entirely
  	private AtomicLong diskBufferredRecords;
  	private AtomicLong droppedRecords;

  	//To be used to create HTable instances in MapRDBSyncPersistors
  	private Configuration hbaseConfiguration;

  	//The minimum elapsed time from submission of the oldest put to now and from completion of the most recent put to now at which the HBaseClient instance will be reinitialized
  	private int putTimeout;
  	
	//Used to uniquely identify an instance of the Coordinator application/MapRDBSyncPersistanceManager
    private int pid;
    private long startTime;
    private String hostname;
    
    //Used to control where Records are flushed to disk for buffering and how much space can be consumed
    private long maprdbLocalWorkDirByteLimit;
	private String maprdbLocalWorkDirPath;
	private long maprdbPutTimeout;
	//Used to indicate to the MapRDBSyncPersistanceManager thread that it should shut down
	private boolean shouldExit;
	
	private int numDiskBufferedFiles;

	public String getMapRDBLocalWorkDirPath(){
		return maprdbLocalWorkDirPath;
	}
	
	public byte[] getColumnFamilyBytes(){
		return columnFamilyBytes;
	}
	
	public byte[] getColumnQualifierBytes(){
		return columnQualifierBytes;
	}
	public Configuration getHBaseConfiguration(){
		return hbaseConfiguration;
	}
	
	public boolean hasPersistor(MapRDBSyncPersistor persistor) {
		synchronized(persistors){
			return persistors.contains(persistor);
		}
	}
	
	//Call this each time a MapRDBSyncPersistor is constructed and should be managed by this
	public void registerPersistor(MapRDBSyncPersistor persistor) throws Exception{
		synchronized(persistors){
			persistors.add(persistor);
			try {
				persistor.setMapRDBSyncPersistanceManager(this);
			} catch (Exception e) {
				persistors.remove(persistor);
				throw new Exception("MapRDBSyncPersistanceManager-" + System.identityHashCode(this) + ": Failed to set self as manager for MapRDBSyncPersistor-" + System.identityHashCode(persistor), e);
			}
		}
	}
	
	public String getColumnFamily(){
		return RECORD_BYTES_COLUMN_FAMILY;
	}

	public MapRDBSyncPersistanceManager(int putTimeout, int pid, long startTime, String hostname, 
										long maprdbLocalWorkDirByteLimit, String maprdbLocalWorkDirPath,
										long maprdbPutTimeout){
		this.putTimeout = putTimeout;
		this.pid = pid;
		this.startTime = startTime;
		this.hostname = hostname;
		this.maprdbLocalWorkDirByteLimit = maprdbLocalWorkDirByteLimit;
		this.maprdbLocalWorkDirPath = maprdbLocalWorkDirPath;
		this.maprdbPutTimeout = maprdbPutTimeout;
        this.serializationFailures = new AtomicLong(0);
      	this.persistedRecords = new AtomicLong(0);
      	this.straightToDiskRecords = new AtomicLong(0);
      	this.persistanceFailures = new AtomicLong(0);
      	this.diskBufferredRecords = new AtomicLong(0);
      	this.droppedRecords = new AtomicLong(0);
      	this.persistors = new LinkedList<MapRDBSyncPersistor>();
      	this.hbaseConfiguration = new HBaseConfiguration();
      	//this.hbaseConfiguration.set("fs.mapr.rpc.timeout", Long.toString(maprdbPutTimeout));
      	//this.hbaseConfiguration.set("fs.mapr.trace", "debug");
      	this.shouldExit = false;
      	this.numDiskBufferedFiles = 0;
	}
	
	
	public void incPersistedRecords(){
		persistedRecords.incrementAndGet();
	}
	public void incPersistanceFailures(){
		persistanceFailures.incrementAndGet();
	}
	public void incSerializationFailures(){
		serializationFailures.incrementAndGet();
	}
	public void incDiskBufferredRecords(int numRecords){
		diskBufferredRecords.addAndGet(numRecords);
	}
	public void incDroppedRecords(){
		droppedRecords.incrementAndGet();
	}
	public void incDroppedRecords(int numRecords){
		droppedRecords.addAndGet(numRecords);
	}
	public long getSerializationFailures(){
		return serializationFailures.get();
	}
	public long getPersistedRecords(){
		return persistedRecords.get();
	}
  	public long getPersistanceFailures(){
  		return persistanceFailures.get();
  	}
  	public long getDiskBufferredRecords(){
  		return diskBufferredRecords.get();
  	}
  	public long getDroppedRecords(){
  		return droppedRecords.get();
  	}

  	public Object[] getNextPut(){
  		Record r = null;
  		synchronized(persistors){
	  		for (MapRDBSyncPersistor p : persistors){
	  			r = p.getNextRecordToBePut();
	  			if(r != null){
	  				return new Object[] {p, r};
	  			}
	  		}
	  		return null;
  		}
  	}
  	
	//This flushes records to files on disk until the number of records in memory falls below thresholds
	private void writeRecordsToDisk(MapRDBSyncPersistor persistor, boolean allowPartialBatch){
		if(allowPartialBatch){
			while(persistor.getNumDiskBoundRecords()!=0 && 
				  writeBatchOfDiskBoundRecordsToWorkDirFile(persistor, allowPartialBatch) )
			{
				//Dummy loop
			}
			if(persistor.getNumDiskBoundRecords() != 0){
				//Something went wrong while dumping records to disk, so now we need to discard records
				long numDroppedRecords =  persistor.truncateDiskBoundRecords(0);
				droppedRecords.addAndGet(numDroppedRecords);
				LOG.error("Dropping " + numDroppedRecords + " records following failure to write Records to disk for " + persistor.getId());
			}
		} else {
			while ( persistor.getNumDiskBoundRecords() >= persistor.getMapRDBWorkDirBatchSize() &&
				    writeBatchOfDiskBoundRecordsToWorkDirFile(persistor, allowPartialBatch) )
			{
				//Dummy loop
			}
			if(persistor.getNumDiskBoundRecords() > persistor.getMapRDBWorkDirBatchSize()){
				//Something went wrong while dumping records to disk, so now we need to discard records
				long numDroppedRecords =  persistor.truncateDiskBoundRecords(persistor.getMapRDBWorkDirBatchSize());
				droppedRecords.addAndGet(numDroppedRecords);
				LOG.error("Dropping " + numDroppedRecords + " records following failure to write Records to disk for " + persistor.getId());
			}
		}
	}
	
	private void writeRecordsToWorkDirFile(String tablePath, Object[] records, String filePath) throws Exception {
		File outputFile = new File(filePath);
		File tempFile = new File(filePath + ".tmp");
		boolean fileExists = false;
		try {
			fileExists = outputFile.exists();
		} catch (Exception e) {
			throw new Exception("Failed to check existance of path " + filePath, e);
		}
		if(fileExists){
			throw new Exception("Something already exists at path " + filePath);
		}
		fileExists = false;
		try {
			fileExists = tempFile.exists();
		} catch (Exception e) {
			throw new Exception("Failed to check existance of path " + filePath + ".tmp", e);
		}
		if(fileExists){
			throw new Exception("Something already exists at path " + filePath + ".tmp");
		}
		try {
			ObjectOutput output = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(filePath + ".tmp")));
			output.writeObject(tablePath);
			for(Object o : records){
				output.writeObject(o);
			}
			output.close();
			try {
				tempFile.renameTo(outputFile);
			} catch (Exception e){
				throw new Exception("Dropping " + records.length + " records due to failure to rename temporary output file to " + filePath, e);
			}
		} catch (Exception e) {
			try {
				outputFile.delete();
			} catch (Exception e2){}
			try {
				tempFile.delete();
			} catch (Exception e2){}
			throw new Exception("Failed to write records to disk at path " + filePath, e);
		}
	}
	
	private boolean writeBatchOfDiskBoundRecordsToWorkDirFile(MapRDBSyncPersistor persistor, boolean allowPartialBatch){
		Object[] recordsToWrite = null;
		try {
			if(allowPartialBatch){
				recordsToWrite = persistor.removeDiskBoundRecords( ((persistor.getNumDiskBoundRecords() < persistor.getMapRDBWorkDirBatchSize()) ? persistor.getNumDiskBoundRecords() : persistor.getMapRDBWorkDirBatchSize()));
			} else {
				recordsToWrite = persistor.removeDiskBoundRecords(persistor.getMapRDBWorkDirBatchSize());
			}
		} catch (Exception e) {
			LOG.error("Failed to retrieve Records from MapRDBSyncPersistor-" + System.identityHashCode(persistor), e);
			return false;
		}
		String filePath = maprdbLocalWorkDirPath + "/" + pid + "_" + startTime + "_" + hostname + "_" + recordsToWrite.length + "_" + numDiskBufferedFiles;
		numDiskBufferedFiles++;
		try {
			writeRecordsToWorkDirFile(persistor.getTablePath(), 
									  recordsToWrite, 
									  filePath );
			persistor.incDiskBufferredRecords(recordsToWrite.length);
			diskBufferredRecords.addAndGet(recordsToWrite.length);
		} catch (Exception e) {
			LOG.error("Dropped records due to write failure for a batch of " + persistor.getMapRDBWorkDirBatchSize() + " Records to the work dir at path " + filePath, e);
			persistor.incDroppedRecords(recordsToWrite.length);
			droppedRecords.addAndGet(recordsToWrite.length);
			return false;
		}
		return true;
	}
	
	public void logPersistorStatus(){
		synchronized(persistors){
			for (MapRDBSyncPersistor p : persistors){
				LOG.info("Persistor status: " + p.getId() + 
						" tableAvailable:" + p.getTableFamilyExists() + 
						" queuedRecords: " + p.getNumRecordsToBePut() + 
						" inFlightPuts: " + p.getNumRecordsInFlight() + 
						" putSuc:" + p.getPersistedRecords() + 
						" putFail:" + p.getPersistanceFailures() + 
						" straightToDisk:" + p.getStraightToDiskRecords() + 
						" recsForDisk:" + p.getNumDiskBoundRecords() + 
						" recsToDisk:" + p.getDiskBufferredRecords() + 
						" serialF:" + p.getSerializationFailures() + 
						" drops:" + p.getDroppedRecords()
						);
			}
		}
	}
	
	public void skipPutAttempts(MapRDBSyncPersistor persistor, int numPutsToKeep){
		persistor.skipPutAttempts(numPutsToKeep);
	}
	public void incStraightToDiskRecords(){
		straightToDiskRecords.incrementAndGet();
	}
	public long getStraightToDiskRecords(){
		return straightToDiskRecords.get();
	}
	
	public Object[] tryToSetLastSerializationFailureLogTime(long newTime){
		synchronized(logFrequencyLock){
			if(newTime > lastSerializationFailureLogTime + minimumLogInterval){
				Long oldTime = new Long(lastSerializationFailureLogTime);
				Integer numFails = new Integer(numSerializationFailuresSinceLastLog);
				lastSerializationFailureLogTime = newTime;
				numSerializationFailuresSinceLastLog=0;
				return new Object[] {oldTime, numFails};
			}
			numSerializationFailuresSinceLastLog++;
			return null;
		}		
	}

	public Object[] tryToSetLastPersistFailureLogTime(long newTime){
		synchronized(logFrequencyLock){
			if(newTime > lastPersistFailureLogTime + minimumLogInterval){
				Long oldTime = new Long(lastPersistFailureLogTime);
				Integer numFails = new Integer(numPersistFailuresSinceLastLog);
				lastPersistFailureLogTime = newTime;
				numPersistFailuresSinceLastLog=0;
				return new Object[] {oldTime, numFails};
			}
			numPersistFailuresSinceLastLog++;
			return null;
		}		
	}
  	
	public void run(){
		//Hard coding 32 threads here for now, should be adaptive though...
		int numPutterThreads=32;
		MapRDBPutter[] putters = new MapRDBPutter[numPutterThreads];
		MapRDBReplayer maprdbReplayer = new MapRDBReplayer(this);
		maprdbReplayer.start();
		for(int x=0; x<numPutterThreads; x++){
			putters[x] = new MapRDBPutter(this);
			putters[x].start();
		}
		while(!shouldExit){
			synchronized(persistors){
				for(MapRDBSyncPersistor p : persistors){
					if(p.getNumRecordsToBePut() >= 2* p.getMapRDBWorkDirBatchSize()){
						skipPutAttempts(p, p.getMapRDBWorkDirBatchSize());
					}
					if(p.getNumDiskBoundRecords() >= p.getMapRDBWorkDirBatchSize()){
	    				writeRecordsToDisk(p, false);
	    			}
	    			if (p.getNumDiskBoundRecords() != 0 &&
	    				System.currentTimeMillis() >  p.getLastDiskBoundRecordAddTime() + putTimeout) 
	    			{
	    				writeRecordsToDisk(p, true);
	    			}
				}
			}
			try {
				Thread.sleep(5000);
			} catch (Exception e){}
		}
	}
}
