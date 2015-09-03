package com.mapr.distiller.server.persistance;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.persistance.MapRDBPersistanceManager;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import com.stumbleupon.async.Deferred;

public class MapRDBPersistor implements Persistor {
	
	//The table to which Records should be put
	private String tablePath;
    private byte[] tablePathBytes;
    
    //The ID of the metric being persisted
    private String producerId;
    
    //A reference to the MapRDBPersistanceManager to which Records will be passed when a put is to be performed
    private MapRDBPersistanceManager maprdbPersistanceManager;
    
    //Used to control whether or not records are buffered to disk for later replay when Puts are failing
    private boolean maprdbWorkDirEnabled;
    
    //Set to true when the MapRDBPersistanceManager is able to connect to the MapR cluster via HBaseClient and confirm that the target table exists with the required column family.
	private boolean tableFamilyExists;

	//Whether or not tables/column families should be created in MapRDB if they don't already exist
	private boolean createTablesIfNecessary;
	
	//A list of Records for which Puts should be issued to HBaseClient as soon as tableFamilyExists==true
	//Also, a list of Records from which Records should be selected and written to disk or discarded 
	//upon reaching 2X batch size in conjunction with the diskDestinedRecordList;
	private List<Record> recordsToBePut;
	
	//Map of Deferred for in flight Put to the target Record
	private Map<Deferred<Object>, Record> deferredPutRecordMap;

	//List of Records from failed Puts that should be written to disk for buffering and later replay
	private List<Record> diskDestinedRecordList;
	//Most recent time at which diskDestinedRecordList had was given a Record
	private long lastDiskDestinedRecordAdd;
	
	//The number of Records destined for disk that are buffered in memory before being written to the work dir (to save on IO workload)
	private int maprdbWorkDirBatchSize;
	
    //Counters...
  	//First we serialize the records...
  	private long serializationFailures;
  	//Then we try to persist (e.g. HBaseClient.put)..
  	private long persistedRecords;
  	private long persistanceFailures;
  	//If the persist attempt fails, the record is buffered to disk for later replay or dropped entirely
  	private long diskBufferredRecords;
  	private long droppedRecords;

	
	public MapRDBPersistor(String tablePath, boolean maprdbWorkDirEnabled, int maprdbWorkDirBatchSize, 
							boolean createTablesIfNecessary, String producerId){
    	this.tablePath = tablePath;
    	this.maprdbWorkDirEnabled = maprdbWorkDirEnabled;
    	this.maprdbWorkDirBatchSize = maprdbWorkDirBatchSize;
    	this.createTablesIfNecessary = createTablesIfNecessary;
    	this.producerId = producerId;
    	
    	this.tablePathBytes = tablePath.getBytes();
    	
    	this.recordsToBePut = new LinkedList<Record>();
    	this.deferredPutRecordMap = new HashMap<Deferred<Object>, Record>(128);
    	this.diskDestinedRecordList = new LinkedList<Record>();
    	this.tableFamilyExists = false;
        this.serializationFailures = 0;
      	this.persistedRecords = 0;
      	this.persistanceFailures = 0;
      	this.diskBufferredRecords = 0;
      	this.droppedRecords = 0;
      	this.lastDiskDestinedRecordAdd = -1;
	}
	
	
	//This method is used to determine whether disk destined records should be flushed to disk based on their age
	//Flushing to disk is triggered by having too many records in memory or by the age of those Records.
	public long getLastDiskDestinedRecordAddTime(){
		return lastDiskDestinedRecordAdd;
	}

	
	//Moves all Records from Put map to list of records to buffer to disk (if enabled)
	//Discards contents of Put map
	//Returns 0 when work dir is enabled, otherwise it returns the number of records dropped (for tracking purposes)
	public long queueRecordsToDisk(){
		long ret = 0l;
		if(maprdbWorkDirEnabled){
			if(deferredPutRecordMap.size() > 0){
				Iterator<Map.Entry<Deferred<Object>, Record>> i = deferredPutRecordMap.entrySet().iterator();
				while(i.hasNext()){
					diskDestinedRecordList.add(i.next().getValue());
				}
				lastDiskDestinedRecordAdd = System.currentTimeMillis();
			}
		} else {
			ret = deferredPutRecordMap.size();
		}
		deferredPutRecordMap.clear();
		return ret;
	}
	
	public long clearDiskDestinedRecordList(){
		long numDrops = diskDestinedRecordList.size();
		diskDestinedRecordList.clear();
		lastDiskDestinedRecordAdd = -1;
		droppedRecords += numDrops;
		return numDrops;
	}

	public long truncateDiskDestinedRecords(int numRecordsToLeave){
		long numDrops = diskDestinedRecordList.size() - numRecordsToLeave;
		if(numDrops>0){
			diskDestinedRecordList = diskDestinedRecordList.subList(diskDestinedRecordList.size() - numRecordsToLeave, diskDestinedRecordList.size());
			droppedRecords += numDrops;
			return numDrops;
		}
		return 0;
	}
	
	public long truncateRecordsToBePut(int numRecordsToLeave){
		long numDrops = recordsToBePut.size() - numRecordsToLeave;
		if(numDrops>0){
			recordsToBePut = recordsToBePut.subList(recordsToBePut.size() - numRecordsToLeave, recordsToBePut.size());
			droppedRecords += numDrops;
			return numDrops;
		}
		return 0;
	}
	public void setTableFamilyExists(boolean b){
		tableFamilyExists = b;
	}
	
	public boolean getTableFamilyExists(){
		return tableFamilyExists;
	}
	
	public byte[] getTablePathBytes(){
		return tablePathBytes;
	}
	
	public String getTablePath(){
		return tablePath;
	}
	
	public boolean getMapRDBWorkDirEnabled(){
		return maprdbWorkDirEnabled;
	}
	
	public int getMapRDBWorkDirBatchSize(){
		return maprdbWorkDirBatchSize;
	}
	
	public void addRecordToBePut(Record r){
		recordsToBePut.add(r);
	}
	
	public void addDiskBoundRecord(Record r){
		diskDestinedRecordList.add(r);
		lastDiskDestinedRecordAdd = System.currentTimeMillis();
	}
	
	public Object[] removeRecordsToBePut(int numRecords) throws Exception{
		if(recordsToBePut.size() < numRecords)
			throw new Exception("Can not remove " + numRecords + " from recordsToBePut with " + recordsToBePut.size() + " records");
		Object[] ret = (recordsToBePut.subList(0, numRecords)).toArray();
		recordsToBePut = recordsToBePut.subList(numRecords, recordsToBePut.size());
		return ret;
	}
	
	public Object[] removeDiskDestinedRecords(int numRecords) throws Exception{
		if(diskDestinedRecordList.size() < numRecords)
			throw new Exception("Can not remove " + numRecords + " from diskDestinedRecordList with " + diskDestinedRecordList.size() + " records");
		Object[] ret = (diskDestinedRecordList.subList(0, numRecords)).toArray();
		diskDestinedRecordList = diskDestinedRecordList.subList(numRecords, diskDestinedRecordList.size());
		if(diskDestinedRecordList.size()==0)
			lastDiskDestinedRecordAdd = -1;
		return ret;
	}
	
	public void setMapRDBPersistanceManager(MapRDBPersistanceManager maprdbPersistanceManager) throws Exception{
		if(maprdbPersistanceManager.hasPersistor(this)){
			this.maprdbPersistanceManager = maprdbPersistanceManager;
		} else {
			throw new Exception("MapRDBPersistor-" + System.identityHashCode(this) + ": Received a request to set MapRDBPersistanceManager to " + 
								System.identityHashCode(maprdbPersistanceManager) + " but this is not registered with it. " + 
								"The current MapRDBPersistanceManager for this is " + 
								((maprdbPersistanceManager==null) ? "null" : System.identityHashCode(maprdbPersistanceManager)));
		}
	}
	
	public void addDeferredEntry(Deferred<Object> d, Record r){
		deferredPutRecordMap.put(d, r);
	}
	
	public void removeDeferredEntry(Deferred<Object> d){
		deferredPutRecordMap.remove(d);
	}
	
	public void incPersistedRecords(){
		persistedRecords++;
	}
	
	public void incPersistanceFailures(){
		persistanceFailures++;
	}
	
	public void incSerializationaFailures(){
		serializationFailures++;
	}
	
	public void incDiskBufferredRecords(int numRecords){
		diskBufferredRecords += numRecords;
	}
	
	public void incDroppedRecords(){
		droppedRecords++;
	}

	public void incDroppedRecords(int numRecords){
		droppedRecords += numRecords;
	}

	public String getId(){
		return producerId;
	}
	
	public int getNumRecordsToBePut(){
		return recordsToBePut.size();
	}
	public int getNumInFlightPuts(){
		return deferredPutRecordMap.size();
	}
	public int getNumDiskDestinedRecords(){
		return diskDestinedRecordList.size();
	}
	public long getSerializationFailures(){
		return serializationFailures;
	}
	public long getPersistedRecords(){
		return persistedRecords;
	}
  	public long getPersistanceFailures(){
  		return persistanceFailures;
  	}
  	public long getDiskBufferredRecords(){
  		return diskBufferredRecords;
  	}
  	public long getDroppedRecords(){
  		return droppedRecords;
  	}

  	public void doPutsForBacklog(){
  		int numRecordsToPut = recordsToBePut.size();
  		if(tableFamilyExists){
  			for (Record r : recordsToBePut){
  				persist(r);
  			}
  			recordsToBePut.clear();
  		}
  	}

    public void persist(Record record){
    	maprdbPersistanceManager.persist(this, record);
    }
}
