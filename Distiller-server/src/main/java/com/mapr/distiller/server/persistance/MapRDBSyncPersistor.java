package com.mapr.distiller.server.persistance;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.persistance.MapRDBSyncPersistanceManager;
import com.mapr.distiller.server.utils.TimestampBasedRecordComparator;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRDBSyncPersistor implements Persistor {
	
	private class TimeWindow{
		long start, end;
		public TimeWindow(long start, long end){
			this.start = start;
			this.end = end;
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(MapRDBSyncPersistor.class);
	
	//The table to which Records should be put
	private String tablePath;
    
    //The HTable instance connected to the specified tablePath
    private HTable table;
    
    //The ID of the metric being persisted
    private String producerId;
    
    //A reference to the MapRDBSyncPersistanceManager to which Records will be passed when a put is to be performed
    private MapRDBSyncPersistanceManager maprdbSyncPersistanceManager;
    
    //Used to control whether or not records are buffered to disk for later replay when Puts are failing
    private boolean maprdbWorkDirEnabled;
    
    //Set to true when the MapRDBSyncPersistanceManager is able to connect to the MapR cluster via HBaseClient and confirm that the target table exists with the required column family.
	private AtomicBoolean tableFamilyExists;

	//Whether or not tables/column families should be created in MapRDB if they don't already exist
	private boolean createTablesIfNecessary;
	
	//A LinkedList of records to be put to MapRDB.  As persist(Record) is called in this class, the record is 
	//added to the end of this list and then we notify MapRDBSyncPersistanceManager that we have Puts that need 
	//to be done.
	private TreeSet<Record> recordsToBePut;
	
	//When a MapRDBPutter finds a Record in recordsToBePut, it will try to serialize it then add the Record to this list as it tries to Put
	private TreeSet<Record> recordsInFlight;
	
	//If the put did not succeed, due to a timeout or hard error, the MapRDBPutter will put the Record into this List which
	//is checked periodically by the MapRDBSyncPersistanceManager to which will write out batches of Records to files as needed
	private TreeSet<Record> diskBoundRecords;
	
	//Most recent time at which diskDestinedRecordList was given a Record
	private long lastDiskBoundRecordAddTime;
	
	//The minimum number of Records that need to accumulate in the diskDestinedRecordList before a batch of Records of that size is written to disk
	private int maprdbWorkDirBatchSize;
	
    //Counters...
  	//First we serialize the records...
  	private AtomicLong serializationFailures;
  	//Then we try to persist (e.g. HTable.Put(put))..
  	private AtomicLong persistedRecords;
  	//Or if the target table isn't available, the record is sent straight to the disk queue
  	private AtomicLong straightToDiskRecords;
  	//If the operation times out or returns a hard error
  	private AtomicLong persistanceFailures;
  	//If the persist attempt fails, the record is buffered to disk for later replay or dropped entirely
  	private AtomicLong diskBufferredRecords;
  	private AtomicLong droppedRecords;

	private long lastTableFamilyCheckTime;
	private long minTableFamilyCheckInterval;
	private AtomicBoolean tableFamilyCheckInProgress;
	
	private long firstRecordTimestamp;
	private long mostRecentRecordTimestamp;
	
	private LinkedList<TimeWindow> droppedRecordWindows;
	
	public MapRDBSyncPersistor(String tablePath, boolean maprdbWorkDirEnabled, int maprdbWorkDirBatchSize, 
							boolean createTablesIfNecessary, String producerId){
    	this.tablePath = tablePath;
    	this.maprdbWorkDirEnabled = maprdbWorkDirEnabled;
    	this.maprdbWorkDirBatchSize = maprdbWorkDirBatchSize;
    	this.createTablesIfNecessary = createTablesIfNecessary;
    	this.producerId = producerId;
    	
    	this.recordsToBePut = new TreeSet<Record>(new TimestampBasedRecordComparator());
    	this.recordsInFlight = new TreeSet<Record>(new TimestampBasedRecordComparator());
    	this.diskBoundRecords= new TreeSet<Record>(new TimestampBasedRecordComparator());
    	this.tableFamilyExists = new AtomicBoolean(false);
        this.serializationFailures = new AtomicLong(0);
      	this.persistedRecords = new AtomicLong(0);
      	this.straightToDiskRecords = new AtomicLong(0);
      	this.persistanceFailures = new AtomicLong(0);
      	this.diskBufferredRecords = new AtomicLong(0);
      	this.droppedRecords = new AtomicLong(0);
      	this.lastDiskBoundRecordAddTime = -1;
      	this.lastTableFamilyCheckTime = -1;
      	this.minTableFamilyCheckInterval = 60000;
      	this.tableFamilyCheckInProgress = new AtomicBoolean(false);
      	this.firstRecordTimestamp = -1;
      	this.mostRecentRecordTimestamp = -1;
	}
	
	public void callPut(Put p) throws Exception{
		LOG.debug("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Calling put to table " + System.identityHashCode(table) + " and put " + System.identityHashCode(p));
		table.put(p);
		LOG.debug("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": returning normally after put");
	}
	
	public int getNumDiskBoundRecords(){
		synchronized(diskBoundRecords){
			return diskBoundRecords.size();
		}
	}
	
	public long getLastDiskBoundRecordAddTime(){
		synchronized(diskBoundRecords){
			return lastDiskBoundRecordAddTime;
		}
	}
	
	public void addDiskBoundRecord(Record r){
		synchronized(diskBoundRecords){
			diskBoundRecords.add(r);
			lastDiskBoundRecordAddTime = System.currentTimeMillis();
		}
	}
	public void addRecordInFlight(Record r){
		synchronized(recordsInFlight){
			recordsInFlight.add(r);
		}
	}
	public void removeRecordInFlight(Record r){
		synchronized(recordsInFlight){
			recordsInFlight.remove(r);
		}
	}
	public void skipPutAttempts(int numPutsToKeep){
		synchronized(recordsToBePut){
			synchronized(diskBoundRecords){
				Record r = null;
				int x=0;
				for( ; x<recordsToBePut.size() - numPutsToKeep; x++){
					r = recordsToBePut.first();
					diskBoundRecords.add(r);	
					recordsToBePut.remove(r);
				}
				LOG.info("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Skipping put attempts for " + x + " records");
			}
		}
	}
	
	public void persist(Record record) throws Exception{
		if(firstRecordTimestamp==-1){
    		firstRecordTimestamp = record.getTimestamp();
    		mostRecentRecordTimestamp = firstRecordTimestamp;
    	} else {
    		if(mostRecentRecordTimestamp > record.getTimestamp()){
    			throw new Exception("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Timestamps out of order, last: " +
    								mostRecentRecordTimestamp + " current:" + record.getTimestamp());
    		}
    		mostRecentRecordTimestamp = record.getTimestamp();
    	}
		synchronized(recordsToBePut){
			recordsToBePut.add(record);
		}
		synchronized(maprdbSyncPersistanceManager.newRecordToBePut){
			maprdbSyncPersistanceManager.newRecordToBePut.notify();
		}
    }
    
    public Record getNextRecordToBePut(){
    	synchronized(recordsToBePut){
    		try {
    			Record r = recordsToBePut.first();
    			recordsToBePut.remove(r);
    			return r;
    		} catch (Exception e) {
    			return null;
    		}
    	}
    }
	
	public void setMapRDBSyncPersistanceManager(MapRDBSyncPersistanceManager maprdbSyncPersistanceManager) throws Exception{
		if(maprdbSyncPersistanceManager.hasPersistor(this)){
			this.maprdbSyncPersistanceManager = maprdbSyncPersistanceManager;
		} else {
			throw new Exception("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Received a request to set MapRDBSyncPersistanceManager to " + 
								System.identityHashCode(maprdbSyncPersistanceManager) + " but this is not registered with it. " + 
								"The current MapRDBSyncPersistanceManager for this is " + 
								((maprdbSyncPersistanceManager==null) ? "null" : System.identityHashCode(maprdbSyncPersistanceManager)));
		}
		try {
			table = new HTable(maprdbSyncPersistanceManager.getHBaseConfiguration(), tablePath);
			table.setAutoFlush(true);
			checkTableFamily();
		} catch (Exception e) {
			//Looks like MapRDB/table/family is not available right now...  Records will either go to disk or be discarded.
		}
	}
	
	public void checkTableFamily(){
		synchronized(tableFamilyCheckInProgress){
			if(tableFamilyCheckInProgress.get())
				return;
			tableFamilyCheckInProgress.set(true);
		}
		try {
			if (System.currentTimeMillis() > lastTableFamilyCheckTime + minTableFamilyCheckInterval){
				LOG.info("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Checking for table " + tablePath);
				lastTableFamilyCheckTime = System.currentTimeMillis();
				if(table == null) {
					try {
						table = new HTable(maprdbSyncPersistanceManager.getHBaseConfiguration(), tablePath);
						table.setAutoFlush(false);
					} catch (Exception e) {
						LOG.error("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Failed to construct HTable instance, records will be buffered to disk or discarded", e);
						tableFamilyExists.set(false);
						tableFamilyCheckInProgress.set(false);
						return;
					}
				}
				HTableDescriptor d = null;
				try {
					d = table.getTableDescriptor();
				} catch (Exception e) {
					LOG.error("Failed to get table descriptor for table " + tablePath, e);
					tableFamilyExists.set(false);
				}
				if(d!=null && d.hasFamily(maprdbSyncPersistanceManager.getColumnFamilyBytes())){
					LOG.debug("Found family " + maprdbSyncPersistanceManager.getColumnFamily() + " in table " + tablePath);
					tableFamilyExists.set(true);
				} else {
					LOG.error("Family " + maprdbSyncPersistanceManager.getColumnFamily() + " does not exist in table " + tablePath);
					tableFamilyExists.set(false);
				}
			}
		} finally {
			tableFamilyCheckInProgress.set(false);
		}
	}
	
	public long truncateDiskBoundRecords(int numRecordsToLeave){
		synchronized(diskBoundRecords){
			long numDrops = diskBoundRecords.size() - numRecordsToLeave;
			if(numDrops>0){
				long startTimeOfDrops = diskBoundRecords.first().getTimestamp();
				long endTimeOfDrops=-1;
				Record r;
				for(int x=0; x<numDrops; x++){
					r = diskBoundRecords.first();
					endTimeOfDrops = r.getTimestamp();
					diskBoundRecords.remove(r);
				}
				droppedRecords.addAndGet(numDrops);
				droppedRecordWindows.add(new TimeWindow(startTimeOfDrops, endTimeOfDrops));
				return numDrops;
			}
			return 0;
		}
	}
	
	public Object[] removeDiskBoundRecords(int numRecords) throws Exception{
		synchronized(diskBoundRecords){
			if(diskBoundRecords.size() < numRecords)
				throw new Exception("Can not remove " + numRecords + " from diskDestinedRecordList with " + diskBoundRecords.size() + " records");
			Object[] ret = new Object[numRecords];
			for(int x=0; x<numRecords; x++){
				ret[x] = diskBoundRecords.first();
				diskBoundRecords.remove(ret[x]);
			}
			return ret;
		}
	}
	
	public String getTablePath(){
		return tablePath;
	}
	
	public int getNumRecordsToBePut(){
		synchronized(recordsToBePut){
			return recordsToBePut.size();
		}
	}
	public int getNumRecordsInFlight(){
		synchronized(recordsInFlight){
			return recordsInFlight.size();
		}
	}
	
	public long getStraightToDiskRecords(){
		return straightToDiskRecords.get();
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
	public void incStraightToDiskRecords(){
		straightToDiskRecords.incrementAndGet();
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
	public void incDroppedRecords(int numRecordsDropped){
		droppedRecords.addAndGet(numRecordsDropped);
	}
	public String getId(){
		return producerId;
	}
	public void setTableFamilyExists(boolean b){
		tableFamilyExists.set(b);
	}
	public boolean getTableFamilyExists(){
		return tableFamilyExists.get();
	}
	public boolean getMapRDBWorkDirEnabled(){
		return maprdbWorkDirEnabled;
	}
	public int getMapRDBWorkDirBatchSize(){
		return maprdbWorkDirBatchSize;
	}
	
}