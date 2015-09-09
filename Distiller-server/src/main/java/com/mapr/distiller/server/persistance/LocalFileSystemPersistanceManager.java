package com.mapr.distiller.server.persistance;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSystemPersistanceManager extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemPersistanceManager.class);

	//List of all the LocalFileSystemPersistors that this manages
    private List<LocalFileSystemPersistor> persistors;
    
    private Object logFrequencyLock = new Object();
	private long lastSerializationFailureLogTime=-1;
	private int numSerializationFailuresSinceLastLog=0;
	
	public final Object newRecordToPersist = new Object();

	//Used to uniquely identify an instance of the Coordinator application/MapRDBSyncPersistanceManager
    private int pid;
    private long startTime;
    private String hostname;
    
    //Used to control where Records are flushed to disk for buffering and how much space can be consumed
    private long outputDirByteLimit;
	private String outputDirPath;
	
	//Used to indicate to the MapRDBSyncPersistanceManager thread that it should shut down
	private boolean shouldExit;
	private Object shutdownNotifier = new Object();
	
    //Counters
  	private AtomicLong persistedRecords;
  	private AtomicLong persistanceFailures;
	
  	public List<LocalFileSystemPersistor> getPersistors(){
		return persistors;
	}
	
	public boolean hasPersistor(LocalFileSystemPersistor persistor) {
		synchronized(persistors){
			return persistors.contains(persistor);
		}
	}

	public void registerPersistor(LocalFileSystemPersistor persistor) throws Exception{
		synchronized(persistors){
			persistors.add(persistor);
			try {
				persistor.setLocalFileSystemPersistanceManager(this);
				persistor.setTempOutputFileName(pid, startTime, hostname, outputDirPath);
			} catch (Exception e) {
				persistors.remove(persistor);
				throw new Exception("LocalFileSystemPersistanceManager-" + System.identityHashCode(this) + ": Failed to set self as manager for LocalFileSystemPersistor-" + System.identityHashCode(persistor), e);
			}
		}
	}
	
	public void logPersistorStatus(){
		for (LocalFileSystemPersistor p : persistors){
			synchronized(p){
				LOG.info("Persistor status: " + p.getId() + 
						" queued:" + p.numRecordsQueued() + 
						" persisted: " + p.getPersistedRecords() +
						" dropped: " + p.getPersistanceFailures() + 
						" lastFlush: " + p.getLastFlushTime() + 
						" flushFreq:" + p.getFlushFrequency() + 
						" recsInCurFile: " + p.getRecordsInCurrentFile() + 
						" now: " + System.currentTimeMillis()
						);
			}
		}
	}
  	
	public LocalFileSystemPersistanceManager(int pid, long startTime, String hostname, 
			long outputDirByteLimit, String outputDirPath){
		this.pid = pid;
		this.startTime = startTime;
		this.hostname = hostname;
		this.outputDirByteLimit = outputDirByteLimit;
		this.outputDirPath = outputDirPath;
		this.persistedRecords = new AtomicLong(0);
	  	this.persistanceFailures = new AtomicLong(0);
		this.persistors = new LinkedList<LocalFileSystemPersistor>();
		this.shouldExit = false;
	}
	
	private void flushRecordsToDisk(LocalFileSystemPersistor p){
		long[] results = p.writeRecordsToDisk(shouldExit);
		persistedRecords.addAndGet(results[0]);
		persistanceFailures.addAndGet(results[1]);
	}
	
	public void requestShutdown(){
		shouldExit = true;
		synchronized(shutdownNotifier){
			shutdownNotifier.notify();
		}
	}

	
	public void run(){
		while(!shouldExit){
			synchronized(persistors){
				for(LocalFileSystemPersistor p : persistors){
					if
					( p.numRecordsQueued() >= p.getWriteBatchSize() ||
					  ( p.numRecordsQueued() > 0 &&
					    System.currentTimeMillis() >= p.getLastFlushTime() + p.getFlushFrequency()
					  )
					)
					{
						flushRecordsToDisk(p);
					}
				}
			}
			synchronized(shutdownNotifier){
				try {
					shutdownNotifier.wait(10000);
				} catch (Exception e){}
			}
		}
		LOG.info("Received shutdown request");
		while(persistors.size() != 0){
			LocalFileSystemPersistor p = persistors.get(0);
			flushRecordsToDisk(p);
			p.shutdown();
			persistors.remove(p);
		}
	}
}
