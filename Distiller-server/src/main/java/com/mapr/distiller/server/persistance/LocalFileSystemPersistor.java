package com.mapr.distiller.server.persistance;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;

public class LocalFileSystemPersistor implements Persistor{
	private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemPersistor.class);

	//The ID of the metric being persisted
    private String producerId;
    
    //A reference to the MapRDBPersistanceManager to which Records will be passed when a put is to be performed
    private LocalFileSystemPersistanceManager localFileSystemPersistanceManager;
    
    //Counters
  	private AtomicLong persistedRecords;
  	private AtomicLong persistanceFailures;

  	//A list of Records for which Puts should be issued to HBaseClient as soon as tableFamilyExists==true
  	//Also, a list of Records from which Records should be selected and written to disk or discarded 
  	//upon reaching 2X batch size in conjunction with the diskDestinedRecordList;
  	private LinkedList<Record> recordsToPersist;
  	
  	private int writeBatchSize;
  	int flushFrequency;
  	int maxRecordsPerFile;
  	
  	private long lastFlushTime;
  	private int recordsInCurrentFile;
  	
  	private ObjectOutputStream currentOutputFileStream;
  	private File currentOutputFile;
  	
  	private String tempOutputFileName;
  	private String tempOutputFilePath;
  	
  	private long lastWriteFailureLogTime=0;
  	private long writeFailureLogFrequency=60000;
  	private int numWriteFailuresSinceLastLog=0;
  	
  	private long firstRecordInFileTimestamp=-1, 
  				 firstRecordInFilePreviousTimestamp=-1, 
  				 lastRecordInFileTimestamp=-1, 
  				 lastRecordInFilePreviousTimestamp=-1;

  	
  	public long getPersistedRecords(){
  		return persistedRecords.get();
  	}
  	
  	public long getPersistanceFailures(){
  		return persistanceFailures.get();
  	}
  	public int numRecordsQueued(){
  		return recordsToPersist.size();
  	}
  	
  	public long getLastFlushTime(){
  		return lastFlushTime;
  	}
  	
  	public long getFlushFrequency(){
  		return flushFrequency;
  	}
  	
  	public int getMaxRecordsPerFile(){
  		return maxRecordsPerFile;
  	}
  	
  	public int getRecordsInCurrentFile(){
  		return recordsInCurrentFile;
  	}
  	
  	public int getWriteBatchSize(){
  		return writeBatchSize;
  	}
  	
	public LocalFileSystemPersistor(int writeBatchSize, int flushFrequency, int maxRecordsPerFile, 
									String producerId){
		this.writeBatchSize = writeBatchSize;
		this.flushFrequency = flushFrequency;
		this.maxRecordsPerFile = maxRecordsPerFile;
		this.producerId = producerId;
		
		this.recordsToPersist = new LinkedList<Record>();
		this.persistedRecords = new AtomicLong(0);
	  	this.persistanceFailures = new AtomicLong(0);
	  	this.lastFlushTime=System.currentTimeMillis();
	  	this.recordsInCurrentFile=0;
	  	this.tempOutputFileName = null;
	}
	
	public void setTempOutputFileName(int pid, long starttime, String hostname, String outputDirPath){
		tempOutputFileName = hostname + "_" + producerId + "_" + pid + "_" + starttime;
		tempOutputFilePath = outputDirPath + "/" + tempOutputFileName;
	}
	
	public void shutdown(){
		if(firstRecordInFilePreviousTimestamp != -1){
			try {
				currentOutputFileStream.close();
				currentOutputFileStream = null;
				currentOutputFile.renameTo(new File(tempOutputFilePath + "_" + 
													firstRecordInFilePreviousTimestamp + "_" + 
													firstRecordInFileTimestamp + "_" + 
													lastRecordInFilePreviousTimestamp + "_" + 
													lastRecordInFileTimestamp));
				currentOutputFile = null;
			} catch (Exception e){}
		}
		LOG.info("LocalFileSystemPersistor-" + System.identityHashCode(this) + ": Shutdown, persisted: " + persistedRecords + " failed: " + persistanceFailures + " most recent file: " + tempOutputFilePath);
	}
	
	public long[] writeRecordsToDisk(boolean shutdown){
		long[] ret = new long[]{0, 0};
		while(recordsToPersist.size()!=0){
			try {
				setOutputFile();	
			} catch (Exception e){
				int numDrops = recordsToPersist.size();
				LOG.error("Dropping " + numDrops + " records due to failure to setup output file " + tempOutputFilePath, e);
				recordsToPersist.clear();
				persistanceFailures.addAndGet(numDrops);
				ret[1] += numDrops;
				return ret;
			}
			int numRecordsToWrite = maxRecordsPerFile - recordsInCurrentFile;
			Record[] recordsToWrite;
			synchronized(recordsToPersist){
				recordsToWrite = new Record[((numRecordsToWrite > recordsToPersist.size()) ? recordsToPersist.size() : numRecordsToWrite)];
				for(int x=0; x<recordsToWrite.length; x++){
					recordsToWrite[x] = recordsToPersist.removeFirst();
				}
			}
			if(firstRecordInFileTimestamp==-1){
				firstRecordInFileTimestamp = recordsToWrite[0].getTimestamp();
				firstRecordInFilePreviousTimestamp = recordsToWrite[0].getPreviousTimestamp();
			}
			lastRecordInFileTimestamp = recordsToWrite[recordsToWrite.length - 1].getTimestamp();
			lastRecordInFilePreviousTimestamp = recordsToWrite[recordsToWrite.length - 1 ].getPreviousTimestamp();
			for(int x=0; x<recordsToWrite.length; x++){
				try {
					currentOutputFileStream.writeObject((recordsToWrite[x]));
					LOG.debug(producerId + " persisted record " + recordsToWrite[x].toString());
					recordsInCurrentFile++;
					persistedRecords.incrementAndGet();
					ret[0]++;
				} catch (Exception e) {
					persistanceFailures.incrementAndGet();
					ret[1]++;
					if(System.currentTimeMillis() >= lastWriteFailureLogTime + writeFailureLogFrequency){
						LOG.error("LocalFileSystemPersistor-" + System.identityHashCode(this) + ": Failed to write " + recordsToWrite.length + " records to output file " + tempOutputFilePath, e);
						LOG.error("LocalFileSystemPersistor-" + System.identityHashCode(this) + ": This has occurred " + numWriteFailuresSinceLastLog + " times in the last " + (System.currentTimeMillis() - lastWriteFailureLogTime) + " ms");
						lastWriteFailureLogTime = System.currentTimeMillis();
						numWriteFailuresSinceLastLog = 0;
					} else {
						numWriteFailuresSinceLastLog++;
					}
				}
			}
		}
		try {
			if(currentOutputFileStream != null){
				currentOutputFileStream.flush();
				currentOutputFileStream.reset();
			}
		} catch (Exception e) {
			LOG.error("LocalFileSystemPersistor-" + System.identityHashCode(this) + ": Ouptut records may have been lost, caught exception while flushing output stream to " + tempOutputFilePath, e);
		}
		if(!shutdown){
			try {
				setOutputFile();
			} catch (Exception e) {
				LOG.error("Failed to setup output file " + tempOutputFilePath, e);
			}
		}
		lastFlushTime = System.currentTimeMillis();
		return ret;
	}
	
	public void setOutputFile() throws Exception{
		if (currentOutputFile != null &&
			recordsInCurrentFile >= maxRecordsPerFile){
			currentOutputFileStream.close();
			currentOutputFileStream = null;
			currentOutputFile.renameTo(new File(tempOutputFilePath + "_" + 
												firstRecordInFilePreviousTimestamp + "_" + 
												firstRecordInFileTimestamp + "_" + 
												lastRecordInFilePreviousTimestamp + "_" + 
												lastRecordInFileTimestamp));
			currentOutputFile = null;
		}
		if (currentOutputFile == null){
			currentOutputFile = new File(tempOutputFilePath);
			try {
				currentOutputFile.createNewFile();
			} catch (Exception e) {
				throw new Exception("Failed to create output file " + tempOutputFilePath, e);
			}
			try {
				currentOutputFileStream = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(tempOutputFilePath)));
			} catch (Exception e) {
				throw new Exception("LocalFileSystemPersistor-" + System.identityHashCode(this) + ": Failed to create output file stream for path " + tempOutputFilePath, e);
			}
			recordsInCurrentFile = 0;
			firstRecordInFilePreviousTimestamp = -1;
			firstRecordInFileTimestamp = -1;
			lastRecordInFilePreviousTimestamp = -1;
			lastRecordInFileTimestamp = -1;
		}
	}
	
	
	public void setLocalFileSystemPersistanceManager(LocalFileSystemPersistanceManager localFileSystemPersistanceManager) throws Exception{
		if(localFileSystemPersistanceManager.hasPersistor(this)){
			this.localFileSystemPersistanceManager = localFileSystemPersistanceManager;
		} else {
			throw new Exception("LocalFileSystemPersistor-" + System.identityHashCode(this) + ": Received a request to set LocalFileSystemPersistanceManager to " + 
								System.identityHashCode(localFileSystemPersistanceManager) + " but this is not registered with it. " + 
								"The current LocalFileSystemPersistanceManager for this is " + 
								((this.localFileSystemPersistanceManager==null) ? "null" : System.identityHashCode(this.localFileSystemPersistanceManager)));
		}
	}
	
	public String getId(){
		return producerId;
	}
	
    public void persist(Record record){
		synchronized(recordsToPersist){
			recordsToPersist.add(record);
		}
		synchronized(localFileSystemPersistanceManager.newRecordToPersist){
			localFileSystemPersistanceManager.newRecordToPersist.notify();
		}
    }
    
    
}