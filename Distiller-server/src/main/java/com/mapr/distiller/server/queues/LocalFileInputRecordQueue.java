package com.mapr.distiller.server.queues;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

import java.io.ObjectInputStream;
import java.io.FileInputStream;
import java.io.File;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.io.EOFException;

public class LocalFileInputRecordQueue implements RecordQueue {
	
	private class ConsumerInputPosition{
		public int fileNum;
		public ObjectInputStream inputStream;
	}
	
	private static final Logger LOG = LoggerFactory
			.getLogger(LocalFileInputRecordQueue.class);
	
	protected String id;
	private HashMap<String, ConsumerInputPosition> consumers;
	private HashMap<String, Record> consumerPeekRecord;
	private HashMap<String, Long> consumerLastReturnedRecordTimestamp;
	private TreeSet<String> consumerFinished;
	private String metricName;
	private String inputDirPath;
	private File inputDir;
	private String[] inputFiles;
	private String scannerType;
	private long startTimestamp;
	private long endTimestamp;
	private AtomicLong skippedRecords;
	private AtomicLong returnedRecords;
	private Object lock = new Object();	

	public long getSkippedRecords(){
		return skippedRecords.get();
	}
	
	public long getReturnedRecords(){
		return returnedRecords.get();
	}
	
	public boolean isValidScannerType(String scannerType){
		if(scannerType==null)
			return false;
		//if (scannerType.equals(Constants.TIMESTAMP_SCANNER) || 
		//	scannerType.equals(Constants.PREVIOUS_TIMESTAMP_SCANNER))
		if (scannerType.equals(Constants.TIMESTAMP_SCANNER))
			return true;
		return false;
	}
	public LocalFileInputRecordQueue(String id, String metricName, String inputDirPath, String scannerType, long startTimestamp, long endTimestamp) throws Exception{
		this.consumers = new HashMap<String, ConsumerInputPosition>();
		this.consumerPeekRecord = new HashMap<String, Record>();
		this.consumerLastReturnedRecordTimestamp = new HashMap<String, Long>();
		this.consumerFinished = new TreeSet<String>();
		this.skippedRecords = new AtomicLong(0);
		this.returnedRecords = new AtomicLong(0);
		
		if(scannerType == null){
			this.scannerType = Constants.TIMESTAMP_SCANNER;
		} else if(isValidScannerType(scannerType)){
			this.scannerType = scannerType;
		} else {
			throw new Exception("Unknown scanner type: " + scannerType);
		}
		if(endTimestamp < 1)
			this.endTimestamp = Long.MAX_VALUE;
		else
			this.endTimestamp = endTimestamp;
		if(startTimestamp > this.endTimestamp){
			throw new Exception("Start timestamp for scanner is greater than end timestamp. start: " + startTimestamp + " end: " + endTimestamp);
		}
		this.startTimestamp = startTimestamp;
		
		
		this.id = id;
		this.metricName = metricName;
		
		this.inputDirPath = inputDirPath;
		this.inputDir = new File(inputDirPath);
		
		if(!this.inputDir.isDirectory()){
			throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Input path is not a directory: " + inputDirPath);
		}
		
		TreeMap<Long, String> inputFileMap = new TreeMap<Long, String>();
		
		String[] dirEntries = null;
		try {
			dirEntries = this.inputDir.list();
		} catch (Exception e){
			throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Failed to retrieve directory list for " + inputDirPath, e);
		}
		
		/**
		 * Make a TreeMap<long, String>() to map timestamp of first record in a file to it's filename.
		 * As records are read, keep track of the timestamp from the most recently read record
		 * When the last record is read from some particular file and the input file needs to be advanced,
		 * open the next file and skip records in the file until a record is found with a timestamp 
		 * greater than that of the last record retrieved.
		 */
		for (String entry : dirEntries){
			String[] subStr = entry.split("_");
			if((subStr.length != 8  && subStr.length != 4) || !subStr[1].equals(this.metricName)){
				LOG.debug("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Ignoring file: " + entry);
			} else {
				ObjectInputStream s = null;
				try {
					s = new ObjectInputStream(new GZIPInputStream(new FileInputStream(inputDirPath + "/" + entry)));
					Record r = ((Record)(s.readObject()));
					inputFileMap.put(new Long(r.getTimestamp()), entry);
					LOG.info("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Adding input file " + entry + " with timestamp " + r.getTimestamp());
				} catch (Exception e){
					LOG.warn("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Failed to read file " + entry + ", omitting it from further processing.", e);
				}
			}
		}
		/**
		if(inputFileMap.size()==0){
			throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Did not find any input files matching metric " + 
					this.metricName + " in input dir " + this.inputDirPath);
		}
		**/
		this.inputFiles = inputFileMap.values().toArray(new String[0]);
	}
	
	//Return the type of the RecordQueue
	public String getQueueType(){
		return Constants.LOCAL_FILE_INPUT_RECORD_QUEUE;
	}
	
	//Return the qualifier key used by the RecordQueue
	public String getQueueQualifierKey(){
		return null;
	}
	
	//Return the name of the RecordQueue
	public String getQueueName(){
		return id;
	}
	
	//Return the queue capacity in number of Records
	public int getQueueRecordCapacity(){
		return 0;
	}
		
	//Return the queue capacity in number of seconds
	public int getQueueTimeCapacity(){
		return 0;
	}
		
	//Return the number of elements in the queue.
	public int queueSize(){
		return -1;
	}

	public int queueSize(String subscriber){
		return -1;
	}
	
	//Add a Record onto the end of the queue
	public boolean put(String producer, Record record){
		return false;
	}

	//Perform a blocking get for the next sequential Record for the specific subscriber
	public Record get(String subscriber) throws Exception{
		return get(subscriber, true);
	} 

	//Perform a get for the next sequential Record for the specific consumer, either blocking or non blocking as specified, return null for non-blocking requests where no records are available
	public Record get(String subscriber, boolean blocking) throws Exception{
		synchronized(lock){
			if(consumerPeekRecord.get(subscriber) != null){
				return consumerPeekRecord.remove(subscriber);
			}
			if(consumerFinished.contains(subscriber)){
				if(blocking){
					throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Consumer " + subscriber + " has read all input records.");
				}
				return null;
			}
			Record recordToReturn = null;
			while(recordToReturn == null){
				ConsumerInputPosition pos = consumers.get(subscriber);
				try {
					while(true){
						recordToReturn = (Record)(pos.inputStream.readObject());
						if 
						(
							recordToReturn.getTimestamp() >= consumerLastReturnedRecordTimestamp.get(subscriber).longValue()
							&&
							recordToReturn.getTimestamp() >= startTimestamp
							&&
							recordToReturn.getTimestamp() <= endTimestamp
						)
						{
							returnedRecords.incrementAndGet();
							consumerLastReturnedRecordTimestamp.put(subscriber,  new Long(recordToReturn.getTimestamp()));
							break;
						} else {
							skippedRecords.incrementAndGet();
						}
					}
				} catch (EOFException eof) {
					//Do nothing, the file will be rotated to the next one.
				} catch (Exception e){
					recordToReturn = null;
					LOG.debug("Exception while reading object for consumer " + subscriber + " from file " + ((inputFiles.length > pos.fileNum && pos.fileNum != -1) ? inputFiles[pos.fileNum] : "null"));
				}
				if(recordToReturn == null){
					boolean openedNewFile = false;
					while(!openedNewFile){
						if(pos.fileNum + 1 == inputFiles.length){
							consumers.put(subscriber, pos);
							consumerFinished.add(subscriber);
							LOG.info("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Consumer " + subscriber + " has read all input records.");
							if(blocking){
								throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Consumer " + subscriber + " has read all input records.");
							} else {
								return null;
							}
						}
						pos.fileNum++;
						try {
							pos.inputStream.close();
						} catch (Exception e){}
						try {
							pos.inputStream = getLocalFileInputStream(pos.fileNum);
							openedNewFile = true;
						} catch (Exception e){
							try {
								pos.inputStream.close();
							} catch (Exception e2){}
							pos.inputStream = null;
							LOG.error("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Failed to open input file, skipping it", e);
						}
					}
					LOG.debug("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Consumer " + subscriber + 
							" has begun processing file " + pos.fileNum + ": "+ inputFiles[pos.fileNum]);
					consumers.put(subscriber, pos);
				}
			}
			return recordToReturn;
		}
	}
	
	//Perform a peek at the next sequential Record for the specific consumer, either blocking or non blocking as specified, return null for non-blocking requests where no records are available
	public Record peek(String subscriber, boolean blocking) throws Exception{
		if(consumerFinished.contains(subscriber)){
			if(blocking){
				throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Consumer " + subscriber + " has read all input records.");
			}
			return null;
		}
		Record recordToReturn = null;
		synchronized(lock){
			if(consumerPeekRecord.get(subscriber) != null){
				return consumerPeekRecord.get(subscriber);
			}
			while(recordToReturn == null){
				ConsumerInputPosition pos = consumers.get(subscriber);
				try {
					while(true){
						recordToReturn = (Record)(pos.inputStream.readObject());
						if 
						(
							recordToReturn.getTimestamp() >= consumerLastReturnedRecordTimestamp.get(subscriber).longValue()
							&&
							recordToReturn.getTimestamp() >= startTimestamp
							&&
							recordToReturn.getTimestamp() <= endTimestamp
						)
						{
							returnedRecords.incrementAndGet();
							consumerLastReturnedRecordTimestamp.put(subscriber,  new Long(recordToReturn.getTimestamp()));
							break;
						} else {
							skippedRecords.incrementAndGet();
						}
					}
				} catch (EOFException eof) {
					//Do nothing, the file will be rotated to the next one.
				} catch (Exception e){
					recordToReturn = null;
				}
				if(recordToReturn == null){
					boolean openedNewFile = false;
					while(!openedNewFile){
						if(pos.fileNum + 1 == inputFiles.length){
							consumers.put(subscriber, pos);
							consumerFinished.add(subscriber);
							LOG.info("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Consumer " + subscriber + " has read all input records.");
							if(blocking){
								throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Consumer " + subscriber + " has read all input records.");
							} else {
								return null;
							}
						}
						pos.fileNum++;
						try {
							pos.inputStream.close();
						} catch (Exception e){}
						try {
							pos.inputStream = getLocalFileInputStream(pos.fileNum);
							openedNewFile = true;
						} catch (Exception e){
							try {
								pos.inputStream.close();
							} catch (Exception e2){}
							pos.inputStream = null;
							LOG.error("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Failed to open input file, skipping it", e);
						}
					}
					consumers.put(subscriber, pos);
				}
			}
			consumerPeekRecord.put(subscriber, recordToReturn);
			return recordToReturn;
		}
	}
	
	//Return a String array where each element represents the name of a registered Producer
	public String[] listProducers(){
		return new String[0];
	}

	//Return a String array where each element represents the name of a registered Consumer
	public String[] listConsumers() {
		synchronized(lock){		
			String[] ret = new String[consumers.size()];
			Iterator<Map.Entry<String, ConsumerInputPosition>> i = consumers.entrySet().iterator();
			for (int x = 0; x < consumers.size(); x++) {
				Map.Entry<String, ConsumerInputPosition> pair = (Map.Entry<String, ConsumerInputPosition>) i
						.next();
				ret[x] = pair.getKey();
	
			}
			return ret;
		}
	}
	
	//Returns true if the given name is registered as a producer
	public boolean hasProducer(String name){
		return false;
	}
	
	//Returns a true if the given name is registered as a consumer
	public boolean hasConsumer(String name){
		synchronized(lock){
			return consumers.containsKey(name);
		}
	}
		
	//Return false, this RecordQueue does not support producers
	public boolean registerProducer(String name){
		return false;
	}
	
	//Return an ObjectInputStream for the specified file number.
	private ObjectInputStream getLocalFileInputStream(int fileNum) throws Exception{
		try {
			return new ObjectInputStream(new GZIPInputStream(new FileInputStream(inputDirPath + "/" + inputFiles[fileNum])));
		} catch (Exception e){
			throw new Exception("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Failed to create an ObjectInputStream for " + inputDirPath + "/" + inputFiles[fileNum], e);
		}
	}
	//Returns true if the consumer with the given name is successfully registered as a consumer
	public boolean registerConsumer(String consumer){
		synchronized(lock){
			if (consumer == null || consumer.equals("")) {
				LOG.error("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Subscription request received for null subscriber name.");
				return false;
			}
			synchronized (lock) {
				if (!consumers.containsKey(consumer)) {
					ConsumerInputPosition pos = new ConsumerInputPosition();
					/**
					pos.fileNum = 0;
					try {
						pos.inputStream = getLocalFileInputStream(pos.fileNum);
					} catch (Exception e){
						LOG.error("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Failed to open local input file", e);
						return false;
					}
					consumers.put(consumer, pos);
					**/
					pos.fileNum = -1;
					pos.inputStream = null;
					consumers.put(consumer, pos);
					consumerLastReturnedRecordTimestamp.put(consumer, new Long(-1));
					return true;
				} else {
					LOG.error("LocalFileInputRecordQueue-" + System.identityHashCode(this) + ": Duplicate subscription request received for "
									+ consumer);
				}
			}
			return false;
		}
	}
	
	//Return false, this RecordQueue does not support producers
	public boolean unregisterProducer(String name){
		return false;
	}
	
	//Returns true if the consumer with the given name was successfully removed as a consumer (false if no change)
	public boolean unregisterConsumer(String consumer){
		synchronized(lock){
			if(!consumers.containsKey(consumer))
				return false;
			ConsumerInputPosition pos = consumers.remove(consumer);
			try {
				pos.inputStream.close();
			} catch (Exception e){}
			consumerPeekRecord.remove(consumer);
			consumerFinished.remove(consumer);
			consumerLastReturnedRecordTimestamp.remove(consumer);
			return true;
		}
	}

	//Does not have any meaning for this type of queue
	public long getOldestRecordTimestamp(){
		return -1;
	}
	
	//Iterate through the records in the queue, calling Record.toString() for each Record that the specified consumer can consume
	//What happens if this gets too big???
	public String printRecords(String subscriber){
		return null;
	}
	
	//Iterate through the records in the queue starting from the most recently added Record, calling Record.toString() for each Record that the specified consumer can consume
	public String printNewestRecords(String consumer, int numRecords){
		return null;
	}

}
