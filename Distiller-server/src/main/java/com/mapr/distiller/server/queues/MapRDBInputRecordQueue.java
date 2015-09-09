package com.mapr.distiller.server.queues;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import com.mapr.distiller.server.persistance.MapRDBPersistanceManager;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public class MapRDBInputRecordQueue implements RecordQueue {
	private static final Logger LOG = LoggerFactory
			.getLogger(MapRDBInputRecordQueue.class);
	
	protected String id;
	private ConcurrentHashMap<String, ResultScanner> consumers;
	private ConcurrentHashMap<String, Record> consumerPeekRecord;
	private String tablePath;
	private String scannerType;
	private String scannerStartKey;
	private String scannerEndKey;
	private Object lock = new Object();
	private Configuration hbaseConfiguration;
	private HTable htable;
	private HTableDescriptor htableDescriptor;
	private Scan scan;
	private byte[] columnFamilyBytes;
	private byte[] columnQualifierBytes;
	private boolean tableHasFamily;
	


	public boolean isValidScannerType(String scannerType){
		if(scannerType==null)
			return false;
		if (scannerType.equals(Constants.TIMESTAMP_SCANNER) || 
			scannerType.equals(Constants.PREVIOUS_TIMESTAMP_SCANNER))
			return true;
		return false;
	}
	public MapRDBInputRecordQueue(String id, String tablePath, String scannerType, String scannerStartKey, String scannerEndKey) throws Exception{
		this.tableHasFamily=false;
		this.columnFamilyBytes = Bytes.toBytes(MapRDBPersistanceManager.RECORD_BYTES_COLUMN_FAMILY);
		this.columnQualifierBytes = Bytes.toBytes(MapRDBPersistanceManager.RECORD_BYTES_COLUMN_QUALIFIER);
		this.consumers = new ConcurrentHashMap<String, ResultScanner>();
		this.consumerPeekRecord = new ConcurrentHashMap<String, Record>();
		
		this.id = id;
		this.tablePath = tablePath;
		
		if(scannerType==null){
			this.scannerType = Constants.TIMESTAMP_SCANNER;
		} else if(isValidScannerType(scannerType)){
			this.scannerType = scannerType;
		} else {
			throw new Exception("Unknown MapRDBInputRecordQueue scanner type: " + scannerType);
		}
		
		this.scannerStartKey = scannerStartKey;
		this.scannerEndKey = scannerEndKey;
		
		this.hbaseConfiguration = HBaseConfiguration.create();
		this.htable = null;
		this.htableDescriptor = null;
		
		String prefix = "";
		if(scannerType.equals(Constants.TIMESTAMP_SCANNER))
			prefix="t";
		else if(scannerType.equals(Constants.PREVIOUS_TIMESTAMP_SCANNER))
			prefix="p";
		else
			throw new Exception("Unknown MapRDBInputRecordQueue scanner type: " + scannerType);
		if(scannerStartKey!=null){
			if(scannerEndKey == null){
				this.scan = new Scan(Bytes.toBytes(prefix + scannerStartKey), Bytes.toBytes(prefix + Long.MAX_VALUE));
			} else {
				this.scan = new Scan(Bytes.toBytes(prefix + scannerStartKey), Bytes.toBytes(prefix + scannerEndKey));
			}
		} else if (scannerEndKey != null) {
			this.scan = new Scan(Bytes.toBytes(prefix), Bytes.toBytes(prefix + scannerEndKey));
		} else {
			//Default to scanning for all records of the given type
			this.scan = new Scan(Bytes.toBytes(prefix), Bytes.toBytes(prefix + Long.MAX_VALUE));
		}
		this.scan.addColumn(Bytes.toBytes(MapRDBPersistanceManager.RECORD_BYTES_COLUMN_FAMILY), 
					Bytes.toBytes(MapRDBPersistanceManager.RECORD_BYTES_COLUMN_QUALIFIER) );
	}
	
	//Return the type of the RecordQueue
	public String getQueueType(){
		return Constants.MAPRDB_INPUT_RECORD_QUEUE;
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
		Record recordToReturn = null;
		synchronized(lock){
			while(!tableHasFamily){
				try {
					htable = new HTable(this.hbaseConfiguration, this.tablePath);
				} catch (Exception e) {
					LOG.error("MapRDBInputRecordQueue-" + System.identityHashCode(this) + ": Failed to initialize HTable instance for " + tablePath, e);
					try {
						htable.close();
					} catch (Exception e2) {}
					htable = null;
				}
				if(htable==null && !blocking){
					return null;
				} else if(htable!=null){
					htableDescriptor=null;
					try {
						htableDescriptor = htable.getTableDescriptor();
					} catch (Exception e) {
						LOG.error("MapRDBInputRecordQueue-" + System.identityHashCode(this) + ": Failed to getTableDescriptor for " + tablePath, e);
						htableDescriptor=null;
						try {
							htable.close();
						} catch (Exception e2){}
						htable = null;
					}
					if(htableDescriptor==null){
						try {
							htable.close();
						} catch (Exception e) {}
						htable=null;
						if(!blocking){
							return null;
						}
					} else {
						if(!htableDescriptor.hasFamily(Bytes.toBytes(MapRDBPersistanceManager.RECORD_BYTES_COLUMN_FAMILY))) {
							tableHasFamily=false;
							try {
								htable.close();
							} catch (Exception e) {}
							htable=null;
							htableDescriptor=null;
							throw new Exception("Specified input table " + tablePath + " does not have column family " + MapRDBPersistanceManager.RECORD_BYTES_COLUMN_FAMILY);
						} else {
							tableHasFamily = true;
						}
					}
				}
				try {
					Thread.sleep(1000);
				} catch (Exception e) {}
			}
			recordToReturn = consumerPeekRecord.remove(subscriber);
			if(recordToReturn != null)
				return recordToReturn;
			Result result = null;
			try {
				result = consumers.get(subscriber).next();
			} catch (Exception e) {
				if(!consumers.containsKey(subscriber))
					throw new Exception("Consumer is not registered with this queue: " + subscriber);
			}
			if(result==null){
				if(blocking){
					throw new Exception("All matching rows from MapRDB table have been retrieved by consumer " + subscriber);
				} else {
					return null;
				}
			}

			byte[] ba = result.getValue(columnFamilyBytes, columnQualifierBytes);
			ByteArrayInputStream bais = new ByteArrayInputStream(ba);
			ObjectInputStream ois = new ObjectInputStream(bais);
			Object o = ois.readObject();
			recordToReturn = (Record)o;
			return recordToReturn;
		}
	}
	
	//Perform a peek at the next sequential Record for the specific consumer, either blocking or non blocking as specified, return null for non-blocking requests where no records are available
	public Record peek(String subscriber, boolean blocking) throws Exception{
		Record recordToReturn = null;
		synchronized(lock){
			while(!tableHasFamily){
				try {
					htable = new HTable(this.hbaseConfiguration, this.tablePath);
				} catch (Exception e) {
					LOG.error("MapRDBInputRecordQueue-" + System.identityHashCode(this) + ": Failed to initialize HTable instance for " + tablePath, e);
					try {
						htable.close();
					} catch (Exception e2) {}
					htable = null;
				}
				if(htable==null && !blocking){
					return null;
				} else if(htable!=null){
					htableDescriptor=null;
					try {
						htableDescriptor = htable.getTableDescriptor();
					} catch (Exception e) {
						LOG.error("MapRDBInputRecordQueue-" + System.identityHashCode(this) + ": Failed to getTableDescriptor for " + tablePath, e);
						htableDescriptor=null;
						try {
							htable.close();
						} catch (Exception e2){}
						htable = null;
					}
					if(htableDescriptor==null){
						try {
							htable.close();
						} catch (Exception e) {}
						htable=null;
						if(!blocking){
							return null;
						}
					} else {
						if(!htableDescriptor.hasFamily(Bytes.toBytes(MapRDBPersistanceManager.RECORD_BYTES_COLUMN_FAMILY))) {
							tableHasFamily=false;
							try {
								htable.close();
							} catch (Exception e) {}
							htable=null;
							htableDescriptor=null;
							throw new Exception("Specified input table " + tablePath + " does not have column family " + MapRDBPersistanceManager.RECORD_BYTES_COLUMN_FAMILY);
						} else {
							tableHasFamily = true;
						}
					}
				}
				try {
					Thread.sleep(1000);
				} catch (Exception e) {}
			}
			recordToReturn = consumerPeekRecord.get(subscriber);
			if(recordToReturn != null)
				return recordToReturn;
			Result result = null;
			try {
				result = consumers.get(subscriber).next();
			} catch (Exception e) {
				if(!consumers.containsKey(subscriber))
					throw new Exception("Consumer is not registered with this queue: " + subscriber);
			}
			if(result==null){
				if(blocking){
					throw new Exception("All matching rows from MapRDB table have been retrieved by consumer " + subscriber);
				} else {
					return null;
				}
			}
			//This seems pretty trashy... but in the interest of time... so it is...
			recordToReturn = (Record)(new ObjectInputStream(new ByteArrayInputStream(result.getValue(columnFamilyBytes, columnQualifierBytes)))).readObject();
			consumerPeekRecord.put(subscriber,  recordToReturn);
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
			Iterator<Map.Entry<String, ResultScanner>> i = consumers.entrySet().iterator();
			for (int x = 0; x < consumers.size(); x++) {
				Map.Entry<String, ResultScanner> pair = (Map.Entry<String, ResultScanner>) i
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
	
	//Returns true if the consumer with the given name is successfully registered as a consumer
	public boolean registerConsumer(String consumer){
		synchronized(lock){
			if (consumer == null || consumer.equals("")) {
				LOG.error("MapRDBInputRecordQueue-" + System.identityHashCode(this) + ": Subscription request received for null subscriber name.");
				return false;
			}
			synchronized (lock) {
				if (!consumers.containsKey(consumer)) {
					ResultScanner scanner = null;
					try {
						scanner = htable.getScanner(scan);
					} catch (Exception e) {
						try {
							scanner.close();
						} catch (Exception e2){}
						return false;
					}
					consumers.put(consumer, scanner);
					return true;
				} else {
					LOG.error("MapRDBInputRecordQueue-" + System.identityHashCode(this) + ": Duplicate subscription request received for "
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
			consumers.remove(consumer);
			consumerPeekRecord.remove(consumer);
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
