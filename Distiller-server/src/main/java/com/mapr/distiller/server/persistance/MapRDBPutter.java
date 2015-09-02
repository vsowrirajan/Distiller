package com.mapr.distiller.server.persistance;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.persistance.MapRDBSyncPersistanceManager;
import com.mapr.distiller.server.persistance.MapRDBSyncPersistor;

public class MapRDBPutter extends Thread{
	private static final Logger LOG = LoggerFactory.getLogger(MapRDBPutter.class);
	
	MapRDBSyncPersistanceManager maprdbSyncPersistanceManager;
	boolean shouldExit;
	
	public MapRDBPutter(MapRDBSyncPersistanceManager maprdbSyncPersistanceManager){
		this.maprdbSyncPersistanceManager = maprdbSyncPersistanceManager;
		this.shouldExit = false;
	}
	
	public void run(){
		while (!shouldExit){
			Object[] nextPutObjects = null;
			synchronized(maprdbSyncPersistanceManager.newRecordToBePut){
				while(nextPutObjects == null){
					nextPutObjects = maprdbSyncPersistanceManager.getNextPut();
					if(nextPutObjects == null){
						try {
							maprdbSyncPersistanceManager.newRecordToBePut.wait();
						} catch (Exception e) {}
					}
				}
			}
			//We have a Record to put...
			Long putStartTime = System.currentTimeMillis();
			MapRDBSyncPersistor p = (MapRDBSyncPersistor)(nextPutObjects[0]);
			Record r = (Record)(nextPutObjects[1]);
				
			//Check whether the table/family exists
			p.checkTableFamily();
			
			if(p.getTableFamilyExists()){
				//Proceed with the Put
				boolean serializationSucceeded=false;
				byte[] serializedRecord = null;
				try {
					serializedRecord = serializeRecord(r);
					serializationSucceeded = true;
				} catch (Exception e) {
					Object[] ret = maprdbSyncPersistanceManager.tryToSetLastSerializationFailureLogTime(System.currentTimeMillis());
					if(ret != null){
						LOG.error("MapRDBPutter-" + System.identityHashCode(this) + ": Failed to serialize a Record for " + p.getId(), e);
						LOG.error("MapRDBPutter-" + System.identityHashCode(this) + ": There have been " + ((Integer)(ret[1])).intValue() +
								" serialization failures with suppressed log messages in the last " + (System.currentTimeMillis() - ((Long)(ret[0])).longValue()) + " ms");
					}
					p.incSerializationFailures();
					maprdbSyncPersistanceManager.incSerializationFailures();
					serializationSucceeded = false;
				}
				if(serializationSucceeded){
					p.addRecordInFlight(r);
					boolean persistSucceeded=false;
					try {
						persist(p, serializedRecord, ("t" + r.getTimestamp() + "p" + r.getPreviousTimestamp()).getBytes());
						persistSucceeded = true;
					} catch (Exception e) {
						Object[] ret = maprdbSyncPersistanceManager.tryToSetLastPersistFailureLogTime(System.currentTimeMillis());
						if(ret != null){
							LOG.error("MapRDBPutter-" + System.identityHashCode(this) + ": Failed to persist a Record for " + p.getId(), e);
							LOG.error("MapRDBPutter-" + System.identityHashCode(this) + ": There have been " + ((Integer)(ret[1])).intValue() +
									" persist failures with suppressed log messages in the last " + (System.currentTimeMillis() - ((Long)(ret[0])).longValue()) + " ms");
						}
						persistSucceeded = false;
					}
					//LOG.debug("MapRDBPutter-" + System.identityHashCode(this) + ": Put completed " + ((persistSucceeded) ? "successfully" : "unsuccesfully") + " after " + (System.currentTimeMillis() - putStartTime));
					if(persistSucceeded){
						persistSucceeded = false;
						try {
							persist(p, new byte[0], ("p" + r.getPreviousTimestamp() + "t" + r.getTimestamp()).getBytes());
							persistSucceeded = true;
						} catch (Exception e) {
							Object[] ret = maprdbSyncPersistanceManager.tryToSetLastPersistFailureLogTime(System.currentTimeMillis());
							if(ret != null){
								LOG.error("MapRDBPutter-" + System.identityHashCode(this) + ": Failed to persist a Record for " + p.getId(), e);
								LOG.error("MapRDBPutter-" + System.identityHashCode(this) + ": There have been " + ((Integer)(ret[1])).intValue() +
										" persist failures with suppressed log messages in the last " + (System.currentTimeMillis() - ((Long)(ret[0])).longValue()) + " ms");
							}
							persistSucceeded = false;
						}
					}
					p.removeRecordInFlight(r);
					if(persistSucceeded){
						p.incPersistedRecords();
						maprdbSyncPersistanceManager.incPersistedRecords();
					} else {
						p.incPersistanceFailures();
						maprdbSyncPersistanceManager.incPersistanceFailures();
						if(p.getMapRDBWorkDirEnabled()){
							p.addDiskBoundRecord(r);
						} else {
							p.incDroppedRecords();
							maprdbSyncPersistanceManager.incDroppedRecords();
						}
					}
				}
			} else {
				//The table/family doesn't exist (right now)
				//We can not persist
				//Either drop the record or queue it for disk
				if(p.getMapRDBWorkDirEnabled()){
					p.incStraightToDiskRecords();
					p.addDiskBoundRecord(r);
				} else {
					p.incDroppedRecords();
					maprdbSyncPersistanceManager.incDroppedRecords();
				}
			}
		}
	}
		
	private byte[] serializeRecord(Record r) throws Exception{
		ObjectOutput oo = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			oo = new ObjectOutputStream(baos);
			oo.writeObject(r);
			return baos.toByteArray();
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				oo.close();
			} catch (Exception e) {}
		}
	}
	
	private void persist(MapRDBSyncPersistor persistor, byte[] record, byte[] rowKey) throws Exception{
		Put p = new Put(rowKey);
		p.add(	maprdbSyncPersistanceManager.getColumnFamilyBytes(), 
				maprdbSyncPersistanceManager.getColumnQualifierBytes(),
				record );
		//This will either succeed or throw an exception
		LOG.info("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Calling put for " + System.identityHashCode(p));
		persistor.callPut(p);
		LOG.info("MapRDBSyncPersistor-" + System.identityHashCode(this) + ": Put returned normally for " + System.identityHashCode(p));
		
	}
}
