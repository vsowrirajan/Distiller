package com.mapr.distiller.server.persistance;

import com.mapr.distiller.server.persistance.MapRDBSyncPersistanceManager;
import com.mapr.distiller.server.recordtypes.Record;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRDBReplayer extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(MapRDBReplayer.class);
	
	MapRDBSyncPersistanceManager manager;
	boolean shouldExit;
	public MapRDBReplayer(MapRDBSyncPersistanceManager manager){
		this.manager = manager;
	}
	
	public void requestExit(){
		shouldExit = true;
	}
	
	public void run(){
		shouldExit = false;
		while(!shouldExit){
			LOG.info("Checking for work dir files to replay");
			replayFiles();
			try {
				Thread.sleep(60000);
			} catch (Exception e){}
		}
	}
	
	private HTable checkTable(String tablePath){
		HTable table = null;
		try {
			table = new HTable(manager.getHBaseConfiguration(), tablePath);
			table.setAutoFlush(false);
		} catch (Exception e) {
			LOG.error("Failed to construct HTable instance for " + tablePath, e);
			try {
				table.close();
			} catch (Exception e2){}
			return null;
		}
		HTableDescriptor d = null;
		try {
			d = table.getTableDescriptor();
			if(d.hasFamily(manager.getColumnFamilyBytes())){
				return table;
			} else {
				LOG.error("TableDescriptor for table " + tablePath + " does not contain required column family " + manager.getColumnFamily());
				return null;
			}
		} catch (Exception e) {
			LOG.error("Failed to get table descriptor for table " + tablePath, e);
			try {
				table.close();
			} catch (Exception e2){}
			return null;
		}
	}
	
	private byte[] serializeRecord(Record r) throws Exception{
		ObjectOutput oo = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			oo = new ObjectOutputStream(baos);
			oo.writeObject(r);
			return baos.toByteArray();
		} finally {
			try {
				oo.close();
			} catch (Exception e) {}
		}
	}
	
	private void persist(HTable table, byte[] record, byte[] rowKey) throws Exception{
		Put p = new Put(rowKey);
		p.add(	manager.getColumnFamilyBytes(), 
				manager.getColumnQualifierBytes(),
				record );
		//This will either succeed or throw an exception
		table.put(p);
	}
	
	public void persistRecords(ObjectInput input, int numRecordsToPersist, HTable outputTable) throws Exception{
		for(int x=0; x<numRecordsToPersist; x++){
			Record recordToPut = null;
			try {
				recordToPut = (Record)(input.readObject());
				byte[] serializedRecord = null;
				try {
					serializedRecord = serializeRecord(recordToPut);
				} catch (Exception e ){
					throw new Exception("Failed to serialize record", e);
				}
				try {
					persist(outputTable, serializedRecord, ("t" + recordToPut.getTimestamp() + "p" + recordToPut.getPreviousTimestamp()).getBytes());
				} catch (Exception e) {
					throw new Exception("Failed to persist primary record to MapRDB table", e);
				}
				try {
					persist(outputTable, new byte[0], ("p" + recordToPut.getPreviousTimestamp() + "t" + recordToPut.getTimestamp()).getBytes());
				} catch (Exception e) {
					throw new Exception("Failed to persist previousTimestamp lookup record to MapRDB table", e);
				}
			} catch (Exception e){
				throw new Exception("Failed to persist record " + (x+1), e);
			}
		}
	}
	
	private void replayFiles(){
		String workDirPath = manager.getMapRDBLocalWorkDirPath();
		File workDir = new File(workDirPath);
		String[] entries = null;
		try {
			entries = workDir.list();
		} catch (Exception e){
			LOG.error("Failed to retrieve directory listing for path " + workDirPath, e);
			return;
		}
		for(String entry : entries){
			String[] subStr = entry.split("_");
			int numRecordsInFile = -1;
			try {
				 numRecordsInFile = Integer.parseInt(subStr[3]);
			} catch (Exception e){
				numRecordsInFile = -1;
			}
			if(subStr.length != 5){
				LOG.error("Ignoring file in work dir " + workDirPath + " with unknown name format: " + entry);
			} else if(numRecordsInFile == -1){
				LOG.error("Could not parse file name " + entry + " in work dir " + workDirPath);
			}else if(!entry.endsWith(".tmp")){
				ObjectInput input = null;
				boolean deleteInputFile = true;
				
				try {
					input = new ObjectInputStream(new GZIPInputStream(new FileInputStream(workDirPath + "/" + entry)));
					String tablePath = null;
					try {
						tablePath = (String)(input.readObject());
					} catch (Exception e){
						throw new Exception("Failed to read table path from file", e);
					}
					HTable outputTable = checkTable(tablePath);
					if(outputTable == null){
						deleteInputFile = false;
						LOG.error("Skipping replay of file " + entry + " because table " + tablePath + " is not ready");
						throw new Exception();
					}
					try {
						persistRecords(input, numRecordsInFile, outputTable);
						LOG.info("Replayed " + numRecordsInFile + " records from file " + entry + " in work dir " + workDirPath + " to MapRDB table " + tablePath);
					} catch (Exception e) {
						deleteInputFile = false;
						throw new Exception("Failed to persist records", e);
					} finally {
						try {
							outputTable.close();
						} catch (Exception e){}
					}
				} catch (Exception e) {
					if(deleteInputFile){
						LOG.error("Failed to replay file " + entry + " in work dir " + workDirPath, e);
					}
				}
				try {
					input.close();
				} catch (Exception e2){}
				if(deleteInputFile){
					try {
						(new File(workDirPath + "/" + entry)).delete();
						LOG.info("Deleted local replay file " + entry + " from work dir " + workDirPath);
					} catch (Exception e){
						LOG.error("Failed to delete local replay file " + entry + " from work dir " + workDirPath);
					}
				}
			}
		}
	}
}
