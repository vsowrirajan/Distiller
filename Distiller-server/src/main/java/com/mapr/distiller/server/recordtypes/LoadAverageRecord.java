package com.mapr.distiller.server.recordtypes;

import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class LoadAverageRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(LoadAverageRecord.class);
	private static final long serialVersionUID = Constants.SVUID_LOAD_AVERAGE_RECORD;
	/**
	 * DERIVED VALUES
	 * These are variables that are not sourced directly from /proc
	 * These values are computed only for records returned by calls to a diff function of this class
	 */
	
	/**
	 * RAW VALUES
	 */
	private double loadAverage1Min, loadAverage5Min, loadAverage15Min;
	private int numExecutingEntities, numSchedulableEntities, lastPid;
	
	@Override
	public String getRecordType(){
		return Constants.LOAD_AVERAGE_RECORD;
	}
	

	/**
	 * CONSTRUCTORS
	 */
	public LoadAverageRecord() throws Exception {
		super(System.currentTimeMillis());
		RandomAccessFile proc_loadavg = null;
		try {
			proc_loadavg = new RandomAccessFile("/proc/loadavg", "r");
			String[] parts = proc_loadavg.readLine().split("\\s+");

			if (parts.length < 5) {
				throw new Exception("/proc/loadavg is expected to have at least 5 fields, found " + parts.length);
			}
			this.loadAverage1Min = Double.parseDouble(parts[0]);
			this.loadAverage5Min = Double.parseDouble(parts[1]);
			this.loadAverage15Min = Double.parseDouble(parts[2]);
			this.numExecutingEntities = Integer.parseInt(parts[3].split("/")[0]);
			this.numSchedulableEntities = Integer.parseInt(parts[3].split("/")[1]);
			this.lastPid = Integer.parseInt(parts[4]);
		} catch (Exception e) {
			throw new Exception("Failed to generate LoadAverageRecord", e);
		} finally {
			try {
				proc_loadavg.close();
			} catch (Exception e) {}
		}
	}
	public LoadAverageRecord(LoadAverageRecord rec1, LoadAverageRecord rec2) throws Exception {
		//Figure out the time sequence of the records
		LoadAverageRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		if(oldRecord.getPreviousTimestamp()==-1 && newRecord.getPreviousTimestamp()==-1){
			this.setTimestamp(newRecord.getTimestamp());
			this.setPreviousTimestamp(oldRecord.getTimestamp());
			this.loadAverage1Min = newRecord.getLoadAverage1Min();
			this.loadAverage5Min = newRecord.getLoadAverage5Min();
			this.loadAverage15Min = newRecord.getLoadAverage15Min();
		} else if(oldRecord.getTimestamp() != newRecord.getPreviousTimestamp()){
			throw new Exception("Can not merge non-chronologically consecutive LoadAverageRecords");
		} else {
			this.setTimestamp(newRecord.getTimestamp());
			this.setPreviousTimestamp(oldRecord.getPreviousTimestamp());
			this.loadAverage1Min = ( ( newRecord.getLoadAverage1Min() * (double)newRecord.getDurationms() ) + 
								     ( oldRecord.getLoadAverage1Min() * (double)oldRecord.getDurationms() )
								   )
								   /
								   ((double)( newRecord.getDurationms() + oldRecord.getDurationms()));
			this.loadAverage5Min = ( ( newRecord.getLoadAverage5Min() * (double)newRecord.getDurationms() ) + 
				     				 ( oldRecord.getLoadAverage5Min() * (double)oldRecord.getDurationms() )
								   )
								   /
								   ((double)( newRecord.getDurationms() + oldRecord.getDurationms()));
			this.loadAverage15Min = ( ( newRecord.getLoadAverage15Min() * (double)newRecord.getDurationms() ) + 
				     				  ( oldRecord.getLoadAverage15Min() * (double)oldRecord.getDurationms() )
									)
									/
									((double)( newRecord.getDurationms() + oldRecord.getDurationms()));
		}
		this.lastPid = newRecord.getLastPid();
		this.numSchedulableEntities = newRecord.getNumSchedulableEntities();
		this.numExecutingEntities = newRecord.getNumExecutingEntities();
		
	}
	/**
	 * PRODUCE RECORD METHODS
	 */
	public static int[] produceRecord(RecordQueue outputQueue, String producerName){
		int[] ret = new int[] {0, 0, 0, 0};
		LoadAverageRecord record = null;
		try {
			record = new LoadAverageRecord();
		} catch (Exception e) {
			LOG.error("Failed to generate a LoadAverageRecord", e);
			ret[2] = 1;
		}
		if(record != null && !outputQueue.put(producerName, record)){
			ret[3] = 1;
			LOG.error("Failed to put LoadAverageRecord into output queue " + outputQueue.getQueueName() + 
					" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.getQueueRecordCapacity() + 
					" producerName:" + producerName);
		} else {
			ret[1]=1;
		}	
		return ret;
	}
	
	/**
	 * OTHER METHODS
	 */
	public String toString(){
		return super.toString() + " LoadAverage" + 
				" 1min: " + loadAverage1Min + " 5min: " + loadAverage5Min + 
				" 15min: " + loadAverage15Min + " numExce: " + numExecutingEntities + 
				" schedulable: " + numSchedulableEntities + " lastPid:" + lastPid;
	}
	
	public double getLoadAverage1Min(){
		return loadAverage1Min;
	}

	public double getLoadAverage5Min(){
		return loadAverage5Min;
	}

	public double getLoadAverage15Min(){
		return loadAverage15Min;
	}

	public int getNumExecutingEntities(){
		return numExecutingEntities;
	}
	
	public int getNumSchedulableEntities(){
		return numSchedulableEntities;
	}
	
	public int getLastPid(){
		return lastPid;
	}

}
