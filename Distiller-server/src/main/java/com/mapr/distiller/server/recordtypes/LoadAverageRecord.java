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
	
	private double getLoadAverage1Min(){
		return loadAverage1Min;
	}

	private double getLoadAverage5Min(){
		return loadAverage5Min;
	}

	private double getLoadAverage15Min(){
		return loadAverage15Min;
	}

	private int getNumExecutingEntities(){
		return numExecutingEntities;
	}
	
	private int getNumSchedulableEntities(){
		return numSchedulableEntities;
	}
	
	private int getLastPid(){
		return lastPid;
	}

}
