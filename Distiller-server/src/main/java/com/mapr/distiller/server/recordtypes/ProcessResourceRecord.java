package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class ProcessResourceRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(ProcessResourceRecord.class);
	private static final long serialVersionUID = Constants.SVUID_PROCESS_RESOURCE_RECORD;
	/**
	 * DERIVED VALUES
	 */
	private double cpuUtilPct, iowaitUtilPct, readIoCallRate, writeIoCallRate, readIoCharRate, writeIoCharRate, readIoByteRate, writeIoByteRate, cancelledWriteIoByteRate;
	
	/**
	 * RAW VALUES
	 */
	private String comm;
	private char state;
	private int pid, ppid, pgrp, num_threads, clockTick;
	private long starttime;
	private BigInteger cguest_time, cmajflt, cminflt, cstime, cutime, delayacct_blkio_ticks, guest_time, majflt, minflt, rss, rsslim, stime, utime, vsize;
	private BigInteger rchar, wchar, syscr, syscw, read_bytes, write_bytes, cancelled_write_bytes;

	@Override
	public String getRecordType(){
		return Constants.PROCESS_RESOURCE_RECORD;
	}
	

	/**
	 * CONSTRUCTORS
	 */
	public ProcessResourceRecord(ProcessResourceRecord rec1, ProcessResourceRecord rec2) throws Exception{
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential ProcessResourceRecord from input records with matching timestamp values");
		if(rec1.get_starttime() != rec2.get_starttime() || rec1.get_pid() != rec2.get_pid())
			throw new Exception("Differential ProcessResourceRecord can only be generated from input records from the same process");
		
		//Organize the input records.
		ProcessResourceRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		
		this.comm = newRecord.get_comm();
		this.state = newRecord.get_state();
		this.pid = newRecord.get_pid();
		this.ppid = newRecord.get_ppid();
		this.pgrp = newRecord.get_pgrp();
		this.num_threads = newRecord.get_num_threads();
		this.clockTick = newRecord.getClockTick();
		this.starttime = newRecord.get_starttime();
		this.rsslim = newRecord.get_rsslim();
		
		//Check if these are raw records
		if(rec1.getCpuUtilPct()==-1d && rec2.getCpuUtilPct()==-1d){
			//Copied values:
			this.setTimestamp(newRecord.getTimestamp());
			this.setPreviousTimestamp(oldRecord.getTimestamp());
			
			//Differential values:
			this.cguest_time = newRecord.get_cguest_time().subtract(oldRecord.get_cguest_time());
			this.cmajflt = newRecord.get_cmajflt().subtract(oldRecord.get_cmajflt());
			this.cminflt = newRecord.get_cminflt().subtract(oldRecord.get_cminflt());
			this.cstime = newRecord.get_cstime().subtract(oldRecord.get_cstime());
			this.cutime = newRecord.get_cutime().subtract(oldRecord.get_cutime());
			this.delayacct_blkio_ticks = newRecord.get_delayacct_blkio_ticks().subtract(oldRecord.get_delayacct_blkio_ticks());
			this.guest_time = newRecord.get_guest_time().subtract(oldRecord.get_guest_time());
			this.majflt = newRecord.get_majflt().subtract(oldRecord.get_majflt());
			this.minflt = newRecord.get_minflt().subtract(oldRecord.get_minflt());
			this.stime = newRecord.get_stime().subtract(oldRecord.get_stime());
			this.utime = newRecord.get_utime().subtract(oldRecord.get_utime());
			this.rchar = newRecord.get_rchar().subtract(oldRecord.get_rchar());
			this.wchar = newRecord.get_wchar().subtract(oldRecord.get_wchar());
			this.syscr = newRecord.get_syscr().subtract(oldRecord.get_syscr());
			this.syscw = newRecord.get_syscw().subtract(oldRecord.get_syscw());
			this.read_bytes = newRecord.get_read_bytes().subtract(oldRecord.get_read_bytes());
			this.write_bytes = newRecord.get_write_bytes().subtract(oldRecord.get_write_bytes());
			this.cancelled_write_bytes = newRecord.get_cancelled_write_bytes().subtract(oldRecord.get_cancelled_write_bytes());


			//Derived values:
			this.rss = newRecord.get_rss().multiply(new BigInteger(Long.toString(this.getDurationms())));
			this.vsize = newRecord.get_vsize().multiply(new BigInteger(Long.toString(this.getDurationms())));
	
		//Check if these are differential records
		} else if(rec1.getCpuUtilPct()!=-1d && rec2.getCpuUtilPct()!=-1d){
			//Check if these are chronologically consecutive differential records
			if(oldRecord.getTimestamp() != newRecord.getPreviousTimestamp())
				throw new Exception("Can not generate differential ProcessResourceRecord from non-consecutive differential records");
			//Copied values:
			this.setTimestamp(newRecord.getTimestamp());					//Set the end timestamp to the timestamp of the newer record
			this.setPreviousTimestamp(oldRecord.getPreviousTimestamp());	//Set the start timestamp to the start timestamp of the older record
			
			//Differential values:
			this.cguest_time = newRecord.get_cguest_time().add(oldRecord.get_cguest_time());
			this.cmajflt = newRecord.get_cmajflt().add(oldRecord.get_cmajflt());
			this.cminflt = newRecord.get_cminflt().add(oldRecord.get_cminflt());
			this.cstime = newRecord.get_cstime().add(oldRecord.get_cstime());
			this.cutime = newRecord.get_cutime().add(oldRecord.get_cutime());
			this.delayacct_blkio_ticks = newRecord.get_delayacct_blkio_ticks().add(oldRecord.get_delayacct_blkio_ticks());
			this.guest_time = newRecord.get_guest_time().add(oldRecord.get_guest_time());
			this.majflt = newRecord.get_majflt().add(oldRecord.get_majflt());
			this.minflt = newRecord.get_minflt().add(oldRecord.get_minflt());
			this.stime = newRecord.get_stime().add(oldRecord.get_stime());
			this.utime = newRecord.get_utime().add(oldRecord.get_utime());
			this.rchar = newRecord.get_rchar().add(oldRecord.get_rchar());
			this.wchar = newRecord.get_wchar().add(oldRecord.get_wchar());
			this.syscr = newRecord.get_syscr().add(oldRecord.get_syscr());
			this.syscw = newRecord.get_syscw().add(oldRecord.get_syscw());
			this.read_bytes = newRecord.get_read_bytes().add(oldRecord.get_read_bytes());
			this.write_bytes = newRecord.get_write_bytes().add(oldRecord.get_write_bytes());
			this.cancelled_write_bytes = newRecord.get_cancelled_write_bytes().add(oldRecord.get_cancelled_write_bytes());
			this.rss = newRecord.get_rss().add(oldRecord.get_rss());
			this.vsize = newRecord.get_vsize().add(oldRecord.get_vsize());

		//Otherwise, we are being asked to merge one raw record with one derived record and that is not valid.
		} else {
			throw new Exception("Can not generate differential ProcessResourceRecord from a raw record and a differential record");
		}
		this.cpuUtilPct = this.utime.add(this.stime).doubleValue() / 					//The number of jiffies used by the process over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
		this.iowaitUtilPct = this.delayacct_blkio_ticks.doubleValue() / 				//The number of jiffies the process waited for IO over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
		this.readIoCallRate = this.syscr.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.writeIoCallRate = this.syscw.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.readIoCharRate = this.rchar.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.writeIoCharRate = this.wchar.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.readIoByteRate = this.read_bytes.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.writeIoByteRate = this.write_bytes.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.cancelledWriteIoByteRate = this.cancelled_write_bytes.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
	}
	
	public ProcessResourceRecord(String statpath, String iopath, int clockTick) throws Exception {
		//This constructor takes "path" which should be the path to a /proc/[pid]/stat file
		//This constructor takes "clockTick" which should be set to jiffies per second from kernel build (obtained in ProcRecordProducer)
		super(System.currentTimeMillis());
		this.clockTick = clockTick;
		this.cpuUtilPct = -1d;
		this.iowaitUtilPct = -1d;
		this.readIoCallRate = -1d;
		this.writeIoCallRate = -1d;
		this.readIoCharRate = -1d;
		this.writeIoCharRate = -1d;
		this.readIoByteRate = -1d;
		this.writeIoByteRate = -1d;
		this.cancelledWriteIoByteRate = -1d;
		String[] parts = null;
		int statbs = 600; //600 bytes should be enough to hold contents of /proc/[pid]/stat or /proc/[pid]/task/[tid]/stat 
		int iobs = 250; //250 bytes should be enough to hold contents of /proc/[pid]/io or /proc/[pid]/task/[tid]/io
		FileChannel statfc = null, iofc = null;
		ByteBuffer b = null;
		int br=-1;
		
		try {
			//Open, read the file.
			try {
				statfc = FileChannel.open(Paths.get(statpath));
				b = ByteBuffer.allocate(statbs);
				br = statfc.read(b);
			} catch (Exception e) {
				throw new Exception("Failed to produce a ProcessResourceRecord due to an exception reading the stats file: " + statpath, e);
			}
			
			//Check whether we were able to read from the file, and whether we were able to read the entire contents
			if(br > 0 && br < statbs){
				//If the file could be read, parse the values
				String tempStr = new String(b.array());
				this.pid = Integer.parseInt(tempStr.split("\\s+", 2)[0]);
				this.comm = tempStr.split("\\(", 2)[1].split("\\)", 2)[0];
				parts = tempStr.split("\\)", 2)[1].trim().split("\\s+");
				//Expect 44 values in /proc/[pid]/stat based on Linux kernel version used for this dev.
				//Note that 42 is used in below check because 
				if(parts.length<42){ 
					throw new Exception("Failed to produce a ProcessResourceRecord due to unexpected format of stat file, found " + parts.length + " fields");
				}
				this.state = parts[0].charAt(0);
				this.ppid = Integer.parseInt(parts[1]);
				this.pgrp = Integer.parseInt(parts[2]);
				this.minflt = new BigInteger(parts[7]);
				this.cminflt = new BigInteger(parts[8]);
				this.majflt = new BigInteger(parts[9]);
				this.cmajflt = new BigInteger(parts[10]);
				this.utime = new BigInteger(parts[11]);
				this.stime = new BigInteger(parts[12]);
				this.cutime = new BigInteger(parts[13]);
				this.cstime = new BigInteger(parts[14]);
				this.num_threads = Integer.parseInt(parts[17]);
				this.starttime = Integer.parseInt(parts[19]);
				this.vsize = new BigInteger(parts[20]);
				this.rss = new BigInteger(parts[21]);
				this.rsslim = new BigInteger(parts[22]);
				this.delayacct_blkio_ticks = new BigInteger(parts[39]);
				this.guest_time = new BigInteger(parts[40]);
				this.cguest_time = new BigInteger(parts[41]);

				//Process io file
				try {
					iofc = FileChannel.open(Paths.get(iopath));
					b = ByteBuffer.allocate(iobs);
					br = iofc.read(b);
				} catch (Exception e) {
					throw new Exception("Failed to produce a ProcessResourceRecord due to an exception reading the io file: " + iopath, e);
				}
				String line = null;
				if(br > 0 && br < iobs){
					line = new String(b.array());
				} else {
					throw new Exception("Failed to produce a ProcessResourceRecord from io file due to read response length, br:" + br + " bs:" + iobs);
				}
				parts = line.trim().split("\\s+");
				if(parts.length<14){ //Expect 14 values in /proc/[pid]/task/[tid]/io based on Linux kernel version used for this dev.
					throw new Exception("Failed to produce a ProcessResourceRecord due to unexpected format of io file, found " + parts.length + " fields");
				}
				this.rchar = new BigInteger(parts[1]);
				this.wchar = new BigInteger(parts[3]);
				this.syscr = new BigInteger(parts[5]);
				this.syscw = new BigInteger(parts[7]);
				this.read_bytes = new BigInteger(parts[9]);
				this.write_bytes = new BigInteger(parts[11]);
				this.cancelled_write_bytes = new BigInteger(parts[13]);
			} else {
				throw new Exception("Failed to produce a ProcessResourceRecord due to read response length, br:" + br + " bs:" + statbs);
			}
		} catch (Exception e) {
			throw new Exception("Failed to produce a ProcessResourceRecord", e);
		} finally {
			try{
				statfc.close();
				iofc.close();
			} catch (Exception e){}
		}
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	//ret[0] - 0 indicates method completed successfully, 1 indicates method failed to run, this is different from failing to create a record.
	//ret[1] - records created and put to the output queue
	//ret[2] - failed record creation attempts
	//ret[3] - outputQueue put failures
	public static int[] produceRecord(RecordQueue outputQueue, String producerName, String statpath, String iopath, int clockTick){
		int[] ret = new int[] {0, 0, 0, 0};
		ProcessResourceRecord record = null;
		try{
			record = new ProcessResourceRecord(statpath, iopath, clockTick);
		} catch (Exception e) {
			ret[2]=1;
		}
		if(record != null && !outputQueue.put(producerName, record)){
			ret[3]=1;
			LOG.error("Failed to put ProcessResourceRecord into output queue " + outputQueue.getQueueName() + 
					" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.getQueueRecordCapacity() + 
					" producerName:" + producerName);
		} else {
			ret[1] = 1;
		}
		return ret;
	}
	
	/**
	 * OTHER METHODS
	 */
	public String toString(){
		return super.toString() + " process.resources: " + pid + " " + comm + " " + state + " " + ppid + " " + num_threads;
	}
	public String get_comm(){
		return comm;
	}
	public char get_state(){
		return state;
	}
	public int get_pid(){
		return pid;
	}
	public int get_ppid(){
		return ppid;
	}
	public int get_pgrp(){
		return pgrp;
	}
	public int get_num_threads(){
		return num_threads;
	}
	public int getClockTick(){
		return clockTick;
	}
	public long get_starttime(){
		return starttime;
	}
	public BigInteger get_cguest_time(){
		return cguest_time;
	}
	public BigInteger get_cmajflt(){
		return cmajflt;
	}
	public BigInteger get_cminflt(){
		return cminflt;
	}
	public BigInteger get_cstime(){
		return cstime;
	}
	public BigInteger get_cutime(){
		return cutime;
	}
	public BigInteger get_delayacct_blkio_ticks(){
		return delayacct_blkio_ticks;
	}
	public BigInteger get_guest_time(){
		return guest_time;
	}
	public BigInteger get_majflt(){
		return majflt;
	}
	public BigInteger get_minflt(){
		return minflt;
	}
	public BigInteger get_rss(){
		return rss;
	}
	public BigInteger get_rsslim(){
		return rsslim;
	}
	public BigInteger get_stime(){
		return stime;
	}
	public BigInteger get_utime(){
		return utime;
	}
	public BigInteger get_vsize(){
		return vsize;
	}
	public BigInteger get_rchar(){
		return rchar;
	}
	public BigInteger get_wchar(){
		return wchar;
	}
	public BigInteger get_syscr(){
		return syscr;
	}
	public BigInteger get_syscw(){
		return syscw;
	}
	public BigInteger get_read_bytes(){
		return read_bytes;
	}
	public BigInteger get_write_bytes(){
		return write_bytes;
	}
	public BigInteger get_cancelled_write_bytes(){
		return cancelled_write_bytes;
	}
	public double getCpuUtilPct(){
		return cpuUtilPct;
	}
	public double getIowaitUtilPct(){
		return iowaitUtilPct;
	}
	public double getReadIoCallRate(){
		return readIoCallRate;
	}
	public double getWriteIoCallRate(){
		return writeIoCallRate;
	}
	public double getReadIoCharRate(){
		return readIoCharRate;
	}
	public double getWriteIoCharRate(){
		return writeIoCharRate;
	}
	public double getReadIoByteRate(){
		return readIoByteRate;
	}
	public double getWriteIoByteRate(){
		return writeIoByteRate;
	}
	public double getCancelledWriteIoByteRate(){
		return cancelledWriteIoByteRate;
	}
	
	@Override
	public String getValueForQualifier(String qualifier) throws Exception {
		switch(qualifier){
		case "pid":
			return Integer.toString(pid);
		default:
			throw new Exception("Qualifier " + qualifier + " is not valid for this record type");
		}
	}

}
