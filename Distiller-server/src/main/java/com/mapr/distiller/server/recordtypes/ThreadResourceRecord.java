package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class ThreadResourceRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(ThreadResourceRecord.class);
	private static final long serialVersionUID = Constants.SVUID_THREAD_RESOURCE_RECORD;
	/**
	 * DERIVED VALUES
	 */
	private double cpuUtilPct, iowaitUtilPct, readCallRate, writeCallRate, readCharRate, writeCharRate, readByteRate, writeByteRate, cancelledWriteByteRate;
	
	/**
	 * RAW VALUES
	 */
	private String comm;
	private char state;
	private int pid, ppid, clockTick;
	private long starttime;
	private BigInteger delayacct_blkio_ticks, guest_time, majflt, minflt, stime, utime;
	private BigInteger rchar, wchar, syscr, syscw, read_bytes, write_bytes, cancelled_write_bytes;

	@Override
	public String getRecordType(){
		return Constants.THREAD_RESOURCE_RECORD;
	}
	

	/**
	 * CONSTRUCTORS
	 */
	public ThreadResourceRecord(ThreadResourceRecord rec1, ThreadResourceRecord rec2) throws Exception{
		ThreadResourceRecord oldRecord, newRecord;
		
		//Check the input records to ensure they can be diff'd.
		if(rec1.get_starttime() != rec2.get_starttime() || rec1.get_pid() != rec2.get_pid())
			throw new Exception("Differential ThreadResourceRecord can only be generated from input records from the same process");
		if(rec1.getCpuUtilPct()!=-1d || rec2.getCpuUtilPct()!=-1d || rec1.getPreviousTimestamp()!=-1l || rec2.getPreviousTimestamp()!=-1l)
			throw new Exception("Differential ThreadResourceRecord can only be generated from raw ThreadResourceRecords");
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential ThreadResourceRecord from input records with matching timestamp values");
		
		//Organize the input records.
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		
		//Copied values:
		this.setTimestamp(newRecord.getTimestamp());
		this.setPreviousTimestamp(oldRecord.getTimestamp());
		this.comm = newRecord.get_comm();
		this.state = newRecord.get_state();
		this.pid = newRecord.get_pid();
		this.ppid = newRecord.get_ppid();
		this.clockTick = newRecord.getClockTick();
		this.starttime = newRecord.get_starttime();
		
		//Differential values:
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
		this.cpuUtilPct = this.utime.add(this.stime).doubleValue() / 					//The number of jiffies used by the process over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
		this.iowaitUtilPct = this.delayacct_blkio_ticks.doubleValue() / 				//The number of jiffies the process waited for IO over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
		this.readCallRate = this.syscr.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.writeCallRate = this.syscw.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.readCharRate = this.rchar.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.writeCharRate = this.wchar.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.readByteRate = this.read_bytes.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.writeByteRate = this.write_bytes.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.cancelledWriteByteRate = this.cancelled_write_bytes.doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
	}
	public ThreadResourceRecord(String statPath, String ioPath, int ppid, int clockTick) throws Exception{
		super(System.currentTimeMillis());
		this.cpuUtilPct=-1d;
		this.iowaitUtilPct = -1d;
		this.readCallRate = -1d;
		this.writeCallRate = -1d;
		this.readCharRate = -1d;
		this.writeCharRate = -1d;
		this.readByteRate = -1d;
		this.writeByteRate = -1d;
		this.cancelledWriteByteRate = -1d;
		this.ppid = ppid;
		this.clockTick = clockTick;
		int statbs = 600; //600 bytes should be enough to hold contents of /proc/[pid]/stat or /proc/[pid]/task/[tid]/stat 
		int iobs = 250; //250 bytes should be enough to hold contents of /proc/[pid]/io or /proc/[pid]/task/[tid]/io
		FileChannel statfc = null, iofc = null;
		ByteBuffer b = null;
		int br=-1;
		String line;
		try {
			//Process stat file
			try {
				statfc = FileChannel.open(Paths.get(statPath));
				b = ByteBuffer.allocate(statbs);
				br = statfc.read(b);
			} catch (Exception e) {
				throw new Exception("Failed to produce a ThreadResourceRecord due to an exception reading the stats file: " + statPath, e);
			}
			if(br > 0 && br < statbs){
				line = new String(b.array());
			} else {
				throw new Exception("Failed to produce a ThreadResourceRecord from stat file due to read response length, br:" + br + " bs:" + statbs);
			}
			String[] parts = line.split("\\)", 2)[1].trim().split("\\s+");
			if(parts.length<42){ //Expect 44 values in /proc/[pid]/task/[tid]/stat based on Linux kernel version used for this dev.
				throw new Exception("Failed to produce a ThreadResourceRecord due to unexpected format of stat file, found " + parts.length + " fields");
			}
			this.pid = Integer.parseInt(line.split("\\s+", 2)[0]);
			this.comm = line.split("\\(", 2)[1].split("\\)", 2)[0];
			this.state = parts[0].charAt(0);
			this.minflt = new BigInteger(parts[7]);
			this.majflt = new BigInteger(parts[9]);
			this.utime = new BigInteger(parts[11]);
			this.stime = new BigInteger(parts[12]);
			this.starttime = Long.parseLong(parts[19]);
			this.delayacct_blkio_ticks = new BigInteger(parts[39]);
			this.guest_time = new BigInteger(parts[40]);
			
			//Process io file
			try {
				iofc = FileChannel.open(Paths.get(ioPath));
				b = ByteBuffer.allocate(iobs);
				br = iofc.read(b);
			} catch (Exception e) {
				throw new Exception("Failed to produce a ThreadResourceRecord due to an exception reading the io file: " + ioPath, e);
			}
			if(br > 0 && br < iobs){
				line = new String(b.array());
			} else {
				throw new Exception("Failed to produce a ThreadResourceRecord from io file due to read response length, br:" + br + " bs:" + iobs);
			}
			parts = line.trim().split("\\s+");
			if(parts.length<14){ //Expect 14 values in /proc/[pid]/task/[tid]/io based on Linux kernel version used for this dev.
				throw new Exception("Failed to produce a ThreadResourceRecord due to unexpected format of io file, found " + parts.length + " fields");
			}
			this.rchar = new BigInteger(parts[1]);
			this.wchar = new BigInteger(parts[3]);
			this.syscr = new BigInteger(parts[5]);
			this.syscw = new BigInteger(parts[7]);
			this.read_bytes = new BigInteger(parts[9]);
			this.write_bytes = new BigInteger(parts[11]);
			this.cancelled_write_bytes = new BigInteger(parts[13]);
		} catch (Exception e) {
			throw new Exception("Failed to generate ThreadResourceRecord", e);
		} finally {
			try{
				iofc.close();
			} catch (Exception e) {}
			try{
				statfc.close();
			} catch (Exception e) {}
		}
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	//ret[0] - 0 indicates method completed successfully, 1 indicates method failed to run, this is different from failing to create a record.
	//ret[1] - records created and put to the output queue
	//ret[2] - failed record creation attempts
	//ret[3] - outputQueue put failures
	public static int[] produceRecord(RecordQueue outputQueue, String producerName, String statpath, String iopath, int ppid, int clockTick){
		int[] ret = new int[] {0, 0, 0, 0};
		ThreadResourceRecord record = null;
		try{
			record = new ThreadResourceRecord(statpath, iopath, ppid, clockTick);
		} catch (Exception e) {
			ret[2] = 1;
		}
		if(record!= null && !outputQueue.put(producerName, record)){
			ret[3]=1;
			LOG.error("Failed to put ThreadResourceRecord into output queue " + outputQueue.getQueueName() + 
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
		return super.toString() + " thread.resources: " + pid + " " + comm + " " + state + " " + ppid;
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
	public int getClockTick(){
		return clockTick;
	}
	public long get_starttime(){
		return starttime;
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
	public BigInteger get_stime(){
		return stime;
	}
	public BigInteger get_utime(){
		return utime;
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
	public double getReadCallRate(){
		return readCallRate;
	}
	public double getWriteCallRate(){
		return writeCallRate;
	}
	public double getReadCharRate(){
		return readCharRate;
	}
	public double getWriteCharRate(){
		return writeCharRate;
	}
	public double getReadByteRate(){
		return readByteRate;
	}
	public double getWriteByteRate(){
		return writeByteRate;
	}
	public double getCancelledWriteByteRate(){
		return cancelledWriteByteRate;
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
