package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class SlimThreadResourceRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(SlimThreadResourceRecord.class);
	private static final long serialVersionUID = Constants.SVUID_SLIM_THREAD_RESOURCE_RECORD;
	/**
	 * DERIVED VALUES
	 */
	private double cpuUtilPct, iowaitUtilPct, ioCallRate, readIoByteRate, writeIoByteRate;
	
	/**
	 * RAW VALUES
	 */
	private String commandName;
	private int pid, ppid, clockTick;
	private long startTime;
	private BigInteger iowaitTicks, cpuUsageTicks;
	private BigInteger ioCalls, ioBytesRead, ioBytesWritten;	

	@Override
	public String getRecordType(){
		return Constants.SLIM_THREAD_RESOURCE_RECORD;
	}
	

	/**
	 * CONSTRUCTORS
	 */
	public SlimThreadResourceRecord(ThreadResourceRecord tRec) throws Exception{
		try{
			this.setPreviousTimestamp(tRec.getPreviousTimestamp());
			this.setTimestamp(tRec.getTimestamp());
		} catch (Exception e){
			throw new Exception("Could not construct SlimThreadResourceRecord from ThreadResourceRecord", e);
		}
		this.commandName = tRec.get_comm();
		this.pid = tRec.get_pid();
		this.ppid = tRec.get_ppid();
		this.clockTick = tRec.getClockTick();
		this.startTime = tRec.get_starttime();
		this.iowaitTicks = tRec.get_delayacct_blkio_ticks();
		this.cpuUsageTicks = tRec.get_utime().add(tRec.get_stime());
		this.ioCalls = tRec.get_syscr().add(tRec.get_syscw());
		this.ioBytesRead = tRec.get_read_bytes();
		this.ioBytesWritten = tRec.get_write_bytes();
	}
	public SlimThreadResourceRecord(SlimThreadResourceRecord rec1, SlimThreadResourceRecord rec2) throws Exception{
		SlimThreadResourceRecord oldRecord, newRecord;
		
		//Check the input records to ensure they can be diff'd.
		if(rec1.getStartTime() != rec2.getStartTime() || rec1.getPid() != rec2.getPid())
			throw new Exception("Differential SlimThreadResourceRecord can only be generated from input records from the same process");
		if(rec1.getCpuUtilPct()!=-1d || rec2.getCpuUtilPct()!=-1d || rec1.getPreviousTimestamp()!=-1l || rec2.getPreviousTimestamp()!=-1l)
			throw new Exception("Differential SlimThreadResourceRecord can only be generated from raw SlimThreadResourceRecords");
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential SlimThreadResourceRecord from input records with matching timestamp values");
		
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
		this.commandName = newRecord.getCommandName();
		this.pid = newRecord.getPid();
		this.ppid = newRecord.getPpid();
		this.clockTick = newRecord.getClockTick();
		this.startTime = newRecord.getStartTime();
		
		//Differential values:
		this.iowaitTicks = newRecord.getIowaitTicks().subtract(oldRecord.getIowaitTicks());
		this.cpuUsageTicks = newRecord.getCpuUsageTicks().subtract(oldRecord.getCpuUsageTicks());
		this.ioCalls = newRecord.getIoCalls().subtract(oldRecord.getIoCalls());
		this.ioBytesRead = newRecord.getIoBytesRead().subtract(oldRecord.getIoBytesRead());
		this.ioBytesWritten = newRecord.getIoBytesWritten().subtract(oldRecord.getIoBytesWritten());

		//Derived values:
		this.cpuUtilPct = this.getCpuUsageTicks().doubleValue() / 					//The number of jiffies used by the process over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
		this.iowaitUtilPct = this.getIowaitTicks().doubleValue() / 				//The number of jiffies the process waited for IO over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
		this.ioCallRate = this.getIoCalls().doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.readIoByteRate = this.getIoBytesRead().doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
		this.writeIoByteRate = this.getIoBytesWritten().doubleValue() / 
				(((double)(this.clockTick * this.getDurationms())) / 1000d);
	}
	public SlimThreadResourceRecord(String statPath, String ioPath, int ppid, int clockTick) throws Exception{
		super(System.currentTimeMillis());
		this.clockTick = clockTick;
		this.ppid = ppid;
		this.cpuUtilPct = -1d;
		this.iowaitUtilPct = -1d;
		this.ioCallRate = -1d;
		this.readIoByteRate = -1d;
		this.writeIoByteRate = -1d;
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
				throw new Exception("Failed to produce a SlimThreadResourceRecord due to an exception reading the stats file: " + statPath, e);
			}
			if(br > 0 && br < statbs){
				line = new String(b.array());
			} else {
				throw new Exception("Failed to produce a SlimThreadResourceRecord from stat file due to read response length, br:" + br + " bs:" + statbs);
			}
			String[] parts = line.split("\\)", 2)[1].trim().split("\\s+");
			if(parts.length<42){ //Expect 44 values in /proc/[pid]/task/[tid]/stat based on Linux kernel version used for this dev.
				throw new Exception("Failed to produce a SlimThreadResourceRecord due to unexpected format of stat file, found " + parts.length + " fields");
			}
			this.pid = Integer.parseInt(line.split("\\s+", 2)[0]);
			this.commandName = line.split("\\(", 2)[1].split("\\)", 2)[0];
			this.cpuUsageTicks = new BigInteger(parts[11]).add(new BigInteger(parts[12]));
			this.startTime = Long.parseLong(parts[19]);
			this.iowaitTicks = new BigInteger(parts[39]);
			
			//Process io file
			try {
				iofc = FileChannel.open(Paths.get(ioPath));
				b = ByteBuffer.allocate(iobs);
				br = iofc.read(b);
			} catch (Exception e) {
				throw new Exception("Failed to produce a SlimThreadResourceRecord due to an exception reading the io file: " + ioPath, e);
			}
			if(br > 0 && br < iobs){
				line = new String(b.array());
			} else {
				throw new Exception("Failed to produce a SlimThreadResourceRecord from io file due to read response length, br:" + br + " bs:" + iobs);
			}
			parts = line.trim().split("\\s+");
			if(parts.length<14){ //Expect 14 values in /proc/[pid]/task/[tid]/io based on Linux kernel version used for this dev.
				throw new Exception("Failed to produce a SlimThreadResourceRecord due to unexpected format of io file, found " + parts.length + " fields");
			}
			this.ioCalls = new BigInteger(parts[5]).add(new BigInteger(parts[7]));
			this.ioBytesRead = new BigInteger(parts[9]);
			this.ioBytesWritten = new BigInteger(parts[11]);
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
		SlimThreadResourceRecord record = null;
		try{
			record = new SlimThreadResourceRecord(statpath, iopath, ppid, clockTick);
		} catch (Exception e) {
			ret[2] = 1;
		}
		if(record!= null && !outputQueue.put(producerName, record)){
			ret[3]=1;
			LOG.error("Failed to put SlimThreadResourceRecord into output queue " + outputQueue.getQueueName() + 
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
		if(this.getPreviousTimestamp()==-1l)
			return super.toString() + " STRR c:" + commandName + " p:" + pid + " cpu:" + cpuUsageTicks + " ior:" + ioBytesRead + " iow:" + ioBytesWritten;
		return super.toString() + " STRR c:" + commandName + " p:" + pid + " cpu:" + cpuUtilPct + " wait:" + iowaitUtilPct + " ioc:" + ioCallRate + " ior:" + readIoByteRate + " iow:" + writeIoByteRate;
	}
	
	public double getCpuUtilPct(){
		return cpuUtilPct;
	}
	public double getIowaitUtilPct(){
		return iowaitUtilPct;
	}
	public double getIoCallRate(){
		return ioCallRate;
	}
	public double getReadIoByteRate(){
		return readIoByteRate;
	}
	public double getWriteIoByteRate(){
		return writeIoByteRate;
	}
	public String getCommandName(){
		return commandName;
	}
	public int getPid(){
		return pid;
	}
	public int getPpid(){
		return ppid;
	}
	public int getClockTick(){
		return clockTick;
	}
	public long getStartTime(){
		return startTime;
	}
	public BigInteger getIowaitTicks(){
		return iowaitTicks;
	}
	public BigInteger getCpuUsageTicks(){
		return cpuUsageTicks;
	}
	public BigInteger getIoCalls(){
		return ioCalls;
	}
	public BigInteger getIoBytesRead(){
		return ioBytesRead;
	}
	public BigInteger getIoBytesWritten(){
		return ioBytesWritten;
	}
	public String get_upid(){
		return pid + "_" + startTime;
	}
	
	@Override
	public String getValueForQualifier(String qualifier) throws Exception {
		switch(qualifier){
		case "pid":
			return Integer.toString(pid);
		case "upid":
			return get_upid();
		default:
			throw new Exception("Qualifier " + qualifier + " is not valid for this record type");
		}
	}
}
