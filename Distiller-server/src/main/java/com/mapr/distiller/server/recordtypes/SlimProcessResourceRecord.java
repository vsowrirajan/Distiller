package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import com.mapr.distiller.server.queues.RecordQueue;

public class SlimProcessResourceRecord extends Record {
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
	private BigInteger iowaitTicks, cpuUsageTicks, rss;
	private BigInteger ioCalls, ioBytesRead, ioBytesWritten;	

	/**
	 * CONSTRUCTORS
	 */
	public SlimProcessResourceRecord(ProcessResourceRecord pRec) throws Exception{
		try{
			this.setPreviousTimestamp(pRec.getPreviousTimestamp());
			this.setTimestamp(pRec.getTimestamp());
		} catch (Exception e){
			throw new Exception("Could not construct SlimProcessResourceRecord from ProcessResourceRecord", e);
		}
		this.commandName = pRec.get_comm();
		this.pid = pRec.get_pid();
		this.ppid = pRec.get_ppid();
		this.clockTick = pRec.getClockTick();
		this.startTime = pRec.get_starttime();
		this.iowaitTicks = pRec.get_delayacct_blkio_ticks();
		this.cpuUsageTicks = pRec.get_utime().add(pRec.get_stime());
		this.ioCalls = pRec.get_syscr().add(pRec.get_syscw());
		this.ioBytesRead = pRec.get_read_bytes();
		this.ioBytesWritten = pRec.get_write_bytes();
		this.rss = pRec.get_rss();
	}
	public SlimProcessResourceRecord(SlimProcessResourceRecord rec1, SlimProcessResourceRecord rec2) throws Exception{
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential SlimProcessResourceRecord from input records with matching timestamp values");
		if(rec1.getStartTime() != rec2.getStartTime() || rec1.getPid() != rec2.getPid())
			throw new Exception("Differential SlimProcessResourceRecord can only be generated from input records from the same process");
		
		//Organize the input records.
		SlimProcessResourceRecord oldRecord, newRecord;
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
		
		//Check if these are raw records
		if(rec1.getCpuUtilPct()==-1d && rec2.getCpuUtilPct()==-1d){
			this.setTimestamp(newRecord.getTimestamp());
			this.setPreviousTimestamp(oldRecord.getTimestamp());
				
			this.iowaitTicks = newRecord.getIowaitTicks().subtract(oldRecord.getIowaitTicks());
			this.cpuUsageTicks = newRecord.getCpuUsageTicks().subtract(oldRecord.getCpuUsageTicks());
			this.ioCalls = newRecord.getIoCalls().subtract(oldRecord.getIoCalls());
			this.ioBytesRead = newRecord.getIoBytesRead().subtract(oldRecord.getIoBytesRead());
			this.ioBytesWritten = newRecord.getIoBytesWritten().subtract(oldRecord.getIoBytesWritten());
			
			this.rss = newRecord.getRss().multiply(new BigInteger(Long.toString(this.getDurationms())));
			
		//Check if these are differential records
		} else if(rec1.getCpuUtilPct()!=-1d && rec2.getCpuUtilPct()!=-1d){
			//Check if these are chronologically consecutive differential records
			if(oldRecord.getTimestamp() != newRecord.getPreviousTimestamp())
				throw new Exception("Can not generate differential SlimProcessResourceRecord from non-consecutive differential records");
			
			this.setTimestamp(newRecord.getTimestamp());					//Set the end timestamp to the timestamp of the newer record
			this.setPreviousTimestamp(oldRecord.getPreviousTimestamp());	//Set the start timestamp to the start timestamp of the older record
			
			this.iowaitTicks = newRecord.getIowaitTicks().add(oldRecord.getIowaitTicks());
			this.cpuUsageTicks = newRecord.getCpuUsageTicks().add(oldRecord.getCpuUsageTicks());
			this.ioCalls = newRecord.getIoCalls().add(oldRecord.getIoCalls());
			this.ioBytesRead = newRecord.getIoBytesRead().add(oldRecord.getIoBytesRead());
			this.ioBytesWritten = newRecord.getIoBytesWritten().add(oldRecord.getIoBytesWritten());
			this.rss = newRecord.getRss().add(oldRecord.getRss());
		
		//Otherwise, we are being asked to merge one raw record with one derived record and that is not valid.
		} else {
			throw new Exception("Can not generate differential ProcessResourceRecord from a raw record and a differential record");
		}
		
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
	
	public SlimProcessResourceRecord(String statpath, String iopath, int clockTick) throws Exception {
		//This constructor takes "path" which should be the path to a /proc/[pid]/stat file
		//This constructor takes "clockTick" which should be set to jiffies per second from kernel build (obtained in ProcRecordProducer)
		super(System.currentTimeMillis());
		this.clockTick = clockTick;
		this.cpuUtilPct = -1d;
		this.iowaitUtilPct = -1d;
		this.ioCallRate = -1d;
		this.readIoByteRate = -1d;
		this.writeIoByteRate = -1d;
		
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
				throw new Exception("Failed to produce a SlimProcessResourceRecord due to an exception reading the stats file: " + statpath, e);
			}
			
			//Check whether we were able to read from the file, and whether we were able to read the entire contents
			if(br > 0 && br < statbs){
				//If the file could be read, parse the values
				String tempStr = new String(b.array());
				this.pid = Integer.parseInt(tempStr.split("\\s+", 2)[0]);
				this.commandName = "(" + tempStr.split("\\(", 2)[1].split("\\)", 2)[0] + ")";
				parts = tempStr.split("\\)", 2)[1].trim().split("\\s+");
				//Expect 44 values in /proc/[pid]/stat based on Linux kernel version used for this dev.
				//Note that 42 is used in below check because 
				if(parts.length<42){ 
					throw new Exception("Failed to produce a SlimProcessResourceRecord due to unexpected format of stat file, found " + parts.length + " fields");
				}
				this.ppid = Integer.parseInt(parts[1]);
				this.cpuUsageTicks = new BigInteger(parts[11]).add(new BigInteger(parts[12]));
				this.startTime = Integer.parseInt(parts[19]);
				this.rss = new BigInteger(parts[21]);
				this.iowaitTicks = new BigInteger(parts[39]);
				

				//Process io file
				try {
					iofc = FileChannel.open(Paths.get(iopath));
					b = ByteBuffer.allocate(iobs);
					br = iofc.read(b);
				} catch (Exception e) {
					throw new Exception("Failed to produce a SlimProcessResourceRecord due to an exception reading the io file: " + iopath, e);
				}
				String line = null;
				if(br > 0 && br < iobs){
					line = new String(b.array());
				} else {
					throw new Exception("Failed to produce a SlimProcessResourceRecord from io file due to read response length, br:" + br + " bs:" + iobs);
				}
				parts = line.trim().split("\\s+");
				if(parts.length<14){ //Expect 14 values in /proc/[pid]/task/[tid]/io based on Linux kernel version used for this dev.
					throw new Exception("Failed to produce a ThreadResourceRecord due to unexpected format of io file, found " + parts.length + " fields");
				}
				this.ioCalls = new BigInteger(parts[5]).add(new BigInteger(parts[7]));
				this.ioBytesRead = new BigInteger(parts[9]);
				this.ioBytesWritten = new BigInteger(parts[11]);
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
		SlimProcessResourceRecord record = null;
		try{
			record = new SlimProcessResourceRecord(statpath, iopath, clockTick);
		} catch (Exception e) {
			ret[2]=1;
		}
		if(record != null && !outputQueue.put(producerName, record)){
			ret[3]=1;
			System.err.println("Failed to put SlimProcessResourceRecord into output queue " + outputQueue.getQueueName() + 
					" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.maxQueueSize() + 
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
		if(this.getPreviousTimestamp()==-1l)
			return super.toString() + " SPRR c:" + commandName + " p:" + pid + " cpu:" + cpuUsageTicks + " rss:" + rss + " ior:" + ioBytesRead + " iow:" + ioBytesWritten;
		return super.toString() + " SPRR c:" + commandName + " p:" + pid + " cpu:" + cpuUtilPct + " wait:" + iowaitUtilPct + " ioc:" + ioCallRate + " ior:" + readIoByteRate + " iow:" + writeIoByteRate + " rss:" + rss;
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
	public double getwriteIoByteRate(){
		return writeIoByteRate;
	}
	public String getCommandName(){
		return commandName;
	}
	private int getPid(){
		return pid;
	}
	private int getPpid(){
		return ppid;
	}
	private int getClockTick(){
		return clockTick;
	}
	private long getStartTime(){
		return startTime;
	}
	private BigInteger getIowaitTicks(){
		return iowaitTicks;
	}
	private BigInteger getCpuUsageTicks(){
		return cpuUsageTicks;
	}
	private BigInteger getRss(){
		return rss;
	}
	private BigInteger getIoCalls(){
		return ioCalls;
	}
	private BigInteger getIoBytesRead(){
		return ioBytesRead;
	}
	private BigInteger getIoBytesWritten(){
		return ioBytesWritten;
	}
}