package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import com.mapr.distiller.server.queues.RecordQueue;

public class ThreadResourceRecord extends Record {
	/**
	 * DERIVED VALUES
	 */
	private Double cpuUtilPct;
	
	/**
	 * RAW VALUES
	 */
	private String comm;
	private char state;
	private int pid, ppid, clockTick;
	private long starttime;
	private BigInteger delayacct_blkio_ticks, guest_time, majflt, minflt, stime, utime;

	/**
	 * CONSTRUCTORS
	 */
	public ThreadResourceRecord(ThreadResourceRecord rec1, ThreadResourceRecord rec2) throws Exception{
		ThreadResourceRecord oldRecord, newRecord;
		
		//Check the input records to ensure they can be diff'd.
		if(rec1.get_starttime() != rec2.get_starttime() || rec1.get_pid() != rec2.get_pid())
			throw new Exception("Differential ProcessResourceRecord can only be generated from input records from the same process");
		if(rec1.getCpuUtilPct()!=-1d || rec2.getCpuUtilPct()!=-1d || rec1.getPreviousTimestamp()!=-1l || rec2.getPreviousTimestamp()!=-1l)
			throw new Exception("Differential ProcessResourceRecord can only be generated from raw ProcessResourceRecords");
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential ProcessResourceRecord from input records with matching timestamp values");
		
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

		//Derived values:
		this.cpuUtilPct = this.utime.add(this.stime).doubleValue() / 					//The number of jiffies used by the process over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
	}
	public ThreadResourceRecord(String path, int ppid, int clockTick) throws Exception{
		super(System.currentTimeMillis());
		this.cpuUtilPct=-1d;
		this.ppid = ppid;
		this.clockTick = clockTick;
		int bs = 600; //600 bytes should be enough to hold contents of /proc/[pid]/stat or /proc/[pid]/task/[tid]/stat 
		FileChannel fc = null;
		ByteBuffer b = null;
		int br=-1;
		String line;
		try {
			boolean failedToReadFile=false;
			try {
				fc = FileChannel.open(Paths.get(path));
				b = ByteBuffer.allocate(bs);
				br = fc.read(b);
			} catch (Exception e) {
				failedToReadFile = true;
			}
			if(!failedToReadFile){
				if(br > 0 && br < bs){
					line = new String(b.array());
				} else {
					throw new Exception("Failed to produce a ThreadResourceRecord due to read response length, br:" + br + " bs:" + bs);
				}
				String[] parts = line.split("\\)", 2)[1].trim().split("\\s+");
				if(parts.length<42){ //Expect 44 values in /proc/[pid]/task/[tid]/stat based on Linux kernel version used for this dev.
					throw new Exception("Failed to produce a ThreadResourceRecord due to unexpected format of stat file, found " + parts.length + " fields");
				}
				this.pid = Integer.parseInt(line.split("\\s+", 2)[0]);
				this.comm = "(" + line.split("\\(", 2)[1].split("\\)", 2)[0] + ")";
				this.state = parts[0].charAt(0);
				this.minflt = new BigInteger(parts[7]);
				this.majflt = new BigInteger(parts[9]);
				this.utime = new BigInteger(parts[11]);
				this.stime = new BigInteger(parts[12]);
				this.starttime = Integer.parseInt(parts[19]);
				this.delayacct_blkio_ticks = new BigInteger(parts[39]);
				this.guest_time = new BigInteger(parts[40]);
			}
		} catch (Exception e) {
			throw new Exception("Failed to generate ThreadResourceRecord", e);
		} finally {
			try{
				fc.close();
			} catch (Exception e) {}
		}
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	public static boolean produceRecord(RecordQueue outputQueue, String producerName, String path, int ppid, int clockTick){
		try{
			ThreadResourceRecord record = new ThreadResourceRecord(path, ppid, clockTick);
			if(!outputQueue.put(producerName, record)){
				throw new Exception("Failed to put ThreadResourceRecord into output queue " + outputQueue.getQueueName() + 
						" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.maxQueueSize() + 
						" producerName:" + producerName);
			}
		} catch (Exception e) {
			System.err.println("Failed to generate a ThreadResourceRecord");
			e.printStackTrace();
			return false;
		}
		return true;
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
	public double getCpuUtilPct(){
		return cpuUtilPct;
	}
}