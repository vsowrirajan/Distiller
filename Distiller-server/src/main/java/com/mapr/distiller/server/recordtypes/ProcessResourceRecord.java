package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import com.mapr.distiller.server.queues.RecordQueue;

public class ProcessResourceRecord extends Record {
	/**
	 * DERIVED VALUES
	 */
	private double cpuUtilPct;;
	
	/**
	 * RAW VALUES
	 */
	private String comm;
	private char state;
	private int pid, ppid, pgrp, num_threads, clockTick;
	private long starttime;
	private BigInteger cguest_time, cmajflt, cminflt, cstime, cutime, delayacct_blkio_ticks, guest_time, majflt, minflt, rss, rsslim, stime, utime, vsize;

	/**
	 * CONSTRUCTORS
	 */
	public ProcessResourceRecord(ProcessResourceRecord rec1, ProcessResourceRecord rec2) throws Exception{
		ProcessResourceRecord oldRecord, newRecord;
		
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
		this.pgrp = newRecord.get_pgrp();
		this.num_threads = newRecord.get_num_threads();
		this.clockTick = newRecord.getClockTick();
		this.starttime = newRecord.get_starttime();
		this.rss = newRecord.get_rss();
		this.rsslim = newRecord.get_rsslim();
		this.vsize = newRecord.get_vsize();
		
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

		//Derived values:
		this.cpuUtilPct = this.utime.add(this.stime).doubleValue() / 					//The number of jiffies used by the process over the duration
				(((double)(this.clockTick * this.getDurationms())) / 1000d);			//The number of jiffies that went by over the duration
	}
	
	public ProcessResourceRecord(String path, int clockTick) throws Exception {
		//This constructor takes "path" which should be the path to a /proc/[pid]/stat file
		//This constructor takes "clockTick" which should be set to jiffies per second from kernel build (obtained in ProcRecordProducer)
		super(System.currentTimeMillis());
		this.clockTick = clockTick;
		this.cpuUtilPct = -1d;
		String[] parts = null;
		int bs = 600; //600 bytes should be enough to hold contents of /proc/[pid]/stat or /proc/[pid]/task/[tid]/stat 
		FileChannel fc = null;
		ByteBuffer b = null;
		int br=-1;
		
		try {
			//Open, read the file.
			fc = FileChannel.open(Paths.get(path));
			b = ByteBuffer.allocate(bs);
			br = fc.read(b);
			
			//Check whether we were able to read from the file, and whether we were able to read the entire contents
			if(br > 0 && br < bs){
				//If the file could be read, parse the values
				String tempStr = new String(b.array());
				this.pid = Integer.parseInt(tempStr.split("\\s+", 2)[0]);
				this.comm = "(" + tempStr.split("\\(", 2)[1].split("\\)", 2)[0] + ")";
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
			} else {
				throw new Exception("Failed to produce a ProcessResourceRecord due to read response length, br:" + br + " bs:" + bs);
			}
		} catch (Exception e) {
			throw new Exception("Failed to produce a ProcessResourceRecord for path " + path, e);
		} finally {
			fc.close();
		}
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	public static boolean produceRecord(RecordQueue process_resources, String producerName, String path, int clockTick){
		try{
			process_resources.put(producerName, new ProcessResourceRecord(path, clockTick));
		} catch (Exception e) {
			System.err.println("Failed to generate a ProcessResourceRecord");
			e.printStackTrace();
			return false;
		}
		return  true;
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
	public double getCpuUtilPct(){
		return cpuUtilPct;
	}
}