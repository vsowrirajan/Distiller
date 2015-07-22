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
	//private Double cpuUtilPct = -1d;
	
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
	public ThreadResourceRecord(String path, int ppid, int clockTick) throws Exception{
		super(System.currentTimeMillis());
		this.ppid = ppid;
		this.clockTick = clockTick;
		int bs = 600; //600 bytes should be enough to hold contents of /proc/[pid]/stat or /proc/[pid]/task/[tid]/stat 
		FileChannel fc = null;
		String line;
		try {
			fc = FileChannel.open(Paths.get(path));
			ByteBuffer b = ByteBuffer.allocate(bs);
			int br = fc.read(b);
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
	public static boolean produceRecord(RecordQueue thread_resources, String path, int ppid, int clockTick){
		try{
			thread_resources.put(new ThreadResourceRecord(path, ppid, clockTick));
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
}
/**
	public static ThreadResourceRecord diff(ThreadResourceRecord oldRecord, ThreadResourceRecord newRecord){
		//This function should be called to diff ThreadResourceRecords when the clockTick field of those records is populated.
		//If this is called against records without clockTick populated then no cpu utilization will be calculated (because we can't).
		
		//Only diff the records if they are from the same thread, as identified by the TID and starttime.
		//This works under the assumption that the system is not able to cycle through all process IDs and reuse them within a single
		//tick of the starttime clock
		if(	oldRecord.pid != newRecord.pid 				||
			oldRecord.starttime != newRecord.starttime 	)
			return null;
			
		ThreadResourceRecord diffRecord = new ThreadResourceRecord(newRecord.timestamp, oldRecord.timestamp);
		diffRecord.comm = newRecord.comm;
		diffRecord.state = newRecord.state;
		diffRecord.pid = newRecord.pid;
		diffRecord.ppid = newRecord.ppid;
		diffRecord.starttime = newRecord.starttime;
		diffRecord.minflt = newRecord.minflt.subtract(oldRecord.minflt);
		diffRecord.majflt = newRecord.majflt.subtract(oldRecord.majflt);
		diffRecord.utime = newRecord.utime.subtract(oldRecord.utime);
		diffRecord.stime = newRecord.stime.subtract(oldRecord.stime);
		diffRecord.delayacct_blkio_ticks = newRecord.delayacct_blkio_ticks.subtract(oldRecord.delayacct_blkio_ticks);
		diffRecord.guest_time = newRecord.guest_time.subtract(oldRecord.guest_time);
		diffRecord.clockTick = newRecord.clockTick;
		
		//Derived values:
		if(diffRecord.clockTick > 0){
			diffRecord.cpuUtilPct = diffRecord.utime.add(diffRecord.stime).doubleValue() / 					//The amount of jiffies used by the process over the duration
									(((double)(diffRecord.clockTick * diffRecord.durationms)) / 1000d);	//Divided by the amonut of jiffies that elapsed during the duration
		} else {
			diffRecord.cpuUtilPct = -1d;
		}
		return diffRecord;
	}
	public static ThreadResourceRecord diff(ThreadResourceRecord oldRecord, ThreadResourceRecord newRecord, int clockTick){
		//This function should be called to diff ThreadResourceRecords when the clockTick field of those records is populated.
		//If this is called against records without clockTick populated then no cpu utilization will be calculated (because we can't).
		
		//Only diff the records if they are from the same thread, as identified by the TID and starttime.
		//This works under the assumption that the system is not able to cycle through all process IDs and reuse them within a single
		//tick of the starttime clock
		if(	oldRecord.pid != newRecord.pid 				||
			oldRecord.starttime != newRecord.starttime 	)
			return null;
			
		ThreadResourceRecord diffRecord = new ThreadResourceRecord();
		diffRecord.previousTimestamp = oldRecord.timestamp;
		diffRecord.timestamp = newRecord.timestamp;
		diffRecord.durationms = newRecord.timestamp - oldRecord.previousTimestamp;
		diffRecord.comm = newRecord.comm;
		diffRecord.state = newRecord.state;
		diffRecord.pid = newRecord.pid;
		diffRecord.ppid = newRecord.ppid;
		diffRecord.starttime = newRecord.starttime;
		diffRecord.minflt = newRecord.minflt.subtract(oldRecord.minflt);
		diffRecord.majflt = newRecord.majflt.subtract(oldRecord.majflt);
		diffRecord.utime = newRecord.utime.subtract(oldRecord.utime);
		diffRecord.stime = newRecord.stime.subtract(oldRecord.stime);
		diffRecord.delayacct_blkio_ticks = newRecord.delayacct_blkio_ticks.subtract(oldRecord.delayacct_blkio_ticks);
		diffRecord.guest_time = newRecord.guest_time.subtract(oldRecord.guest_time);
		diffRecord.clockTick = newRecord.clockTick;
		
		//Derived values:
		if(clockTick > 0){
			diffRecord.cpuUtilPct = diffRecord.utime.add(diffRecord.stime).doubleValue() / 					//The amount of jiffies used by the process over the duration
									(((double)(clockTick * diffRecord.durationms)) / 1000d);	//Divided by the amonut of jiffies that elapsed during the duration
			diffRecord.clockTick = clockTick;
		} else {
			diffRecord.cpuUtilPct = -1d;
		}
		return diffRecord;
	}
	**/
