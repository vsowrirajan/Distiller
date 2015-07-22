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
	//private double cpuUtilPct = -1d;
	
	/**
	 * RAW VALUES
	 */
	private String comm;
	private char state;
	private int pid, ppid, pgrp, num_threads, clockTick=0;
	private long starttime;
	private BigInteger cguest_time, cmajflt, cminflt, cstime, cutime, delayacct_blkio_ticks, guest_time, majflt, minflt, rss, rsslim, stime, utime, vsize;

	/**
	 * CONSTRUCTORS
	 */
	public ProcessResourceRecord(String path, int clockTick) throws Exception {
		//This constructor takes "path" which should be the path to a /proc/[pid]/stat file
		//This constructor takes "clockTick" which should be set to jiffies per second from kernel build (obtained in ProcRecordProducer)
		super(System.currentTimeMillis());
		this.clockTick = clockTick;
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
	public static boolean produceRecord(RecordQueue process_resources, String path, int clockTick){
		try{
			process_resources.put(new ProcessResourceRecord(path, clockTick));
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

}
/**

	public static ProcessResourceRecord diff(ProcessResourceRecord oldRecord, ProcessResourceRecord newRecord){
		//This function should be called to diff ProcessResourceRecords when the clockTick field of those records is populated.
		//If this is called against records without clockTick populated then no cpu utilization will be calculated (because we can't).
		
		//Only diff the records if they are from the same process, as identified by the PID and starttime, and if the oldRecord is
		//indeed older than the newRecord by timestamp.
		//This works under the assumption that the system is not able to cycle through all process IDs and reuse them within a single
		//tick of the starttime clock
		if(	oldRecord.pid != newRecord.pid 				||
			oldRecord.starttime != newRecord.starttime 	||
			oldRecord.timestamp >= newRecord.timestamp	)
			return null;
			
		ProcessResourceRecord diffRecord = new ProcessResourceRecord();
		diffRecord.previousTimestamp = oldRecord.timestamp;
		diffRecord.timestamp = newRecord.timestamp;
		diffRecord.comm = newRecord.comm;
		diffRecord.state = newRecord.state;
		diffRecord.pid = newRecord.pid;
		diffRecord.ppid = newRecord.ppid;
		diffRecord.pgrp = newRecord.pgrp;
		diffRecord.num_threads = newRecord.num_threads;
		diffRecord.starttime = newRecord.starttime;
		diffRecord.vsize = newRecord.vsize;
		diffRecord.rss = newRecord.rss;
		diffRecord.rsslim = newRecord.rsslim;
		diffRecord.minflt = newRecord.minflt.subtract(oldRecord.minflt);
		diffRecord.cminflt = newRecord.cminflt.subtract(oldRecord.cminflt);
		diffRecord.cmajflt = newRecord.cmajflt.subtract(oldRecord.cmajflt);
		diffRecord.cutime = newRecord.cutime.subtract(oldRecord.cutime);
		diffRecord.cstime = newRecord.cstime.subtract(oldRecord.cstime);
		diffRecord.cguest_time = newRecord.cguest_time.subtract(oldRecord.cguest_time);
		diffRecord.majflt = newRecord.majflt.subtract(oldRecord.majflt);
		diffRecord.utime = newRecord.utime.subtract(oldRecord.utime);
		diffRecord.stime = newRecord.stime.subtract(oldRecord.stime);
		diffRecord.delayacct_blkio_ticks = newRecord.delayacct_blkio_ticks.subtract(oldRecord.delayacct_blkio_ticks);
		diffRecord.guest_time = newRecord.guest_time.subtract(oldRecord.guest_time);
		diffRecord.clockTick = newRecord.clockTick;
		
		//Derived values:
		if(diffRecord.clockTick > 0){
			diffRecord.cpuUtilPct = diffRecord.utime.add(diffRecord.stime).doubleValue() / 					//The amount of jiffies used by the process over the duration
									(((double)(diffRecord.clockTick * diffRecord.getDurationms())) / 1000d);	//Divided by the amonut of jiffies that elapsed during the duration
		} else {
			diffRecord.cpuUtilPct = -1d;
		}
		return diffRecord;
	}
	public static ProcessResourceRecord diff(ProcessResourceRecord oldRecord, ProcessResourceRecord newRecord, int clockTick){
		//This function should be called to diff ProcessResourceRecords when the clockTick field of those records is not populated,
		//or when it is desirable to override clockTick saved in the records (not sure when that would ever be)
		
		//Only diff the records if they are from the same process, as identified by the PID and starttime.
		//This works under the assumption that the system is not able to cycle through all process IDs and reuse them within a single
		//tick of the starttime clock
		if(	oldRecord.pid != newRecord.pid 				||
			oldRecord.starttime != newRecord.starttime 	)
			return null;

			
		ProcessResourceRecord diffRecord = new ProcessResourceRecord();
		diffRecord.previousTimestamp = oldRecord.timestamp;
		diffRecord.timestamp = newRecord.timestamp;
		diffRecord.comm = newRecord.comm;
		diffRecord.state = newRecord.state;
		diffRecord.pid = newRecord.pid;
		diffRecord.ppid = newRecord.ppid;
		diffRecord.pgrp = newRecord.pgrp;
		diffRecord.num_threads = newRecord.num_threads;
		diffRecord.starttime = newRecord.starttime;
		diffRecord.vsize = newRecord.vsize;
		diffRecord.rss = newRecord.rss;
		diffRecord.rsslim = newRecord.rsslim;
		diffRecord.minflt = newRecord.minflt.subtract(oldRecord.minflt);
		diffRecord.cminflt = newRecord.cminflt.subtract(oldRecord.cminflt);
		diffRecord.cmajflt = newRecord.cmajflt.subtract(oldRecord.cmajflt);
		diffRecord.cutime = newRecord.cutime.subtract(oldRecord.cutime);
		diffRecord.cstime = newRecord.cstime.subtract(oldRecord.cstime);
		diffRecord.cguest_time = newRecord.cguest_time.subtract(oldRecord.cguest_time);
		diffRecord.majflt = newRecord.majflt.subtract(oldRecord.majflt);
		diffRecord.utime = newRecord.utime.subtract(oldRecord.utime);
		diffRecord.stime = newRecord.stime.subtract(oldRecord.stime);
		diffRecord.delayacct_blkio_ticks = newRecord.delayacct_blkio_ticks.subtract(oldRecord.delayacct_blkio_ticks);
		diffRecord.guest_time = newRecord.guest_time.subtract(oldRecord.guest_time);
		
		//Derived values:
		if(clockTick > 0){
			diffRecord.cpuUtilPct = diffRecord.utime.add(diffRecord.stime).doubleValue() / 				//The amount of jiffies used by the process over the duration
									(((double)(clockTick * diffRecord.getDurationms())) / 1000d);	//Divided by the amonut of jiffies that elapsed during the duration
			diffRecord.clockTick = clockTick;
		} else {
			diffRecord.cpuUtilPct = -1d;
		}

		return diffRecord;
	}
*/
