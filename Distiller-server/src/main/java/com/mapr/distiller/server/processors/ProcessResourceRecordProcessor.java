package com.mapr.distiller.server.processors;

import java.math.BigInteger;

import com.mapr.distiller.server.recordtypes.ProcessResourceRecord;

public class ProcessResourceRecordProcessor implements 
		Thresholdable<ProcessResourceRecord>, MovingAverageable<ProcessResourceRecord> {
	
	public boolean isNotEqual(ProcessResourceRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}
	
	private double cpuUtilPct;;
	
	/**
	 * RAW VALUES
	 */
	private String comm;
	private char state;
	private int pid, ppid, pgrp, num_threads, clockTick;
	private long starttime;
	private BigInteger cguest_time, cmajflt, cminflt, cstime, cutime, delayacct_blkio_ticks, guest_time, majflt, minflt, rss, rsslim, stime, utime, vsize;
	
	public boolean isEqual(ProcessResourceRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "comm":
			return record.get_comm().equals(thresholdValue);

		case "state":
			return record.get_state() == thresholdValue.charAt(0);
			
		case "pid":
			return record.get_pid() == Integer.parseInt(thresholdValue);
			
		case "ppid":
			return record.get_ppid() == Integer.parseInt(thresholdValue);
			
		case "pgrp":
			return record.get_pgrp() == Integer.parseInt(thresholdValue);
			
		case "num_threads":
			return record.get_num_threads() == Integer.parseInt(thresholdValue);
			
		case "starttime":
			return record.get_starttime() == Long.parseLong(thresholdValue);
			
		case "rss":
			return record.get_rsslim().equals(new BigInteger(thresholdValue));
			
		case "rsslim":
			return record.get_rss().equals(new BigInteger(thresholdValue));
			
		case "vsize":
			return record.get_vsize().equals(new BigInteger(thresholdValue));
			
		case "delayacct_blkio_ticks":
			return record.get_delayacct_blkio_ticks().equals(new BigInteger(thresholdValue));	
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	public boolean isAboveThreshold(ProcessResourceRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "cpuUtilPct":
			if(record.getCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw ProcessResourceRecord to threshold");
			else
				return record.getCpuUtilPct() > Double.parseDouble(thresholdValue);
		
		case "num_threads":
			return record.get_num_threads() > Integer.parseInt(thresholdValue);
		
		case "cguest_time":
			return record.get_cguest_time().compareTo(new BigInteger(thresholdValue)) == 1;
		
		case "cmajflt":
			return record.get_cmajflt().compareTo(new BigInteger(thresholdValue)) == 1;
		
		case "cminflt":
			return record.get_cminflt().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "cstime":
			return record.get_cstime().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "cutime":
			return record.get_cutime().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "delayacct_blkio_ticks":
			return record.get_delayacct_blkio_ticks().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "guest_time":
			return record.get_guest_time().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "majflt":
			return record.get_majflt().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "minflt":
			return record.get_minflt().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "stime":
			return record.get_stime().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "utime":
			return record.get_utime().compareTo(new BigInteger(thresholdValue)) == 1;
			
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(ProcessResourceRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "cpuUtilPct":
			if(record.getCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw ProcessResourceRecord to threshold");
			else
				return record.getCpuUtilPct() < Double.parseDouble(thresholdValue);
		
		case "num_threads":
			return record.get_num_threads() < Integer.parseInt(thresholdValue);
		
		case "cguest_time":
			return record.get_cguest_time().compareTo(new BigInteger(thresholdValue)) == -1;
		
		case "cmajflt":
			return record.get_cmajflt().compareTo(new BigInteger(thresholdValue)) == -1;
		
		case "cminflt":
			return record.get_cminflt().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "cstime":
			return record.get_cstime().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "cutime":
			return record.get_cutime().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "delayacct_blkio_ticks":
			return record.get_delayacct_blkio_ticks().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "guest_time":
			return record.get_guest_time().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "majflt":
			return record.get_majflt().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "minflt":
			return record.get_minflt().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "stime":
			return record.get_stime().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "utime":
			return record.get_utime().compareTo(new BigInteger(thresholdValue)) == -1;
			
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	@Override
	public ProcessResourceRecord movingAverage(ProcessResourceRecord rec1,
			ProcessResourceRecord rec2) throws Exception{
		return new ProcessResourceRecord(rec1, rec2);
	}

}
