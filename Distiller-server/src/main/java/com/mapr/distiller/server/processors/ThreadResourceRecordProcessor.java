package com.mapr.distiller.server.processors;

import java.math.BigInteger;

import com.mapr.distiller.server.recordtypes.ThreadResourceRecord;

public class ThreadResourceRecordProcessor implements 
	Thresholdable<ThreadResourceRecord>, MovingAverageable<ThreadResourceRecord> {
	public boolean isNotEqual(ThreadResourceRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	public boolean isEqual(ThreadResourceRecord record, String metric,
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
			
		case "starttime":
			return record.get_starttime() == Long.parseLong(thresholdValue);

		case "delayacct_blkio_ticks":
			return record.get_delayacct_blkio_ticks().equals(new BigInteger(thresholdValue));	
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in ThreadResourceRecord");
		}
	}

	public boolean isAboveThreshold(ThreadResourceRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "cpuUtilPct":
			if(record.getCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw ThreadResourceRecord to threshold");
			else
				return record.getCpuUtilPct() > Double.parseDouble(thresholdValue);
		
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
					+ " is not Thresholdable in ThreadResourceRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(ThreadResourceRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "cpuUtilPct":
			if(record.getCpuUtilPct() == -1d)
				throw new Exception("Can not compare raw ThreadResourceRecord to threshold");
			else
				return record.getCpuUtilPct() < Double.parseDouble(thresholdValue);
		
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
					+ " is not Thresholdable in ThreadResourceRecord");
		}
	}

	@Override
	public ThreadResourceRecord movingAverage(ThreadResourceRecord rec1,
			ThreadResourceRecord rec2) throws Exception{
		return new ThreadResourceRecord(rec1, rec2);
	}
}
