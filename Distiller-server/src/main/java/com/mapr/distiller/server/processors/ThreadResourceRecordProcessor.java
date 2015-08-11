package com.mapr.distiller.server.processors;

import java.math.BigInteger;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.ThreadResourceRecord;

public class ThreadResourceRecordProcessor implements RecordProcessor<Record> {
	public String getName(){
		return "ThreadResourceRecordProcessor";
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		ThreadResourceRecord currentRecord = (ThreadResourceRecord) record;

		switch (metric) {
		case "comm":
			return currentRecord.get_comm().equals(thresholdValue);

		case "state":
			return currentRecord.get_state() == thresholdValue.charAt(0);

		case "pid":
			return currentRecord.get_pid() == Integer.parseInt(thresholdValue);

		case "ppid":
			return currentRecord.get_ppid() == Integer.parseInt(thresholdValue);

		case "starttime":
			return currentRecord.get_starttime() == Long
					.parseLong(thresholdValue);

		case "delayacct_blkio_ticks":
			return currentRecord.get_delayacct_blkio_ticks().equals(
					new BigInteger(thresholdValue));

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in ThreadResourceRecord");
		}
	}

	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		ThreadResourceRecord currentRecord = (ThreadResourceRecord) record;

		switch (metric) {
		case "cpuUtilPct":
			if (currentRecord.getCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw ThreadResourceRecord to threshold");
			else
				return currentRecord.getCpuUtilPct() > Double
						.parseDouble(thresholdValue);

		case "delayacct_blkio_ticks":
			return currentRecord.get_delayacct_blkio_ticks().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "guest_time":
			return currentRecord.get_guest_time().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "majflt":
			return currentRecord.get_majflt().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "minflt":
			return currentRecord.get_minflt().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "stime":
			return currentRecord.get_stime().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "utime":
			return currentRecord.get_utime().compareTo(
					new BigInteger(thresholdValue)) == 1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in ThreadResourceRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		ThreadResourceRecord currentRecord = (ThreadResourceRecord) record;

		switch (metric) {
		case "cpuUtilPct":
			if (currentRecord.getCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw ThreadResourceRecord to threshold");
			else
				return currentRecord.getCpuUtilPct() < Double
						.parseDouble(thresholdValue);

		case "delayacct_blkio_ticks":
			return currentRecord.get_delayacct_blkio_ticks().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "guest_time":
			return currentRecord.get_guest_time().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "majflt":
			return currentRecord.get_majflt().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "minflt":
			return currentRecord.get_minflt().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "stime":
			return currentRecord.get_stime().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "utime":
			return currentRecord.get_utime().compareTo(
					new BigInteger(thresholdValue)) == -1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in ThreadResourceRecord");
		}
	}

	@Override
	public ThreadResourceRecord movingAverage(Record rec1, Record rec2)
			throws Exception {
		return new ThreadResourceRecord((ThreadResourceRecord) rec1,
				(ThreadResourceRecord) rec2);
	}
}
