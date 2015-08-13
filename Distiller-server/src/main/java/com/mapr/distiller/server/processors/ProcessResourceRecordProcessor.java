package com.mapr.distiller.server.processors;

import java.math.BigInteger;

import com.mapr.distiller.server.recordtypes.ProcessResourceRecord;
import com.mapr.distiller.server.recordtypes.Record;

public class ProcessResourceRecordProcessor implements RecordProcessor<Record> {

	public String getName(){
		return "ProcessResourceRecordProcessor";
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		ProcessResourceRecord currentRecord = (ProcessResourceRecord) record;

		switch (metric) {
		case "comm":
			return currentRecord.get_comm().equals(thresholdValue);

		case "state":
			return currentRecord.get_state() == thresholdValue.charAt(0);

		case "pid":
			return currentRecord.get_pid() == Integer.parseInt(thresholdValue);

		case "ppid":
			return currentRecord.get_ppid() == Integer.parseInt(thresholdValue);

		case "pgrp":
			return currentRecord.get_pgrp() == Integer.parseInt(thresholdValue);

		case "num_threads":
			return currentRecord.get_num_threads() == Integer
					.parseInt(thresholdValue);

		case "starttime":
			return currentRecord.get_starttime() == Long
					.parseLong(thresholdValue);

		case "rss":
			return currentRecord.get_rsslim().equals(
					new BigInteger(thresholdValue));

		case "rsslim":
			return currentRecord.get_rss().equals(
					new BigInteger(thresholdValue));

		case "vsize":
			return currentRecord.get_vsize().equals(
					new BigInteger(thresholdValue));

		case "delayacct_blkio_ticks":
			return currentRecord.get_delayacct_blkio_ticks().equals(
					new BigInteger(thresholdValue));

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in ProcessResourceRecord");
		}
	}

	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		ProcessResourceRecord currentRecord = (ProcessResourceRecord) record;

		switch (metric) {
		case "cpuUtilPct":
			if (currentRecord.getCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw ProcessResourceRecord to threshold");
			else
				return currentRecord.getCpuUtilPct() > Double
						.parseDouble(thresholdValue);

		case "num_threads":
			return currentRecord.get_num_threads() > Integer
					.parseInt(thresholdValue);

		case "cguest_time":
			return currentRecord.get_cguest_time().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "cmajflt":
			return currentRecord.get_cmajflt().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "cminflt":
			return currentRecord.get_cminflt().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "cstime":
			return currentRecord.get_cstime().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "cutime":
			return currentRecord.get_cutime().compareTo(
					new BigInteger(thresholdValue)) == 1;

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
					+ " is not Thresholdable in ProcessResourceRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		ProcessResourceRecord currentRecord = (ProcessResourceRecord) record;

		switch (metric) {
		case "cpuUtilPct":
			if (currentRecord.getCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw ProcessResourceRecord to threshold");
			else
				return currentRecord.getCpuUtilPct() < Double
						.parseDouble(thresholdValue);

		case "num_threads":
			return currentRecord.get_num_threads() < Integer
					.parseInt(thresholdValue);

		case "cguest_time":
			return currentRecord.get_cguest_time().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "cmajflt":
			return currentRecord.get_cmajflt().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "cminflt":
			return currentRecord.get_cminflt().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "cstime":
			return currentRecord.get_cstime().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "cutime":
			return currentRecord.get_cutime().compareTo(
					new BigInteger(thresholdValue)) == -1;

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
					+ " is not Thresholdable in ProcessResourceRecord");
		}
	}

	@Override
	public ProcessResourceRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new ProcessResourceRecord((ProcessResourceRecord) rec1,
				(ProcessResourceRecord) rec2);
	}

}
