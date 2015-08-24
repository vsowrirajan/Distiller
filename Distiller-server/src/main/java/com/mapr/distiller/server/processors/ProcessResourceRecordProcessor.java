package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.ProcessResourceRecord;
import com.mapr.distiller.server.recordtypes.Record;

public class ProcessResourceRecordProcessor implements RecordProcessor<Record> {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(ProcessResourceRecordProcessor.class);

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric) throws Exception {
		if( rec1.getPreviousTimestamp()==-1l ||
			rec2.getPreviousTimestamp()==-1l )
			throw new Exception("ProcessResourceRecords can only be diff'd from non-raw ProcessResourceRecords");
			
		ProcessResourceRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = (ProcessResourceRecord)rec1;
			newRecord = (ProcessResourceRecord)rec2;
		} else {
			oldRecord = (ProcessResourceRecord)rec2;
			newRecord = (ProcessResourceRecord)rec1;
		}
		
		if(oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception("Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");

		if(oldRecord.get_starttime() != newRecord.get_starttime() || oldRecord.get_pid() != newRecord.get_pid())
			throw new Exception("Can not calculate diff for input records that are from different processes");
		
		switch (metric) {
		case "num_threads":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"int",
												 newRecord.get_num_threads() - oldRecord.get_num_threads() );


		case "cpuUtilPct":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getCpuUtilPct() - oldRecord.getCpuUtilPct() );


		case "iowaitUtilPct":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getIowaitUtilPct() - oldRecord.getIowaitUtilPct() );


		case "readIoCallRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getReadIoCallRate() - oldRecord.getReadIoCallRate() );


		case "writeIoCallRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getWriteIoCallRate() - oldRecord.getWriteIoCallRate() );


		case "readIoCharRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getReadIoCharRate() - oldRecord.getReadIoCharRate() );


		case "writeIoCharRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getWriteIoCharRate() - oldRecord.getWriteIoCharRate() );


		case "readIoByteRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getReadIoByteRate() - oldRecord.getReadIoByteRate() );


		case "writeIoByteRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getWriteIoByteRate() - oldRecord.getWriteIoByteRate() );


		case "cancelledWriteIoByteRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getCancelledWriteIoByteRate() - oldRecord.getCancelledWriteIoByteRate() );

		case "cguest_time":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_cguest_time().subtract(oldRecord.get_cguest_time()) );


		case "cmajflt":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_cmajflt().subtract(oldRecord.get_cmajflt()) );


		case "cminflt":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_cminflt().subtract(oldRecord.get_cminflt()) );


		case "cstime":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_cstime().subtract(oldRecord.get_cstime()) );


		case "cutime":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_cutime().subtract(oldRecord.get_cutime()) );


		case "delayacct_blkio_ticks":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_delayacct_blkio_ticks().subtract(oldRecord.get_delayacct_blkio_ticks()) );


		case "guest_time":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_guest_time().subtract(oldRecord.get_guest_time()) );


		case "majflt":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_majflt().subtract(oldRecord.get_majflt()) );


		case "minflt":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_minflt().subtract(oldRecord.get_minflt()) );


		case "stime":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_stime().subtract(oldRecord.get_stime()) );


		case "utime":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_utime().subtract(oldRecord.get_utime()) );

		case "vsize":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_vsize().subtract(oldRecord.get_vsize()) );

		case "rss":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_rss().subtract(oldRecord.get_rss()) );

		case "rsslim":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_rsslim().subtract(oldRecord.get_rsslim()) );

		case "state":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												 newRecord.get_state() != (oldRecord.get_state()) );


		
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable in ProcessResourceRecordProcessor");
		}
	}	

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

		case "iowaitUtilPct":
			if (currentRecord.getIowaitUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw ProcessResourceRecord to threshold");
			else
				return currentRecord.getIowaitUtilPct() > Double
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
