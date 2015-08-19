package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;

import java.math.BigInteger;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.ThreadResourceRecord;

public class ThreadResourceRecordProcessor implements RecordProcessor<Record> {

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric) throws Exception {
		if( rec1.getPreviousTimestamp()==-1l ||
			rec2.getPreviousTimestamp()==-1l )
			throw new Exception("ThreadResourceRecords can only be diff'd from non-raw ThreadResourceRecords");
			
		ThreadResourceRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = (ThreadResourceRecord)rec1;
			newRecord = (ThreadResourceRecord)rec2;
		} else {
			oldRecord = (ThreadResourceRecord)rec2;
			newRecord = (ThreadResourceRecord)rec1;
		}
		
		if(oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception("Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");

		if(oldRecord.get_starttime() != newRecord.get_starttime() || oldRecord.get_pid() != newRecord.get_pid())
			throw new Exception("Can not calculate diff for input records that are from different threads");
		
		switch (metric) {
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


		case "readCallRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getReadCallRate() - oldRecord.getReadCallRate() );


		case "writeCallRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getWriteCallRate() - oldRecord.getWriteCallRate() );


		case "readCharRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getReadCharRate() - oldRecord.getReadCharRate() );


		case "writeCharRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getWriteCharRate() - oldRecord.getWriteCharRate() );


		case "readByteRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getReadByteRate() - oldRecord.getReadByteRate() );


		case "writeByteRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getWriteByteRate() - oldRecord.getWriteByteRate() );


		case "cancelledWriteByteRate":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getCancelledWriteByteRate() - oldRecord.getCancelledWriteByteRate() );


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
					+ " is not Diffable in ThreadResourceRecordProcessor");
		}
	}	

	
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
		case "idlePct":
			if(currentRecord.getCpuUtilPct() == -1d || currentRecord.getIowaitUtilPct() == -1d)
				throw new Exception("Can not compare raw ThreadResourceRecord to threshold");
			else
				return 1d - currentRecord.getCpuUtilPct() - currentRecord.getIowaitUtilPct() > Double.parseDouble(thresholdValue);
				
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
		case "idlePct":
			if(currentRecord.getCpuUtilPct() == -1d || currentRecord.getIowaitUtilPct() == -1d)
				throw new Exception("Can not compare raw SlimThreadResourceRecord to threshold");
			else
				return 1d - currentRecord.getCpuUtilPct() - currentRecord.getIowaitUtilPct() < Double.parseDouble(thresholdValue);
				
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
	public ThreadResourceRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new ThreadResourceRecord((ThreadResourceRecord) rec1,
				(ThreadResourceRecord) rec2);
	}
}
