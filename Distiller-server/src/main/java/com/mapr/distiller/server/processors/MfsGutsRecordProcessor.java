package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.MfsGutsRecord;
import java.math.BigInteger;

public class MfsGutsRecordProcessor implements
		Thresholdable<MfsGutsRecord>, MovingAverageable<MfsGutsRecord> {
	
	public boolean isNotEqual(MfsGutsRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(MfsGutsRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "dbWriteOpsRate":
			if(record.getDbWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbWriteOpsRate() == Double.parseDouble(thresholdValue);

		case "dbReadOpsRate":
			if(record.getDbReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbReadOpsRate() == Double.parseDouble(thresholdValue);

		case "dbRowWritesRate":
			if(record.getDbRowWritesRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbRowWritesRate() == Double.parseDouble(thresholdValue);

		case "dbRowReadsRate":
			if(record.getDbRowReadsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbRowReadsRate() == Double.parseDouble(thresholdValue);

		case "fsWriteOpsRate":
			if(record.getFsWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getFsWriteOpsRate() == Double.parseDouble(thresholdValue);

		case "fsReadOpsRate":
			if(record.getFsReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getFsReadOpsRate() == Double.parseDouble(thresholdValue);

		case "cleanerOpsRate":
			if(record.getCleanerOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getCleanerOpsRate() == Double.parseDouble(thresholdValue);

		case "kvstoreOpsRate":
			if(record.getKvstoreOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getKvstoreOpsRate() == Double.parseDouble(thresholdValue);

		case "btreeReadOpsRate":
			if(record.getBtreeReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getBtreeReadOpsRate() == Double.parseDouble(thresholdValue);

		case "btreeWriteOpsRate":
			if(record.getBtreeWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getBtreeWriteOpsRate() == Double.parseDouble(thresholdValue);

		case "rpcRate":
			if(record.getRpcRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getRpcRate() == Double.parseDouble(thresholdValue);

		case "pcRate":
			if(record.getPcRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getPcRate() == Double.parseDouble(thresholdValue);

		case "slowReadsRate":
			return record.getSlowReadsRate() == Double.parseDouble(thresholdValue);

		case "verySlowReadsRate":
			return record.getVerySlowReadsRate() == Double.parseDouble(thresholdValue);

		case "dbResvFree":
			return record.getDbResvFree().equals(new BigInteger(thresholdValue));
			
		case "logSpaceFree":
			return record.getLogSpaceFree().equals(new BigInteger(thresholdValue));
			
		case "dbWriteOpsInProgress":
			return record.getDbWriteOpsInProgress().equals(new BigInteger(thresholdValue));
			
		case "dbReadOpsInProgress":
			return record.getDbReadOpsInProgress().equals(new BigInteger(thresholdValue));
			
		default:
			throw new Exception("Metric " + metric
					+ " is not comparable in MfsGutsRecord");
		}
	}
	
	@Override
	public boolean isAboveThreshold(MfsGutsRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "dbWriteOpsRate":
			if(record.getDbWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbWriteOpsRate() > Double.parseDouble(thresholdValue);

		case "dbReadOpsRate":
			if(record.getDbReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbReadOpsRate() > Double.parseDouble(thresholdValue);

		case "dbRowWritesRate":
			if(record.getDbRowWritesRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbRowWritesRate() > Double.parseDouble(thresholdValue);

		case "dbRowReadsRate":
			if(record.getDbRowReadsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbRowReadsRate() > Double.parseDouble(thresholdValue);

		case "fsWriteOpsRate":
			if(record.getFsWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getFsWriteOpsRate() > Double.parseDouble(thresholdValue);

		case "fsReadOpsRate":
			if(record.getFsReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getFsReadOpsRate() > Double.parseDouble(thresholdValue);

		case "cleanerOpsRate":
			if(record.getCleanerOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getCleanerOpsRate() > Double.parseDouble(thresholdValue);

		case "kvstoreOpsRate":
			if(record.getKvstoreOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getKvstoreOpsRate() > Double.parseDouble(thresholdValue);

		case "btreeReadOpsRate":
			if(record.getBtreeReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getBtreeReadOpsRate() > Double.parseDouble(thresholdValue);

		case "btreeWriteOpsRate":
			if(record.getBtreeWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getBtreeWriteOpsRate() > Double.parseDouble(thresholdValue);

		case "rpcRate":
			if(record.getRpcRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getRpcRate() > Double.parseDouble(thresholdValue);

		case "pcRate":
			if(record.getPcRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getPcRate() > Double.parseDouble(thresholdValue);

		case "slowReadsRate":
			return record.getSlowReadsRate() > Double.parseDouble(thresholdValue);

		case "verySlowReadsRate":
			return record.getVerySlowReadsRate() > Double.parseDouble(thresholdValue);

		case "dbResvFree":
			return record.getDbResvFree().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "logSpaceFree":
			return record.getLogSpaceFree().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "dbWriteOpsInProgress":
			return record.getDbWriteOpsInProgress().compareTo(new BigInteger(thresholdValue)) == 1;
			
		case "dbReadOpsInProgress":
			return record.getDbReadOpsInProgress().compareTo(new BigInteger(thresholdValue)) == 1;
			
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in MfsGutsRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(MfsGutsRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "dbWriteOpsRate":
			if(record.getDbWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbWriteOpsRate() < Double.parseDouble(thresholdValue);

		case "dbReadOpsRate":
			if(record.getDbReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbReadOpsRate() < Double.parseDouble(thresholdValue);

		case "dbRowWritesRate":
			if(record.getDbRowWritesRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbRowWritesRate() < Double.parseDouble(thresholdValue);

		case "dbRowReadsRate":
			if(record.getDbRowReadsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getDbRowReadsRate() < Double.parseDouble(thresholdValue);

		case "fsWriteOpsRate":
			if(record.getFsWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getFsWriteOpsRate() < Double.parseDouble(thresholdValue);

		case "fsReadOpsRate":
			if(record.getFsReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getFsReadOpsRate() < Double.parseDouble(thresholdValue);

		case "cleanerOpsRate":
			if(record.getCleanerOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getCleanerOpsRate() < Double.parseDouble(thresholdValue);

		case "kvstoreOpsRate":
			if(record.getKvstoreOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getKvstoreOpsRate() < Double.parseDouble(thresholdValue);

		case "btreeReadOpsRate":
			if(record.getBtreeReadOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getBtreeReadOpsRate() < Double.parseDouble(thresholdValue);

		case "btreeWriteOpsRate":
			if(record.getBtreeWriteOpsRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getBtreeWriteOpsRate() < Double.parseDouble(thresholdValue);

		case "rpcRate":
			if(record.getRpcRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getRpcRate() < Double.parseDouble(thresholdValue);

		case "pcRate":
			if(record.getPcRate() == -1d)
				throw new Exception("Can not compare raw MfsGutsRecord to value");
			else
				return record.getPcRate() < Double.parseDouble(thresholdValue);

		case "slowReadsRate":
			return record.getSlowReadsRate() < Double.parseDouble(thresholdValue);

		case "verySlowReadsRate":
			return record.getVerySlowReadsRate() < Double.parseDouble(thresholdValue);

		case "dbResvFree":
			return record.getDbResvFree().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "logSpaceFree":
			return record.getLogSpaceFree().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "dbWriteOpsInProgress":
			return record.getDbWriteOpsInProgress().compareTo(new BigInteger(thresholdValue)) == -1;
			
		case "dbReadOpsInProgress":
			return record.getDbReadOpsInProgress().compareTo(new BigInteger(thresholdValue)) == -1;
			
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in MfsGutsRecord");
		}
	}

	@Override
	public MfsGutsRecord movingAverage(MfsGutsRecord rec1,
			MfsGutsRecord rec2) throws Exception{
		return new MfsGutsRecord(rec1, rec2);
	}
}
