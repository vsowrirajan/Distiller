package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.MfsGutsRecord;
import com.mapr.distiller.server.recordtypes.Record;

import java.math.BigInteger;

public class MfsGutsRecordProcessor implements RecordProcessor<Record> {

	public String getName(){
		return "MfsGutsRecordProcessor";
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		MfsGutsRecord currentRecord = (MfsGutsRecord) record;

		switch (metric) {
		case "dbWriteOpsRate":
			if (currentRecord.getDbWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbWriteOpsRate() == Double
						.parseDouble(thresholdValue);

		case "dbReadOpsRate":
			if (currentRecord.getDbReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbReadOpsRate() == Double
						.parseDouble(thresholdValue);

		case "dbRowWritesRate":
			if (currentRecord.getDbRowWritesRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbRowWritesRate() == Double
						.parseDouble(thresholdValue);

		case "dbRowReadsRate":
			if (currentRecord.getDbRowReadsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbRowReadsRate() == Double
						.parseDouble(thresholdValue);

		case "fsWriteOpsRate":
			if (currentRecord.getFsWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getFsWriteOpsRate() == Double
						.parseDouble(thresholdValue);

		case "fsReadOpsRate":
			if (currentRecord.getFsReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getFsReadOpsRate() == Double
						.parseDouble(thresholdValue);

		case "cleanerOpsRate":
			if (currentRecord.getCleanerOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getCleanerOpsRate() == Double
						.parseDouble(thresholdValue);

		case "kvstoreOpsRate":
			if (currentRecord.getKvstoreOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getKvstoreOpsRate() == Double
						.parseDouble(thresholdValue);

		case "btreeReadOpsRate":
			if (currentRecord.getBtreeReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getBtreeReadOpsRate() == Double
						.parseDouble(thresholdValue);

		case "btreeWriteOpsRate":
			if (currentRecord.getBtreeWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getBtreeWriteOpsRate() == Double
						.parseDouble(thresholdValue);

		case "rpcRate":
			if (currentRecord.getRpcRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getRpcRate() == Double
						.parseDouble(thresholdValue);

		case "pcRate":
			if (currentRecord.getPcRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getPcRate() == Double
						.parseDouble(thresholdValue);

		case "slowReadsRate":
			return currentRecord.getSlowReadsRate() == Double
					.parseDouble(thresholdValue);

		case "verySlowReadsRate":
			return currentRecord.getVerySlowReadsRate() == Double
					.parseDouble(thresholdValue);

		case "dbResvFree":
			return currentRecord.getDbResvFree().equals(
					new BigInteger(thresholdValue));

		case "logSpaceFree":
			return currentRecord.getLogSpaceFree().equals(
					new BigInteger(thresholdValue));

		case "dbWriteOpsInProgress":
			return currentRecord.getDbWriteOpsInProgress().equals(
					new BigInteger(thresholdValue));

		case "dbReadOpsInProgress":
			return currentRecord.getDbReadOpsInProgress().equals(
					new BigInteger(thresholdValue));

		default:
			throw new Exception("Metric " + metric
					+ " is not comparable in MfsGutsRecord");
		}
	}

	@Override
	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		MfsGutsRecord currentRecord = (MfsGutsRecord) record;

		switch (metric) {
		case "dbWriteOpsRate":
			if (currentRecord.getDbWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbWriteOpsRate() > Double
						.parseDouble(thresholdValue);

		case "dbReadOpsRate":
			if (currentRecord.getDbReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbReadOpsRate() > Double
						.parseDouble(thresholdValue);

		case "dbRowWritesRate":
			if (currentRecord.getDbRowWritesRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbRowWritesRate() > Double
						.parseDouble(thresholdValue);

		case "dbRowReadsRate":
			if (currentRecord.getDbRowReadsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbRowReadsRate() > Double
						.parseDouble(thresholdValue);

		case "fsWriteOpsRate":
			if (currentRecord.getFsWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getFsWriteOpsRate() > Double
						.parseDouble(thresholdValue);

		case "fsReadOpsRate":
			if (currentRecord.getFsReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getFsReadOpsRate() > Double
						.parseDouble(thresholdValue);

		case "cleanerOpsRate":
			if (currentRecord.getCleanerOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getCleanerOpsRate() > Double
						.parseDouble(thresholdValue);

		case "kvstoreOpsRate":
			if (currentRecord.getKvstoreOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getKvstoreOpsRate() > Double
						.parseDouble(thresholdValue);

		case "btreeReadOpsRate":
			if (currentRecord.getBtreeReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getBtreeReadOpsRate() > Double
						.parseDouble(thresholdValue);

		case "btreeWriteOpsRate":
			if (currentRecord.getBtreeWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getBtreeWriteOpsRate() > Double
						.parseDouble(thresholdValue);

		case "rpcRate":
			if (currentRecord.getRpcRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getRpcRate() > Double
						.parseDouble(thresholdValue);

		case "pcRate":
			if (currentRecord.getPcRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getPcRate() > Double
						.parseDouble(thresholdValue);

		case "slowReadsRate":
			return currentRecord.getSlowReadsRate() > Double
					.parseDouble(thresholdValue);

		case "verySlowReadsRate":
			return currentRecord.getVerySlowReadsRate() > Double
					.parseDouble(thresholdValue);

		case "dbResvFree":
			return currentRecord.getDbResvFree().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "logSpaceFree":
			return currentRecord.getLogSpaceFree().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "dbWriteOpsInProgress":
			return currentRecord.getDbWriteOpsInProgress().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "dbReadOpsInProgress":
			return currentRecord.getDbReadOpsInProgress().compareTo(
					new BigInteger(thresholdValue)) == 1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in MfsGutsRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		MfsGutsRecord currentRecord = (MfsGutsRecord) record;

		switch (metric) {
		case "dbWriteOpsRate":
			if (currentRecord.getDbWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbWriteOpsRate() < Double
						.parseDouble(thresholdValue);

		case "dbReadOpsRate":
			if (currentRecord.getDbReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbReadOpsRate() < Double
						.parseDouble(thresholdValue);

		case "dbRowWritesRate":
			if (currentRecord.getDbRowWritesRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbRowWritesRate() < Double
						.parseDouble(thresholdValue);

		case "dbRowReadsRate":
			if (currentRecord.getDbRowReadsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getDbRowReadsRate() < Double
						.parseDouble(thresholdValue);

		case "fsWriteOpsRate":
			if (currentRecord.getFsWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getFsWriteOpsRate() < Double
						.parseDouble(thresholdValue);

		case "fsReadOpsRate":
			if (currentRecord.getFsReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getFsReadOpsRate() < Double
						.parseDouble(thresholdValue);

		case "cleanerOpsRate":
			if (currentRecord.getCleanerOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getCleanerOpsRate() < Double
						.parseDouble(thresholdValue);

		case "kvstoreOpsRate":
			if (currentRecord.getKvstoreOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getKvstoreOpsRate() < Double
						.parseDouble(thresholdValue);

		case "btreeReadOpsRate":
			if (currentRecord.getBtreeReadOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getBtreeReadOpsRate() < Double
						.parseDouble(thresholdValue);

		case "btreeWriteOpsRate":
			if (currentRecord.getBtreeWriteOpsRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getBtreeWriteOpsRate() < Double
						.parseDouble(thresholdValue);

		case "rpcRate":
			if (currentRecord.getRpcRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getRpcRate() < Double
						.parseDouble(thresholdValue);

		case "pcRate":
			if (currentRecord.getPcRate() == -1d)
				throw new Exception(
						"Can not compare raw MfsGutsRecord to value");
			else
				return currentRecord.getPcRate() < Double
						.parseDouble(thresholdValue);

		case "slowReadsRate":
			return currentRecord.getSlowReadsRate() < Double
					.parseDouble(thresholdValue);

		case "verySlowReadsRate":
			return currentRecord.getVerySlowReadsRate() < Double
					.parseDouble(thresholdValue);

		case "dbResvFree":
			return currentRecord.getDbResvFree().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "logSpaceFree":
			return currentRecord.getLogSpaceFree().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "dbWriteOpsInProgress":
			return currentRecord.getDbWriteOpsInProgress().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "dbReadOpsInProgress":
			return currentRecord.getDbReadOpsInProgress().compareTo(
					new BigInteger(thresholdValue)) == -1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in MfsGutsRecord");
		}
	}

	@Override
	public MfsGutsRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new MfsGutsRecord((MfsGutsRecord) rec1, (MfsGutsRecord) rec2);
	}
}
