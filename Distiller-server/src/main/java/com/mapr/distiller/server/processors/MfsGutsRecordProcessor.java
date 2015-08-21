package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;
import com.mapr.distiller.server.recordtypes.MfsGutsRecord;
import com.mapr.distiller.server.recordtypes.Record;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MfsGutsRecordProcessor implements RecordProcessor<Record> {

	private static final Logger LOG = LoggerFactory
			.getLogger(MfsGutsRecordProcessor.class);

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric)
			throws Exception {
		if (rec1.getPreviousTimestamp() == -1l
				|| rec2.getPreviousTimestamp() == -1l)
			throw new Exception(
					"MfsGutsRecords can only be diff'd from non-raw MfsGutsRecords");

		MfsGutsRecord oldRecord, newRecord;
		if (rec1.getTimestamp() < rec2.getTimestamp()) {
			oldRecord = (MfsGutsRecord) rec1;
			newRecord = (MfsGutsRecord) rec2;
		} else {
			oldRecord = (MfsGutsRecord) rec2;
			newRecord = (MfsGutsRecord) rec1;
		}

		if (oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception(
					"Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");

		switch (metric) {
		case "dbWriteOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getDbWriteOpsRate()
							- oldRecord.getDbWriteOpsRate());

		case "dbReadOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getDbReadOpsRate()
							- oldRecord.getDbReadOpsRate());

		case "dbRowWritesRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getDbRowWritesRate()
							- oldRecord.getDbRowWritesRate());

		case "dbRowReadsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getDbRowReadsRate()
							- oldRecord.getDbRowReadsRate());

		case "fsWriteOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getFsWriteOpsRate()
							- oldRecord.getFsWriteOpsRate());

		case "fsReadOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getFsReadOpsRate()
							- oldRecord.getFsReadOpsRate());

		case "cleanerOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getCleanerOpsRate()
							- oldRecord.getCleanerOpsRate());

		case "kvstoreOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getKvstoreOpsRate()
							- oldRecord.getKvstoreOpsRate());

		case "btreeReadOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getBtreeReadOpsRate()
							- oldRecord.getBtreeReadOpsRate());

		case "btreeWriteOpsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getBtreeWriteOpsRate()
							- oldRecord.getBtreeWriteOpsRate());

		case "rpcRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getRpcRate()
							- oldRecord.getRpcRate());

		case "pcRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getPcRate()
							- oldRecord.getPcRate());

		case "slowReadsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getSlowReadsRate()
							- oldRecord.getSlowReadsRate());

		case "verySlowReadsRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getVerySlowReadsRate()
							- oldRecord.getVerySlowReadsRate());

		case "dbResvFree":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "BigInteger", newRecord.getDbResvFree()
							.subtract(oldRecord.getDbResvFree()));

		case "logSpaceFree":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "BigInteger", newRecord
							.getLogSpaceFree().subtract(
									oldRecord.getLogSpaceFree()));

		case "dbWriteOpsInProgress":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "BigInteger", newRecord
							.getDbWriteOpsInProgress().subtract(
									oldRecord.getDbWriteOpsInProgress()));

		case "dbReadOpsInProgress":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "BigInteger", newRecord
							.getDbReadOpsInProgress().subtract(
									oldRecord.getDbReadOpsInProgress()));

		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable in MfsGutsRecordProcessor");
		}
	}

	public String getName() {
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
	public MfsGutsRecord merge(Record rec1, Record rec2) throws Exception {
		return new MfsGutsRecord((MfsGutsRecord) rec1, (MfsGutsRecord) rec2);
	}
}
