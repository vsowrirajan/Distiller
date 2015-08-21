package com.mapr.distiller.server.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;
import com.mapr.distiller.server.recordtypes.SystemCpuRecord;

public class SystemCpuRecordProcessor implements RecordProcessor<Record> {

	private static final Logger LOG = LoggerFactory
			.getLogger(SystemCpuRecordProcessor.class);

	public String getName() {
		return "SystemCpuRecordProcessor";
	}

	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		SystemCpuRecord currentRecord = (SystemCpuRecord) record;

		switch (metric) {
		case "%idle":
			if (currentRecord.getIdleCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to value");
			else
				return currentRecord.getIdleCpuUtilPct() == Double
						.parseDouble(thresholdValue);

		case "%iowait":
			if (currentRecord.getIowaitCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to value");
			else
				return currentRecord.getIowaitCpuUtilPct() == Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		SystemCpuRecord currentRecord = (SystemCpuRecord) record;

		switch (metric) {
		case "%idle":
			if (currentRecord.getIdleCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIdleCpuUtilPct() > Double
						.parseDouble(thresholdValue);

		case "%iowait":
			if (currentRecord.getIowaitCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIowaitCpuUtilPct() > Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {
		SystemCpuRecord currentRecord = (SystemCpuRecord) record;

		switch (metric) {
		case "%idle":
			if (currentRecord.getIdleCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIdleCpuUtilPct() < Double
						.parseDouble(thresholdValue);

		case "%iowait":
			if (currentRecord.getIowaitCpuUtilPct() == -1d)
				throw new Exception(
						"Can not compare raw SystemCpuRecord to threshold");
			else
				return currentRecord.getIowaitCpuUtilPct() < Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in SystemCpuRecord");
		}
	}

	@Override
	public SystemCpuRecord merge(Record rec1, Record rec2) throws Exception {
		return new SystemCpuRecord((SystemCpuRecord) rec1,
				(SystemCpuRecord) rec2);
	}

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric)
			throws Exception {
		if (rec1.getPreviousTimestamp() == -1l
				|| rec2.getPreviousTimestamp() == -1l)
			throw new Exception(
					"SystemCpuRecords can only be diff'd from non-raw SystemCpuRecords");

		SystemCpuRecord oldRecord, newRecord;
		if (rec1.getTimestamp() < rec2.getTimestamp()) {
			oldRecord = (SystemCpuRecord) rec1;
			newRecord = (SystemCpuRecord) rec2;
		} else {
			oldRecord = (SystemCpuRecord) rec2;
			newRecord = (SystemCpuRecord) rec1;
		}

		if (oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception(
					"Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");

		switch (metric) {
		case "%idle":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getIdleCpuUtilPct()
							- oldRecord.getIdleCpuUtilPct());

		case "%iowait":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getIowaitCpuUtilPct()
							- oldRecord.getIowaitCpuUtilPct());

		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable in SystemCpuRecordProcessor");
		}
	}

}
