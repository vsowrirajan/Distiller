package com.mapr.distiller.server.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.DiskstatRecord;
import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;
import com.mapr.distiller.server.recordtypes.Record;

public class DiskstatRecordProcessor implements RecordProcessor<Record> {

	private static final Logger LOG = LoggerFactory
			.getLogger(DiskstatRecordProcessor.class);

	public String getName() {
		return "DiskstatRecordProcessor";
	}

	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		DiskstatRecord currentRecord = (DiskstatRecord) record;

		switch (metric) {
		case "device_name":
			return currentRecord.get_device_name().equals(thresholdValue);

		case "averageOperationsInProgress":
			if (currentRecord.getAverageOperationsInProgress() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getAverageOperationsInProgress() == Double
						.parseDouble(thresholdValue);

		case "averageServiceTime":
			if (currentRecord.getAverageServiceTime() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getAverageServiceTime() == Double
						.parseDouble(thresholdValue);

		case "averageWaitTime":
			if (currentRecord.getAverageWaitTime() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getAverageWaitTime() == Double
						.parseDouble(thresholdValue);

		case "diskReadOperationRate":
			if (currentRecord.getDiskReadOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getDiskReadOperationRate() == Double
						.parseDouble(thresholdValue);

		case "diskWriteOperationRate":
			if (currentRecord.getDiskWriteOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getDiskWriteOperationRate() == Double
						.parseDouble(thresholdValue);

		case "readByteRate":
			if (currentRecord.getReadByteRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getReadByteRate() == Double
						.parseDouble(thresholdValue);

		case "readOperationRate":
			if (currentRecord.getReadOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getReadOperationRate() == Double
						.parseDouble(thresholdValue);

		case "utilizationPct":
			if (currentRecord.getUtilizationPct() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getUtilizationPct() == Double
						.parseDouble(thresholdValue);

		case "writeByteRate":
			if (currentRecord.getWriteByteRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getWriteByteRate() == Double
						.parseDouble(thresholdValue);

		case "writeOperationRate":
			if (currentRecord.getWriteOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to value");
			else
				return currentRecord.getWriteOperationRate() == Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		DiskstatRecord currentRecord = (DiskstatRecord) record;

		switch (metric) {
		case "averageOperationsInProgress":
			if (currentRecord.getAverageOperationsInProgress() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getAverageOperationsInProgress() > Double
						.parseDouble(thresholdValue);

		case "averageServiceTime":
			if (currentRecord.getAverageServiceTime() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getAverageServiceTime() > Double
						.parseDouble(thresholdValue);

		case "averageWaitTime":
			if (currentRecord.getAverageWaitTime() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getAverageWaitTime() > Double
						.parseDouble(thresholdValue);

		case "diskReadOperationRate":
			if (currentRecord.getDiskReadOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getDiskReadOperationRate() > Double
						.parseDouble(thresholdValue);

		case "diskWriteOperationRate":
			if (currentRecord.getDiskWriteOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getDiskWriteOperationRate() > Double
						.parseDouble(thresholdValue);

		case "readByteRate":
			if (currentRecord.getReadByteRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getReadByteRate() > Double
						.parseDouble(thresholdValue);

		case "readOperationRate":
			if (currentRecord.getReadOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getReadOperationRate() > Double
						.parseDouble(thresholdValue);

		case "utilizationPct":
			if (currentRecord.getUtilizationPct() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getUtilizationPct() > Double
						.parseDouble(thresholdValue);

		case "writeByteRate":
			if (currentRecord.getWriteByteRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getWriteByteRate() > Double
						.parseDouble(thresholdValue);

		case "writeOperationRate":
			if (currentRecord.getWriteOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getWriteOperationRate() > Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		DiskstatRecord currentRecord = (DiskstatRecord) record;

		switch (metric) {
		case "averageOperationsInProgress":
			if (currentRecord.getAverageOperationsInProgress() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getAverageOperationsInProgress() < Double
						.parseDouble(thresholdValue);

		case "averageServiceTime":
			if (currentRecord.getAverageServiceTime() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getAverageServiceTime() < Double
						.parseDouble(thresholdValue);

		case "averageWaitTime":
			if (currentRecord.getAverageWaitTime() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getAverageWaitTime() < Double
						.parseDouble(thresholdValue);

		case "diskReadOperationRate":
			if (currentRecord.getDiskReadOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getDiskReadOperationRate() < Double
						.parseDouble(thresholdValue);

		case "diskWriteOperationRate":
			if (currentRecord.getDiskWriteOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getDiskWriteOperationRate() < Double
						.parseDouble(thresholdValue);

		case "readByteRate":
			if (currentRecord.getReadByteRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getReadByteRate() < Double
						.parseDouble(thresholdValue);

		case "readOperationRate":
			if (currentRecord.getReadOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getReadOperationRate() < Double
						.parseDouble(thresholdValue);

		case "utilizationPct":
			if (currentRecord.getUtilizationPct() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getUtilizationPct() < Double
						.parseDouble(thresholdValue);

		case "writeByteRate":
			if (currentRecord.getWriteByteRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getWriteByteRate() < Double
						.parseDouble(thresholdValue);

		case "writeOperationRate":
			if (currentRecord.getWriteOperationRate() == -1d)
				throw new Exception(
						"Can not compare raw DiskstatRecord to threshold");
			else
				return currentRecord.getWriteOperationRate() < Double
						.parseDouble(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	@Override
	public DiskstatRecord merge(Record rec1, Record rec2) throws Exception {
		return new DiskstatRecord((DiskstatRecord) rec1, (DiskstatRecord) rec2);
	}

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric)
			throws Exception {
		if (rec1.getPreviousTimestamp() == -1l
				|| rec2.getPreviousTimestamp() == -1l)
			throw new Exception(
					"DiskstatRecords can only be diff'd from non-raw DiskstatRecords");

		DiskstatRecord oldRecord, newRecord;
		if (rec1.getTimestamp() < rec2.getTimestamp()) {
			oldRecord = (DiskstatRecord) rec1;
			newRecord = (DiskstatRecord) rec2;
		} else {
			oldRecord = (DiskstatRecord) rec2;
			newRecord = (DiskstatRecord) rec1;
		}

		if (oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception(
					"Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");
		if (oldRecord.get_major_number() != newRecord.get_major_number()
				|| oldRecord.get_minor_number() != newRecord.get_minor_number()
				|| oldRecord.get_hw_sector_size() != newRecord
						.get_hw_sector_size()
				|| !oldRecord.get_device_name().equals(
						newRecord.get_device_name()))
			throw new Exception(
					"Can not diff from DiskstatRecords form different devices");

		switch (metric) {
		case "averageOperationsInProgress":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getAverageOperationsInProgress()
							- oldRecord.getAverageOperationsInProgress());

		case "averageServiceTime":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getAverageServiceTime()
							- oldRecord.getAverageServiceTime());

		case "averageWaitTime":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getAverageWaitTime()
							- oldRecord.getAverageWaitTime());

		case "diskReadOperationRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getDiskReadOperationRate()
							- oldRecord.getDiskReadOperationRate());

		case "diskWriteOperationRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getDiskWriteOperationRate()
							- oldRecord.getDiskWriteOperationRate());

		case "readByteRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getReadByteRate()
							- oldRecord.getReadByteRate());

		case "readOperationRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getReadOperationRate()
							- oldRecord.getReadOperationRate());

		case "utilizationPct":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getUtilizationPct()
							- oldRecord.getUtilizationPct());

		case "writeByteRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double", newRecord.getWriteByteRate()
							- oldRecord.getWriteByteRate());

		case "writeOperationRate":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "double",
					newRecord.getWriteOperationRate()
							- oldRecord.getWriteOperationRate());

		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable in DiskstatRecordProcessor");
		}
	}

}