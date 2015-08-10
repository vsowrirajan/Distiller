package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DiskstatRecord;
import com.mapr.distiller.server.recordtypes.Record;

public class DiskstatRecordProcessor implements RecordProcessor<Record> {

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
	public DiskstatRecord movingAverage(Record rec1, Record rec2)
			throws Exception {
		return new DiskstatRecord((DiskstatRecord) rec1, (DiskstatRecord) rec2);
	}

}
