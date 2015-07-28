package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DiskstatRecord;

public class DiskstatRecordProcessor implements 
		Thresholdable<DiskstatRecord>, MovingAverageable<DiskstatRecord> {
	
	public boolean isNotEqual(DiskstatRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}
	
	public boolean isEqual(DiskstatRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "device_name":
			return record.get_device_name().equals(thresholdValue);

		case "averageOperationsInProgress":
			if(record.getAverageOperationsInProgress() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getAverageOperationsInProgress() == Double.parseDouble(thresholdValue);
		
		case "averageServiceTime":
			if(record.getAverageServiceTime() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getAverageServiceTime() == Double.parseDouble(thresholdValue);
			
		case "averageWaitTime":
			if(record.getAverageWaitTime() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getAverageWaitTime() == Double.parseDouble(thresholdValue);
			
		case "diskReadOperationRate":
			if(record.getDiskReadOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getDiskReadOperationRate() == Double.parseDouble(thresholdValue);
			
		case "diskWriteOperationRate":
			if(record.getDiskWriteOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getDiskWriteOperationRate() == Double.parseDouble(thresholdValue);
			
		case "readByteRate":
			if(record.getReadByteRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getReadByteRate() == Double.parseDouble(thresholdValue);
			
		case "readOperationRate":
			if(record.getReadOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getReadOperationRate() == Double.parseDouble(thresholdValue);
			
		case "utilizationPct":
			if(record.getUtilizationPct() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getUtilizationPct() == Double.parseDouble(thresholdValue);
			
		case "writeByteRate":
			if(record.getWriteByteRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getWriteByteRate() == Double.parseDouble(thresholdValue);
			
		case "writeOperationRate":
			if(record.getWriteOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to value");
			else
				return record.getWriteOperationRate() == Double.parseDouble(thresholdValue);
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	public boolean isAboveThreshold(DiskstatRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "averageOperationsInProgress":
			if(record.getAverageOperationsInProgress() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getAverageOperationsInProgress() > Double.parseDouble(thresholdValue);
		
		case "averageServiceTime":
			if(record.getAverageServiceTime() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getAverageServiceTime() > Double.parseDouble(thresholdValue);
			
		case "averageWaitTime":
			if(record.getAverageWaitTime() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getAverageWaitTime() > Double.parseDouble(thresholdValue);
			
		case "diskReadOperationRate":
			if(record.getDiskReadOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getDiskReadOperationRate() > Double.parseDouble(thresholdValue);
			
		case "diskWriteOperationRate":
			if(record.getDiskWriteOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getDiskWriteOperationRate() > Double.parseDouble(thresholdValue);
			
		case "readByteRate":
			if(record.getReadByteRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getReadByteRate() > Double.parseDouble(thresholdValue);
			
		case "readOperationRate":
			if(record.getReadOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getReadOperationRate() > Double.parseDouble(thresholdValue);
			
		case "utilizationPct":
			if(record.getUtilizationPct() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getUtilizationPct() > Double.parseDouble(thresholdValue);
			
		case "writeByteRate":
			if(record.getWriteByteRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getWriteByteRate() > Double.parseDouble(thresholdValue);
			
		case "writeOperationRate":
			if(record.getWriteOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getWriteOperationRate() > Double.parseDouble(thresholdValue);
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(DiskstatRecord record, String metric,
			String thresholdValue) throws Exception {
		
		switch (metric) {
		case "averageOperationsInProgress":
			if(record.getAverageOperationsInProgress() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getAverageOperationsInProgress() < Double.parseDouble(thresholdValue);
		
		case "averageServiceTime":
			if(record.getAverageServiceTime() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getAverageServiceTime() < Double.parseDouble(thresholdValue);
			
		case "averageWaitTime":
			if(record.getAverageWaitTime() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getAverageWaitTime() < Double.parseDouble(thresholdValue);
			
		case "diskReadOperationRate":
			if(record.getDiskReadOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getDiskReadOperationRate() < Double.parseDouble(thresholdValue);
			
		case "diskWriteOperationRate":
			if(record.getDiskWriteOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getDiskWriteOperationRate() < Double.parseDouble(thresholdValue);
			
		case "readByteRate":
			if(record.getReadByteRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getReadByteRate() < Double.parseDouble(thresholdValue);
			
		case "readOperationRate":
			if(record.getReadOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getReadOperationRate() < Double.parseDouble(thresholdValue);
			
		case "utilizationPct":
			if(record.getUtilizationPct() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getUtilizationPct() < Double.parseDouble(thresholdValue);
			
		case "writeByteRate":
			if(record.getWriteByteRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getWriteByteRate() < Double.parseDouble(thresholdValue);
			
		case "writeOperationRate":
			if(record.getWriteOperationRate() == -1d)
				throw new Exception("Can not compare raw DiskstatRecord to threshold");
			else
				return record.getWriteOperationRate() < Double.parseDouble(thresholdValue);
		
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in DiskstatRecord");
		}
	}

	@Override
	public DiskstatRecord movingAverage(DiskstatRecord rec1,
			DiskstatRecord rec2) throws Exception{
		return new DiskstatRecord(rec1, rec2);
	}


}
