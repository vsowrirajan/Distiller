package com.mapr.distiller.server.processors;

import java.util.List;

import com.mapr.distiller.server.recordtypes.Record;

public interface MovingAverageable<T> {
	// Moving average for records in a window
	public Record movingAverage(List<T> records) throws Exception;

	// Moving average for records where it is monotonically increasing counters
	public Record movingAverage(T oldRecord, T newRecord) throws Exception;
}
