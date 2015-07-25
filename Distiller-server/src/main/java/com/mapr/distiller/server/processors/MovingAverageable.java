package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;

public interface MovingAverageable<T> {
	public Record movingAverage(T oldRecord, T newRecord) throws Exception;
}
