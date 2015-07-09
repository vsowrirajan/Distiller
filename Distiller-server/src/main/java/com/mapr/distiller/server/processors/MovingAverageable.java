package com.mapr.distiller.server.processors;

import java.util.List;

import com.mapr.distiller.server.recordtypes.Record;

public interface MovingAverageable<T> {
	public Record movingAverage(List<T> records);
}
