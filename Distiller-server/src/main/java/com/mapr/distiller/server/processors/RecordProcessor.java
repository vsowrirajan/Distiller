package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;

public interface RecordProcessor<T extends Record> extends Thresholdable<T>,
		MovingAverageable<T> {

	public String getName();

}
