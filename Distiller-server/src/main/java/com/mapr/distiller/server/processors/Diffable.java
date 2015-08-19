package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;

public interface Diffable<T> {
	public Record diff(T oldRecord, T newRecord, String metricName) throws Exception;
}
