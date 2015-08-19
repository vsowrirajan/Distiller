package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;

public interface Mergeable<T> {
	public Record merge(T oldRecord, T newRecord) throws Exception;
}
