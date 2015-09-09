package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;

public interface Convertable<T> {
	public Record convert(T record) throws Exception;
}
