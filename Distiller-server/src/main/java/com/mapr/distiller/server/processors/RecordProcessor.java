package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;

public interface RecordProcessor {
	public Record process(Record record) throws Exception;
}
