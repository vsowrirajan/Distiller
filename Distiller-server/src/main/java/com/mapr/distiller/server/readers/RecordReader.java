package com.mapr.distiller.server.readers;

import com.mapr.distiller.server.recordtypes.Record;

public interface RecordReader {

	public Record getRecord();

	public Record getRecord(String name);
}
