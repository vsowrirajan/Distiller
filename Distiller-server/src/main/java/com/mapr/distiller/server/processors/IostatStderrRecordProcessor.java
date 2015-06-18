package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.WholeLineRecord;

public class IostatStderrRecordProcessor implements RecordProcessor {

	IostatStderrRecordProcessor() {
	};

	public Record process(Record record) throws Exception {
		// Never return a record, if iostat prints to stderr then something is
		// wrong
		// Throw an exception, which will cause the parent RecordProducer thread
		// to exit
		// IostatMonitor will see the RecordProducer thread has exited and will
		// reinit
		throw new Exception();
	}

	public Record process(WholeLineRecord inputRecord) throws Exception {
		throw new Exception();
	}
}
