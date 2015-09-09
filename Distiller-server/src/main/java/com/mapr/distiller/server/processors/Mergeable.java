package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;

public interface Mergeable<T> {
	//This method should never return null.  It should either return a properly constructed Record or throw an exception
	public Record merge(T oldRecord, T newRecord) throws Exception;
	
	//This method should return 2 records.
	//When oldRecord and newRecord are chronologically consecutive:
	//	- the first Record returned should be a Record representing the merge of oldRecord and newRecord
	//	- the first Record should be passed back to the next call to mergeChronologicallyConsecutive as "oldRecord"
	//	- the second Record returned should be null
	//
	//When oldRecord and newRecord are NOT chronologically consecutive:
	//	- the first Record returned should be equal to newRecord
	//	- the second Record returned should be equal to oldRecord
	//	- the second Record represents a unique period of time for which chronologically consecutive Records were found
	public Record[] mergeChronologicallyConsecutive(T oldRecord, T newRecord) throws Exception;
}
