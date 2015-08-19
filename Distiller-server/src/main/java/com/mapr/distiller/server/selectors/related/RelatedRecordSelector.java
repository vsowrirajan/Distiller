package com.mapr.distiller.server.selectors.related;

import com.mapr.distiller.server.recordtypes.Record;

public interface RelatedRecordSelector<T1 extends Record, T2 extends Record>{
	public long[] selectRelatedRecords(Record inputRecord) throws Exception;
}
