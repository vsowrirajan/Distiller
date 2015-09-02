package com.mapr.distiller.server.utils;

import java.util.Comparator;

import com.mapr.distiller.server.recordtypes.Record;

public class TimestampBasedRecordComparator implements Comparator<Record>{
	public int compare(Record r1, Record r2) {
		if(r1.getTimestamp() < r2.getTimestamp()){
			return -1;
		} else if (r2.getTimestamp() < r1.getTimestamp()){
			return 1;
		} else if (r1.getPreviousTimestamp() < r2.getPreviousTimestamp()){
			return -1;
		} else if (r2.getPreviousTimestamp() < r1.getPreviousTimestamp()){
			return 1;
		} else if (System.identityHashCode(r1) < System.identityHashCode(r2)){
			return -1;
		} else if (System.identityHashCode(r2) < System.identityHashCode(r1)){
			return 1;
		}
		return 0;
	}
}
