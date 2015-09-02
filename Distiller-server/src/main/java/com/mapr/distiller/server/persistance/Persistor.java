package com.mapr.distiller.server.persistance;

import com.mapr.distiller.server.recordtypes.Record;

public interface Persistor{
	public void persist(Record record) throws Exception;
}
