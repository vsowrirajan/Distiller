package com.mapr.distiller.server.recordtypes;

public class SplitterRecord extends Record {
	private String type;
	private Record record;

	@Override
	public String toString() {
		return record.toString();
	}
}
