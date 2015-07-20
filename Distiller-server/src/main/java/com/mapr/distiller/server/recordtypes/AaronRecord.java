package com.mapr.distiller.server.recordtypes;

public class SplitterRecord extends Record {
	private String type;
	private Record record;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Record getRecord() {
		return record;
	}

	public void setRecord(Record record) {
		this.record = record;
	}

	@Override
	public String toString() {
		return record.toString();
	}
}
somenewfile
