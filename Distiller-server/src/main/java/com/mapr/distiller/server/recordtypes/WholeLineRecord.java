package com.mapr.distiller.server.recordtypes;

public class WholeLineRecord extends Record {
	private String line;

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	public WholeLineRecord(String line) {
		this.line = line;
	}

	@Override
	public String toString() {
		return super.toString() + " WholeLine:" + line;
	}
}
