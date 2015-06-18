package com.mapr.distiller.server.readers;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.InputStream;

public abstract class InputStreamRecordReader implements RecordReader {
	private InputStream rawInputStream = null;
	protected BufferedReader inputStream = null;
	protected String id;
	private boolean initialized = false;
	
	public InputStreamRecordReader() {
		
	}
	
	public InputStreamRecordReader(String id){
		this.id = id + ":RecRdr";
	}
	
	public boolean setInputStream(InputStream rawInputStream) {
		if(rawInputStream == null) {
			return false;
		} else {
			try {
				this.rawInputStream = rawInputStream;
				this.inputStream = new BufferedReader(new InputStreamReader(this.rawInputStream));
			} catch (Exception e) {
				System.err.println(id + ": Failed to initialize input stream");
				e.printStackTrace();
				return false;
			}
		}
		initialized = true;
		return true;
	}
}
