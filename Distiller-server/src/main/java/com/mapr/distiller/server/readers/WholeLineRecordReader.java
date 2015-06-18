package com.mapr.distiller.server.readers;

import com.mapr.distiller.server.recordtypes.WholeLineRecord;

public class WholeLineRecordReader extends InputStreamRecordReader {

	public WholeLineRecordReader(String id){
		this.id = id + ":WLRR";
	}
	
	public WholeLineRecord getRecord() {
		try {
			String line = inputStream.readLine();
			if (line != null)
				return new WholeLineRecord(line);
		} catch (Exception e) {
			System.err.println(id + ": Failed to readLine from inputStream");
			e.printStackTrace();
		}
		return null;
	}
	
	public WholeLineRecord getRecord(String name) {
		return getRecord();
	}
}
