package com.mapr.distiller.server.recordtypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Record {
	
	private static final Logger LOG = LoggerFactory.getLogger(Record.class);

	private long timestamp, previousTimestamp;
	
	public String getValueForQualifier(String qualifier) throws Exception {
		throw new Exception("Qualifier " + qualifier + " is not valid for this record type");
	}
	
	public Record() {
		this.timestamp = -1l;
		this.previousTimestamp = -1l;	
	}
	
	public Record(Record r) {
		this.timestamp = r.getTimestamp();
		this.previousTimestamp = r.getPreviousTimestamp();
	}
	
	public Record(long timestamp, long previousTimestamp) {
		this.timestamp = timestamp;
		this.previousTimestamp = previousTimestamp;
	}

	public Record(long timestamp) {
		this.timestamp = timestamp;
		this.previousTimestamp = -1l;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) throws Exception{
		if(this.previousTimestamp > timestamp)
			throw new Exception("Invalid value for setTimestamp, timestamp:" + timestamp + 
					" previousTimestamp:" + previousTimestamp);
		this.timestamp = timestamp;
	}

	public long getPreviousTimestamp() {
		return previousTimestamp;
	}

	public void setPreviousTimestamp(long previousTimestamp) throws Exception {
		if(this.timestamp < previousTimestamp)
			throw new Exception("Invalid value for setPreviousTimestamp, timestamp:" + 
					this.timestamp + " previousTimestamp:" + previousTimestamp);
		this.previousTimestamp = previousTimestamp;
	}

	public long getDurationms() {
		if(timestamp==-1 || previousTimestamp==-1)
			return -1;
		return timestamp - previousTimestamp;
	}

	@Override
	public String toString() {
		if(previousTimestamp==-1 && timestamp!=-1)
			return "T:" + timestamp;
		else if (previousTimestamp!=-1 && timestamp!=-1)
			return "T:" + timestamp + " P:" + previousTimestamp + " D:" + getDurationms();
		return "";
	}
}
