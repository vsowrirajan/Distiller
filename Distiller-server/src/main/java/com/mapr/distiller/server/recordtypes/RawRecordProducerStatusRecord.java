package com.mapr.distiller.server.recordtypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class RawRecordProducerStatusRecord extends Record {

	private String producerId;							//This should uniquely identify what generated this status record (e.g. an instance of ProcRecordProducer)
	private long 	recordsCreated,						//The number of Records created by the producer	
					recordCreationFailures,				//The number of times the producer tried to create a Record and failed
					queuePutFailures=0l,				//The number of calls to put a Record into a RecordQueue that did not return successfully
					otherFailures=0l,					//Number of any other types of failures that don't fit into the other buckets.
					runningTimems=0l;					//The amount of time the record producer was actively running (vs. waiting for input).  This is basically the CPU consumption of the record producer
	private HashMap<String,String> extraInfo=null;		//Extra info that a record producer can choose to include.

	public RawRecordProducerStatusRecord(String producerId){
		super(System.currentTimeMillis());
		this.producerId = producerId;
		recordsCreated=0l;
		recordCreationFailures=0l;
		runningTimems=0l;
	}

	public RawRecordProducerStatusRecord(RawRecordProducerStatusRecord oldRecord) throws Exception{
		super(System.currentTimeMillis());
		this.producerId = oldRecord.getProducerId();
		try {
			oldRecord.setPreviousTimestamp(oldRecord.getTimestamp());
			oldRecord.setTimestamp(this.getTimestamp());
		} catch (Exception e) {
			throw new Exception("Failed to set timestamps on old record: " + oldRecord.getPreviousTimestamp() + 
								" " + oldRecord.getTimestamp() + this.getTimestamp(), e);
		}
		recordsCreated=0l;
		recordCreationFailures=0l;
		runningTimems=0l;
	}

	@Override
	public String toString(){
		String cpuString="";
		if(this.getPreviousTimestamp()!=-1l)
			cpuString="\tCPU:" + (((double)runningTimems) / ((double)this.getDurationms()) * 100d) + "%";
		String eiString="";
		if(extraInfo!=null){
			Iterator<Map.Entry<String,String>> i = extraInfo.entrySet().iterator();
			while(i.hasNext()){
				Map.Entry<String,String> p = (Map.Entry<String,String>)i.next();
				//eiString=eiString + "\t" + p.getKey() + ":" + p.getValue();
				if(!p.getValue().equals("0")) eiString=eiString + "\t" + p.getKey() + ":" + p.getValue();
			}
		}
		return super.toString() + ":RPSRec:" + producerId + "\truntime:" + runningTimems +"\trecC:" + recordsCreated +  
				"\trFail:" +  recordCreationFailures + 
				"\tpFail:" + queuePutFailures + 
				cpuString + eiString;	
	}

	public String getProducerId(){
		return producerId;
	}

	public long getRecordsCreated(){
		return recordsCreated;
	}

	public long getRecordCreationFailures(){
		return recordCreationFailures;
	}

	public long getQueuePutFailures(){
		return queuePutFailures;
	}
	
	public long getOtherFailures(){
		return otherFailures;
	}

	public long getRunningTimems(){
		return runningTimems;
	}

	public void setRecordsCreated(long recordsCreated){
		this.recordsCreated = recordsCreated;
	}

	public void setRecordCreationFailures(long recordCreationFailures){
		this.recordCreationFailures = recordCreationFailures;
	}

	public void setQueuePutFailures(long queuePutFailures){
		this.queuePutFailures = queuePutFailures;
	}
	
	public void setOtherFailures(long otherFailures){
		this.otherFailures = otherFailures;
	}

	public void setRunningTimems(long runningTimems){
		this.runningTimems = runningTimems;
	}

	public String addExtraInfo(String key, String value){
		if(extraInfo == null){
			extraInfo = new HashMap<String,String>();
		}
		return extraInfo.put(key, value);
	}
}