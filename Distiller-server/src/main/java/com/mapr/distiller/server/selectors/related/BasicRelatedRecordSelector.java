package com.mapr.distiller.server.selectors.related;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

import java.util.Iterator;
import java.util.LinkedList;

public class BasicRelatedRecordSelector implements RelatedRecordSelector<Record, Record> {
	private static final int maxRelatedTimePeriods = 100;
	private class TimePeriod {
		long start;
		long end;
	}
	
	LinkedList<TimePeriod> relatedTimePeriods;
	String id;
	RecordQueue relatedQueue;
	RecordQueue outputQueue;
	String relationMethod;
	String relationKey;
	String relationValue;
	long window;
	private long inRecCntr, outRecCntr, putFailureCntr, otherFailureCntr;
	
	
	public BasicRelatedRecordSelector(
			String id,							//The string to be used when calling get/put against queues
			RecordQueue relatedQueue,			//e.g. MfsGuts-1s-Raw
			RecordQueue outputQueue,			//e.g. MfsGUts-1s-Raw-During-MfsThread-HighCpu
			String relationMethod,				//e.g. timeBasedWindow
			String relationKey,					//e.g. duration
			String relationValue				//e.g. 10000 (10s)
			) throws Exception
	{
		this.id = id;
		this.relatedQueue = relatedQueue;
		this.outputQueue = outputQueue;
		if(!relationMethod.equals(Constants.TIME_BASED_WINDOW))
			throw new Exception("Unknown method: " + relationMethod);
		this.relationMethod = relationMethod;		
		if(!relationKey.equals("duration"))
			throw new Exception("Unknown key: " + relationKey);
		this.relationKey = relationKey;
		this.relationValue = relationValue;
		this.window = Long.parseLong(relationValue);
		this.relatedTimePeriods = new LinkedList<TimePeriod>();
	}
	
	public long[] selectRelatedRecords(Record inputRecord) throws Exception
	{
		Record relatedRecord;
		if(relationMethod.equals(Constants.TIME_BASED_WINDOW)){
			//Update the list of TimePeriods we are interested in based on the timestamps in the new input record
			if(relationKey.equals("duration")){
				TimePeriod p = new TimePeriod();
				if(relatedTimePeriods.size()!=0 && inputRecord.getPreviousTimestamp() <= relatedTimePeriods.getLast().end){
					if(inputRecord.getTimestamp() < relatedTimePeriods.getLast().start)
						throw new Exception("End timestamp of current input record is less than start timestamp of previous input record, records must be in chronological order.");
					p = relatedTimePeriods.getLast();
					if(inputRecord.getPreviousTimestamp() < p.start)
						p.start = inputRecord.getPreviousTimestamp();
					if(inputRecord.getTimestamp() > p.end)
						p.end = inputRecord.getTimestamp();
					relatedTimePeriods.removeLast();
					relatedTimePeriods.addFirst(p);
				} else {
					if(inputRecord.getPreviousTimestamp()==-1)
						throw new Exception("Can not perform duration based window selection for raw input records.");
					p.start = inputRecord.getPreviousTimestamp() - window;
					p.end = inputRecord.getTimestamp() + window;
					relatedTimePeriods.add(p);
				}
			} else {
				throw new Exception("Unknown key " + relationKey + " for method " + Constants.TIME_BASED_WINDOW);
			}
			//Check the related queue against the new list of related TimePeriods to see if there are matching records to output
						
			while((relatedRecord = relatedQueue.peek(id, false)) != null){
				if (relatedRecord.getPreviousTimestamp()!=-1 && 
					relatedRecord.getPreviousTimestamp() > relatedTimePeriods.getLast().end){
						//The start timestamp of the oldest record in the related queue is newer than the end timestamp of the newest related TimePeriod, leave the Records in the related queue.
					break;
				} else if (relatedRecord.getPreviousTimestamp() != -1 &&
							relatedRecord.getPreviousTimestamp() <= relatedTimePeriods.getLast().end &&
							relatedRecord.getPreviousTimestamp() >= relatedTimePeriods.getFirst().start){
					boolean matchesWindow=false;
					Iterator<TimePeriod> i = relatedTimePeriods.iterator();
					while(i.hasNext()){
						TimePeriod p = i.next();
						if (relatedRecord.getPreviousTimestamp() <= p.end &&
							relatedRecord.getPreviousTimestamp() >= p.start ){
							matchesWindow=true;
							break;
						}
					}
					try {
						relatedRecord = relatedQueue.get(id);
						inRecCntr++;
						if(matchesWindow){
							try {
								outputQueue.put(id, relatedRecord);
								System.err.println("Related records: " + inputRecord.toString() + " ||||| " + relatedRecord.toString());
								outRecCntr++;
							} catch (Exception e) {
								putFailureCntr++;
							}
						}
					} catch (Exception e) {
						otherFailureCntr++;
						break;
					}
				} else if (relatedRecord.getTimestamp() > relatedTimePeriods.getLast().end){
					break;
				} else if (relatedRecord.getTimestamp() <= relatedTimePeriods.getLast().end &&
							relatedRecord.getTimestamp() >= relatedTimePeriods.getFirst().start ){
					boolean matchesWindow = false;
					Iterator<TimePeriod> i = relatedTimePeriods.iterator();
					while(i.hasNext()){
						TimePeriod p = i.next();
						if (relatedRecord.getTimestamp() <= p.end &&
							relatedRecord.getTimestamp() >= p.start ){
							matchesWindow=true;
							break;
						}
					}
					try {
						relatedRecord = relatedQueue.get(id);
						inRecCntr++;
						if(matchesWindow){
							try {
								outputQueue.put(id, relatedRecord);
								outRecCntr++;
							} catch (Exception e) {
								putFailureCntr++;
							}
						}
					} catch (Exception e) {
						otherFailureCntr++;
						break;
					}
				} else {
					try {
						relatedRecord = relatedQueue.get(id);
					} catch (Exception e){
						otherFailureCntr++;
						break;
					}
				}
			}
		} else {
			throw new Exception("Unknown method: " + relationMethod);
		}
		//Cleanup the list of TimePeriods if it exceeds the max
		if(relatedTimePeriods.size() >= maxRelatedTimePeriods){
			for(int x=0; x<(relatedTimePeriods.size() - maxRelatedTimePeriods); x++){
				relatedTimePeriods.removeFirst();
			}
		}
		return new long[]{inRecCntr, outRecCntr, putFailureCntr, otherFailureCntr};
	}
}
