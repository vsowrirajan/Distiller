package com.mapr.distiller.server.queues;



import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

public class UpdatingSubscriptionRecordQueue extends SubscriptionRecordQueue {
	String qualifierKey;
	
	public UpdatingSubscriptionRecordQueue(String id, int queueRecordCapacity, int queueTimeCapacity, String qualifierKey) throws Exception {
		super(id, queueRecordCapacity, queueTimeCapacity);
		if(qualifierKey == null || qualifierKey.equals(""))
			throw new Exception("qualifierKey can not be null/empty");
		this.qualifierKey = qualifierKey;
	}
	
	@Override
	public String getQueueQualifierKey(){
		return qualifierKey;
	}
	
	@Override
	public String getQueueType(){
		return Constants.UPDATING_SUBSCRIPTION_RECORD_QUEUE;
	}
	
	@Override
	public boolean put(String producerName, Record record) {
		try {
			if(super.update(producerName, record, qualifierKey))
				return true;
			else
				return super.put(producerName, record);
		} catch (Throwable e) {
			System.err.println( "UpdatingSubscriptionRecordQueue-" + System.identityHashCode(this) + ": Failed to put or update, producerName:" + producerName + 
								" qualifierKey:" + qualifierKey + " record:" + record.toString() );
			return false;
		}
	}
		
}
