package com.mapr.distiller.server.recordtypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricActionStatusRecord extends Record {

	private static final Logger LOG = LoggerFactory
			.getLogger(MetricActionStatusRecord.class);

	private String producerId; // This should uniquely identify what generated
								// this status record (e.g. an instance of
								// ProcRecordProducer)
	private long inputRecords, // The number of Records taken from the input
								// queue
			outputRecords, // The number of Records put to the output queue
			processingFailures, // The number of calls to the RecordProcessor
								// method that did not return successfully
			queuePutFailures, // The number of calls to put a Record into the
								// output RecordQueue that did not return
								// successfully
			otherFailures, // Number of any other types of failures that don't
							// fit into the other buckets.
			runningTimems, // The amount of time the record producer was
							// actively running (vs. waiting for input). This is
							// basically the CPU consumption of the record
							// producer
			startTime; // Time at which the RecordProcessor started

	public MetricActionStatusRecord(String producerId, long inputRecords,
			long outputRecords, long processingFailures, long queuePutFailures,
			long otherFailures, long runningTimems, long startTime) {
		super(System.currentTimeMillis());
		this.producerId = producerId;
		this.inputRecords = inputRecords;
		this.outputRecords = outputRecords;
		this.processingFailures = processingFailures;
		this.queuePutFailures = queuePutFailures;
		this.otherFailures = otherFailures;
		this.runningTimems = runningTimems;
		this.startTime = startTime;
	}

	@Override
	public String toString() {
		return super.toString()
				+ ":RPSRec:"
				+ producerId
				+ "\truntime:"
				+ runningTimems
				+ "\truntime%: "
				+ ((double) runningTimems / (double) (System
						.currentTimeMillis() - startTime)) + "\tinRec:"
				+ inputRecords + "\toutRec:" + outputRecords + "\tpFail:"
				+ processingFailures + "\tqFail:" + queuePutFailures
				+ "\toFail:" + otherFailures;
	}

	public String getProducerId() {
		return producerId;
	}

	public long getRecordsRead() {
		return inputRecords;
	}

	public long getProcessingFailures() {
		return processingFailures;
	}

	public long getQueuePutFailures() {
		return queuePutFailures;
	}

	public long getOtherFailures() {
		return otherFailures;
	}

	public long getRunningTimems() {
		return runningTimems;
	}

	public void setRecordsRead(long inputRecords) {
		this.inputRecords = inputRecords;
	}

	public void setProcessingFailures(long processingFailures) {
		this.processingFailures = processingFailures;
	}

	public void setQueuePutFailures(long queuePutFailures) {
		this.queuePutFailures = queuePutFailures;
	}

	public void setOtherFailures(long otherFailures) {
		this.otherFailures = otherFailures;
	}

	public void setRunningTimems(long runningTimems) {
		this.runningTimems = runningTimems;
	}
}