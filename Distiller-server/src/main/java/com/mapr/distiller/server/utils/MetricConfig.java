package com.mapr.distiller.server.utils;

import java.util.Map;
import com.mapr.distiller.server.producers.raw.ProcRecordProducer;


public class MetricConfig {

	private final String id;
	private final String inputQueue;
	private final String outputQueue;
	private final int outputQueueRecordCapacity;
	private final int outputQueueTimeCapacity;
	private final int outputQueueMaxProducers;
	private final int periodicity;
	private final String recordType;
	private final String procRecordProducerMetricName;
	private final boolean rawProducerMetricsEnabled;
	private final String metricDescription;
	private String rawRecordProducerName;
	private final String selector;
	private final String processor;
	private final String method;
	private final String thresholdKey;
	private final String thresholdValue;
	private boolean initialized;
	private boolean metricActionStatusRecordsEnabled;
	private long metricActionStatusRecordFrequency;
	
	private MetricConfig(MetricConfigBuilder metricConfigBuilder) {
		// This can be randomly generated using java uuid()
		this.id = metricConfigBuilder.id;
		this.inputQueue = metricConfigBuilder.inputQueue;
		this.outputQueue = metricConfigBuilder.outputQueue;
		this.outputQueueRecordCapacity = metricConfigBuilder.outputQueueRecordCapacity;
		this.outputQueueTimeCapacity = metricConfigBuilder.outputQueueTimeCapacity;
		this.outputQueueMaxProducers = metricConfigBuilder.outputQueueMaxProducers;
		this.periodicity = metricConfigBuilder.periodicity;
		this.recordType = metricConfigBuilder.recordType;
		this.procRecordProducerMetricName = metricConfigBuilder.procRecordProducerMetricName;
		this.rawProducerMetricsEnabled = metricConfigBuilder.rawProducerMetricsEnabled;
		this.metricDescription = metricConfigBuilder.metricDescription;
		this.rawRecordProducerName = metricConfigBuilder.rawRecordProducerName;
		this.selector = metricConfigBuilder.selector;
		this.processor = metricConfigBuilder.processor;
		this.method = metricConfigBuilder.method;
		this.thresholdKey = metricConfigBuilder.thresholdKey;
		this.thresholdValue = metricConfigBuilder.thresholdValue;
		this.metricActionStatusRecordsEnabled = metricConfigBuilder.metricActionStatusRecordsEnabled;
		this.metricActionStatusRecordFrequency = metricConfigBuilder.metricActionStatusRecordFrequency;
		this.initialized = false;
	}
	
	@Override
	public String toString(){
		return  ((id==null || id.equals("")) ? "" : ("id:" + id)) + 
				((inputQueue==null || inputQueue.equals("")) ? "" : (" inputQueue:" + inputQueue)) + 
				((outputQueue==null || outputQueue.equals("")) ? "" : (" outputQueue:" + outputQueue)) + 
				" outputQueueRecordCapacity:" + outputQueueRecordCapacity + 
				" outputQueueTimeCapacity:" + outputQueueTimeCapacity + 
				" outputQueueMaxProducers:" + outputQueueMaxProducers + 
				" periodicity:" + periodicity + 
				((recordType==null || recordType.equals("")) ? "" : (" recordType:" + recordType)) + 
				((procRecordProducerMetricName==null || procRecordProducerMetricName.equals("")) ? "" : (" procRecordProducerMetricName:" + procRecordProducerMetricName)) + 
				" prodMetrics:" + rawProducerMetricsEnabled + 
				((metricDescription==null || metricDescription.equals("")) ? "" : (" metricDescription:" + metricDescription)) + 
				((rawRecordProducerName==null || rawRecordProducerName.equals("")) ? "" : (" rawRecordProducerName:" + rawRecordProducerName)) + 
				((selector==null || selector.equals("")) ? "" : (" selector:" + selector)) + 
				((processor==null || processor.equals("")) ? "" : (" processor:" + processor)) + 
				((method==null || method.equals("")) ? "" : (" method:" + method)) + 
				" metricActionStatusRecordsEnabled:" + metricActionStatusRecordsEnabled + 
				" metricActionStatusRecordFrequency:" + metricActionStatusRecordFrequency + 
				((thresholdKey==null || thresholdKey.equals("")) ? "" : (" thresholdKey:" + thresholdKey)) + 
				((thresholdValue==null || thresholdValue.equals("")) ? "" : (" thresholdValue:" + thresholdValue));

	}

	public String getId() {
		return id;
	}

	public String getInputQueue() {
		return inputQueue;
	}

	public String getOutputQueue() {
		return outputQueue;
	}

	public int getPeriodicity() {
		return periodicity;
	}
	public int getOutputQueueMaxProducers() {
		return outputQueueMaxProducers;
	}

	public int getOutputQueueRecordCapacity() {
		return outputQueueRecordCapacity;
	}

	public int getOutputQueueTimeCapacity() {
		return outputQueueTimeCapacity;
	}

	public String getRecordType() {
		return recordType;
	}

	public boolean getRawProducerMetricsEnabled(){
		return rawProducerMetricsEnabled;
	}

	// Builder pattern
	public static class MetricConfigBuilder {
		private final String id;
		private final String inputQueue;
		private final String outputQueue;
		private final int outputQueueRecordCapacity;
		private final int outputQueueTimeCapacity;
		private final int outputQueueMaxProducers;
		private final int periodicity;
		private final String recordType;
		private final String procRecordProducerMetricName;
		private boolean rawProducerMetricsEnabled;
		private final String metricDescription;
		private final String rawRecordProducerName;
		private final String selector;
		private final String processor;
		private final String method;
		private final String thresholdKey;
		private final String thresholdValue;
		private boolean metricActionStatusRecordsEnabled;
		private long metricActionStatusRecordFrequency;
		

		
		public MetricConfigBuilder(String id, String inputQueue, String outputQueue, int outputQueueRecordCapacity, 
				int outputQueueTimeCapacity, int outputQueueMaxProducers, int periodicity, String recordType, 
				String procRecordProducerMetricName, boolean rawProducerMetricsEnabled, String metricDescription,
				String rawRecordProducerName, String selector, String processor, String method, 
				boolean metricActionStatusRecordsEnabled, long metricActionStatusRecordFrequency,
				String thresholdKey, String thresholdValue) throws Exception{
			
			//Everything needs an id (e.g. "metric.name")
			if(id == null || id.equals(""))
				throw new IllegalArgumentException("Can not build a metric config using a null/empty metric.name");
			
			//Everything except the record producer stats needs a fully specified output queue
			if (!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
				if(outputQueue == null || outputQueue.equals(""))
					throw new IllegalArgumentException("Can not build a metric config using a null/empty output.queue.name");
				if (outputQueueRecordCapacity < 1)
					throw new IllegalArgumentException("Can not build a ProcRecordProducerMetric using output.queue.record.capacity < 1");
				if (outputQueueTimeCapacity < 1)
					throw new IllegalArgumentException("Can not build a ProcRecordProducerMetric using output.queue.time.capacity < 1");
			}
			
			//ProcRecordProducer metrics have specific requirements:
			if (recordType.equals(Constants.PROC_RECORD_PRODUCER_RECORD)) {
				if (procRecordProducerMetricName == null || !ProcRecordProducer.isValidMetricName(procRecordProducerMetricName))
					throw new IllegalArgumentException("Can not build a ProcRecordProducerMetric using a null/invalid proc.record.producer.metric.name");
				else if (periodicity < 1000) 
					throw new IllegalArgumentException("Can not build a ProcRecordProducerMetric using a periodicity < 1000");
			}
			//If it is not a raw RecordProducer metric (e.g. ProcRecordProducer or MfsGutsRecordProducer), then it must have an inputQueue, selector, processor and method
			else if (!recordType.equals(Constants.MFS_GUTS_RECORD_PRODUCER_RECORD) &&
					 !recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
			{
				if(inputQueue == null || inputQueue.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config using a null/empty input.queue.name");
				else if (selector == null || selector.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config using a null/empty input.record.selector");
				else if (processor == null || processor.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config using a null/empty input.record.processor.name");
				else if (method == null || method.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config using a null/empty input.record.processor.method");
			}
				
			
			this.id = id;
			this.inputQueue = inputQueue;
			this.outputQueue = outputQueue;
			this.outputQueueRecordCapacity = outputQueueRecordCapacity;
			this.outputQueueTimeCapacity = outputQueueTimeCapacity;
			this.outputQueueMaxProducers = outputQueueMaxProducers;
			this.periodicity = periodicity;
			this.recordType = recordType;
			this.procRecordProducerMetricName = procRecordProducerMetricName;
			this.rawRecordProducerName = rawRecordProducerName;
			this.rawProducerMetricsEnabled = rawProducerMetricsEnabled;
			this.metricDescription = metricDescription;
			this.selector = selector;
			this.processor = processor;
			this.method = method;
			this.thresholdKey = thresholdKey;
			this.thresholdValue = thresholdValue;
			this.metricActionStatusRecordFrequency = metricActionStatusRecordFrequency;
			this.metricActionStatusRecordsEnabled = metricActionStatusRecordsEnabled;
		}

		public MetricConfig build() {
			return new MetricConfig(this);
		}
	}
	
	public boolean getMetricActionStatusRecordsEnabled() {
		return metricActionStatusRecordsEnabled;
	}
	
	public long getMetricActionStatusRecordFrequency() {
		return metricActionStatusRecordFrequency;
	}

	public String getProcRecordProducerMetricName() {
		return procRecordProducerMetricName;
	}
	
	public String getMetricDescription() {
		return metricDescription;
	}
	
	public String getRawRecordProducerName() {
		return rawRecordProducerName;
	}
	
	public void setRawRecordProducerName(String rawRecordProducerName) {
		this.rawRecordProducerName = rawRecordProducerName;
	}
	
	public boolean isInitialized(){
		return initialized;
	}
	
	public void setInitialized(boolean initialized){
		this.initialized = initialized;
	}
	
	public String getSelector(){
		return selector;
	}
	
	public String getProcessor(){
		return processor;
	}
	
	public String getMethod(){
		return method;
	}
	
	public String getThresholdKey(){
		return thresholdKey;
	}
	
	public String getThresholdValue(){
		return thresholdValue;
	}
}
