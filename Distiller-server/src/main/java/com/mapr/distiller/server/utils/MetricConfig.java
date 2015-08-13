package com.mapr.distiller.server.utils;

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
	private long timeSelectorMaxDelta;
	private long timeSelectorMinDelta;
	private String selectorQualifierKey;
	private long cumulativeSelectorFlushTime;
	private String outputQueueType;
	
	private MetricConfig(MetricConfigBuilder metricConfigBuilder) {
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
		this.timeSelectorMaxDelta = metricConfigBuilder.timeSelectorMaxDelta;
		this.timeSelectorMinDelta = metricConfigBuilder.timeSelectorMinDelta;
		this.selectorQualifierKey = metricConfigBuilder.selectorQualifierKey;
		this.cumulativeSelectorFlushTime = metricConfigBuilder.cumulativeSelectorFlushTime;
		this.outputQueueType = metricConfigBuilder.outputQueueType;
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
				" cumulativeSelectorFlushTime:" + cumulativeSelectorFlushTime  +
				" metricActionStatusRecordsEnabled:" + metricActionStatusRecordsEnabled + 
				" metricActionStatusRecordFrequency:" + metricActionStatusRecordFrequency + 
				((thresholdKey==null || thresholdKey.equals("")) ? "" : (" thresholdKey:" + thresholdKey)) + 
				((thresholdValue==null || thresholdValue.equals("")) ? "" : (" thresholdValue:" + thresholdValue) + 
				" timeSelectorMinDelta:" + timeSelectorMinDelta + 
				" timeSelectorMaxDelta:" + timeSelectorMaxDelta +
				" selectorQualifierKey:" + ((selectorQualifierKey==null) ? "null" : selectorQualifierKey) + 
				" outputQueueType:" + ((outputQueueType==null) ? "null" : outputQueueType)
				);

	}
	
	public String getOutputQueueType(){
		return outputQueueType;
	}
	
	public long getCumulativeSelectorFlushTime() {
		return cumulativeSelectorFlushTime;
	}
	
	public String getSelectorQualifierKey(){
		return selectorQualifierKey;
	}
	
	public long getTimeSelectorMinDelta(){
		return timeSelectorMinDelta;
	}

	public long getTimeSelectorMaxDelta(){
		return timeSelectorMaxDelta;
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
		private long timeSelectorMinDelta;
		private long timeSelectorMaxDelta;
		private String selectorQualifierKey;
		private long cumulativeSelectorFlushTime;
		private String outputQueueType;
		

		
		public MetricConfigBuilder(String id, String inputQueue, String outputQueue, int outputQueueRecordCapacity, 
				int outputQueueTimeCapacity, int outputQueueMaxProducers, int periodicity, String recordType, 
				String procRecordProducerMetricName, boolean rawProducerMetricsEnabled, String metricDescription,
				String rawRecordProducerName, String selector, String processor, String method, 
				boolean metricActionStatusRecordsEnabled, long metricActionStatusRecordFrequency,
				String thresholdKey, String thresholdValue, long timeSelectorMaxDelta, long timeSelectorMinDelta,
				String selectorQualifierKey, long cumulativeSelectorFlushTime, String outputQueueType) throws Exception{
			
			//Everything needs an id (e.g. "metric.name")
			if(id == null || id.equals(""))
				throw new IllegalArgumentException("Can not build a metric config using a null/empty " + Constants.METRIC_NAME);
			
			//Everything except the record producer stats needs a fully specified output queue
			if (!recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD)){
				if(outputQueue == null || outputQueue.equals(""))
					throw new IllegalArgumentException("Can not build a metric config for " + id + " using a null/empty " + Constants.OUTPUT_QUEUE_NAME);
				if (outputQueueRecordCapacity < 1)
					throw new IllegalArgumentException(	"Can not build a metric config for " + id + " using " + Constants.OUTPUT_QUEUE_CAPACITY_RECORDS + 
														" < 1, value: " + outputQueueRecordCapacity);
				if (outputQueueTimeCapacity < 1)
					throw new IllegalArgumentException( "Can not build a metric config for " + id + " using " + Constants.OUTPUT_QUEUE_CAPACITY_SECONDS + 
														" < 1, value: " + outputQueueTimeCapacity);
			}
			
			//Everything needs a recordType
			if(recordType == null || recordType.equals(""))
				throw new IllegalArgumentException("Can not build a metric config for " + id + " using a null/empty " + Constants.RECORD_TYPE);
			
			//ProcRecordProducer metrics have specific requirements:
			if (recordType.equals(Constants.PROC_RECORD_PRODUCER_RECORD)) {
				if (procRecordProducerMetricName == null || !ProcRecordProducer.isValidMetricName(procRecordProducerMetricName))
					throw new IllegalArgumentException("Can not build a ProcRecordProducerMetric for " + id + " using a null/invalid " + Constants.PROC_RECORD_PRODUCER_METRIC_NAME);
				else if (periodicity < 1000) 
					throw new IllegalArgumentException("Can not build a ProcRecordProducerMetric for " + id + " using " + Constants.PERIODICITY_MS + " < 1000, value: " + periodicity);
			}
			//If it is not a raw RecordProducer metric (e.g. ProcRecordProducer or MfsGutsRecordProducer), then it must have an inputQueue, selector, processor and method
			else if (!recordType.equals(Constants.MFS_GUTS_RECORD_PRODUCER_RECORD) &&
					 !recordType.equals(Constants.RAW_RECORD_PRODUCER_STAT_RECORD))
			{
				if(inputQueue == null || inputQueue.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config for " + id + " using a null/empty " + Constants.INPUT_QUEUE_NAME);
				else if (selector == null || selector.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config for " + id + " using a null/empty " + Constants.INPUT_RECORD_SELECTOR);
				else if (processor == null || processor.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config for " + id + " using a null/empty " + Constants.INPUT_RECORD_PROCESSOR_NAME);
				else if (method == null || method.equals(""))
					throw new IllegalArgumentException("Can not build a non-raw metric config for " + id + " using a null/empty " + Constants.INPUT_RECORD_PROCESSOR_METHOD);
			}
			else if (selector!=null && selector.equals(Constants.TIME_SELECTOR)){
				if(timeSelectorMinDelta < 1000)
					throw new Exception("Use of " + Constants.INPUT_RECORD_SELECTOR + "=" + Constants.TIME_SELECTOR + 
										" requires " + Constants.TIME_SELECTOR_MIN_DELTA + " >= 1000, provided value: " + 
										timeSelectorMinDelta);
				if(timeSelectorMaxDelta != -1 && timeSelectorMaxDelta < timeSelectorMinDelta)
					throw new Exception(Constants.TIME_SELECTOR_MAX_DELTA + " can not be less than " + Constants.TIME_SELECTOR_MIN_DELTA);
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
			this.timeSelectorMaxDelta = timeSelectorMaxDelta;
			this.timeSelectorMinDelta = timeSelectorMinDelta;
			this.selectorQualifierKey = selectorQualifierKey;
			this.cumulativeSelectorFlushTime = cumulativeSelectorFlushTime;
			this.outputQueueType = outputQueueType;
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
