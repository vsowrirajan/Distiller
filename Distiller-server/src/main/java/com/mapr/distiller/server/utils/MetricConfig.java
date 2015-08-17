package com.mapr.distiller.server.utils;

public class MetricConfig {

	private final String id;
	private final String inputQueue;
	private final String outputQueue;
	private final int outputQueueRecordCapacity;
	private final int outputQueueTimeCapacity;
	private final int outputQueueMaxProducers;
	private final int relatedOutputQueueRecordCapacity;
	private final int relatedOutputQueueTimeCapacity;
	private final int relatedOutputQueueMaxProducers;
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
	private String selectorQualifierValue = null;
	private String relatedInputQueueName = null;
	private String relatedOutputQueueName = null;
	private String relatedSelectorName = null;
	private String relatedSelectorMethod = null;
	private String updatingSubscriptionQueueKey = null;
	private boolean relatedSelectorEnabled;

	
	private MetricConfig(MetricConfigBuilder metricConfigBuilder) {
		this.id = metricConfigBuilder.id;
		this.inputQueue = metricConfigBuilder.inputQueue;
		this.outputQueue = metricConfigBuilder.outputQueue;
		this.outputQueueRecordCapacity = metricConfigBuilder.outputQueueRecordCapacity;
		this.outputQueueTimeCapacity = metricConfigBuilder.outputQueueTimeCapacity;
		this.outputQueueMaxProducers = metricConfigBuilder.outputQueueMaxProducers;
		this.relatedOutputQueueRecordCapacity = metricConfigBuilder.relatedOutputQueueRecordCapacity;
		this.relatedOutputQueueTimeCapacity = metricConfigBuilder.relatedOutputQueueTimeCapacity;
		this.relatedOutputQueueMaxProducers = metricConfigBuilder.relatedOutputQueueMaxProducers;
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
		this.selectorQualifierValue = metricConfigBuilder.selectorQualifierValue;
		this.relatedInputQueueName = metricConfigBuilder.relatedInputQueueName;
		this.relatedOutputQueueName = metricConfigBuilder.relatedOutputQueueName;
		this.relatedSelectorName = metricConfigBuilder.relatedSelectorName;
		this.relatedSelectorMethod = metricConfigBuilder.relatedSelectorMethod;
		this.updatingSubscriptionQueueKey = metricConfigBuilder.updatingSubscriptionQueueKey;
		this.relatedSelectorEnabled = metricConfigBuilder.relatedSelectorEnabled;
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
				" relatedOutputQueueRecordCapacity:" + relatedOutputQueueRecordCapacity + 
				" relatedOutputQueueTimeCapacity:" + relatedOutputQueueTimeCapacity + 
				" relatedOutputQueueMaxProducers:" + relatedOutputQueueMaxProducers + 
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
				" outputQueueType:" + ((outputQueueType==null) ? "null" : outputQueueType) + 
				" selectorQualifierValue:" + ((selectorQualifierValue==null) ? "null" : selectorQualifierValue) + 
				" relatedInputQueueName:" + ((relatedInputQueueName==null) ? "null" : relatedInputQueueName) + 
				" relatedOutputQueueName:" + ((relatedOutputQueueName==null) ? "null" : relatedOutputQueueName) + 
				" relatedSelectorName:" + ((relatedSelectorName==null) ? "null" : relatedSelectorName) + 
				" relatedSelectorMethod:" + ((relatedSelectorMethod==null) ? "null" : relatedSelectorMethod) + 
				" updatingSubscriptionQueueKey:" + ((updatingSubscriptionQueueKey==null) ? "null" : updatingSubscriptionQueueKey) +
				" relatedSelectorEnabled:" + relatedSelectorEnabled
				);
	}
	
	public boolean getRelatedSelectorEnabled() {
		return relatedSelectorEnabled;
	}
	
	public String getUpdatingSubscriptionQueueKey(){
		return updatingSubscriptionQueueKey;
	}
	public String getSelectorQualifierValue() {
		return selectorQualifierValue;
	}
	
	public String getRelatedInputQueueName() {
		return relatedInputQueueName;
	}
	
	public String getRelatedOutputQueueName() {
		return relatedOutputQueueName;
	}
	
	public String getRelatedSelectorName() {
		return relatedSelectorName;
	}
	
	public String getRelatedSelectorMethod() {
		return relatedSelectorMethod;
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

	public int getRelatedOutputQueueMaxProducers() {
		return relatedOutputQueueMaxProducers;
	}

	public int getRelatedOutputQueueRecordCapacity() {
		return relatedOutputQueueRecordCapacity;
	}

	public int getRelatedOutputQueueTimeCapacity() {
		return relatedOutputQueueTimeCapacity;
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
		private final int relatedOutputQueueRecordCapacity;
		private final int relatedOutputQueueTimeCapacity;
		private final int relatedOutputQueueMaxProducers;
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
		private String selectorQualifierValue = null;
		private String relatedInputQueueName = null;
		private String relatedOutputQueueName = null;
		private String relatedSelectorName = null;
		private String relatedSelectorMethod = null;
		private String updatingSubscriptionQueueKey = null;
		private boolean relatedSelectorEnabled = false;

		

		
		public MetricConfigBuilder(String id, String inputQueue, String outputQueue, int outputQueueRecordCapacity, 
				int outputQueueTimeCapacity, int outputQueueMaxProducers, int periodicity, String recordType, 
				String procRecordProducerMetricName, boolean rawProducerMetricsEnabled, String metricDescription,
				String rawRecordProducerName, String selector, String processor, String method, 
				boolean metricActionStatusRecordsEnabled, long metricActionStatusRecordFrequency,
				String thresholdKey, String thresholdValue, long timeSelectorMaxDelta, long timeSelectorMinDelta,
				String selectorQualifierKey, long cumulativeSelectorFlushTime, String outputQueueType,
				String selectorQualifierValue, String relatedInputQueueName,
				String relatedSelectorName, String relatedSelectorMethod,
				String updatingSubscriptionQueueKey, String relatedOutputQueueName, 
				int relatedOutputQueueRecordCapacity, int relatedOutputQueueTimeCapacity, 
				int relatedOutputQueueMaxProducers, boolean relatedSelectorEnabled) throws Exception{
			
			this.id = id;
			this.inputQueue = inputQueue;
			this.outputQueue = outputQueue;
			this.outputQueueRecordCapacity = outputQueueRecordCapacity;
			this.outputQueueTimeCapacity = outputQueueTimeCapacity;
			this.outputQueueMaxProducers = outputQueueMaxProducers;
			this.relatedOutputQueueRecordCapacity = relatedOutputQueueRecordCapacity;
			this.relatedOutputQueueTimeCapacity = relatedOutputQueueTimeCapacity;
			this.relatedOutputQueueMaxProducers = relatedOutputQueueMaxProducers;
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
			this.selectorQualifierValue = selectorQualifierValue;
			this.relatedInputQueueName = relatedInputQueueName;
			this.relatedOutputQueueName = relatedOutputQueueName;
			this.relatedSelectorName = relatedSelectorName;
			this.relatedSelectorMethod = relatedSelectorMethod;
			this.updatingSubscriptionQueueKey = updatingSubscriptionQueueKey;
			this.relatedSelectorEnabled = relatedSelectorEnabled;
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
