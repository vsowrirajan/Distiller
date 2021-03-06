package com.mapr.distiller.server.producers.raw;

import java.util.Collections;
import java.util.TreeSet;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.io.File;
import java.net.NetworkInterface;
import java.lang.Integer;
import java.io.FilenameFilter;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.DiskstatRecord;
import com.mapr.distiller.server.recordtypes.LoadAverageRecord;
import com.mapr.distiller.server.recordtypes.NetworkInterfaceRecord;
import com.mapr.distiller.server.recordtypes.ProcessResourceRecord;
import com.mapr.distiller.server.recordtypes.SlimProcessResourceRecord;
import com.mapr.distiller.server.recordtypes.SystemCpuRecord;
import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;
import com.mapr.distiller.server.recordtypes.TcpConnectionStatRecord;
import com.mapr.distiller.server.recordtypes.ThreadResourceRecord;
import com.mapr.distiller.server.recordtypes.SlimThreadResourceRecord;
import com.mapr.distiller.server.scheduler.GatherMetricEvent;
import com.mapr.distiller.server.scheduler.MetricEventComparator;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.datatypes.ProcMetricDescriptorManager;
import com.mapr.distiller.server.recordtypes.RawRecordProducerStatusRecord;

public class ProcRecordProducer extends Thread {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(ProcRecordProducer.class);
	
	//Counters for how many times a raw metric was not successfully samples
	int diskstatRecordCreationFailures,
		networkInterfaceRecordCreationFailures,
		processResourceRecordCreationFailures,
		slimProcessResourceRecordCreationFailures,
		systemCpuRecordCreationFailures,
		systemMemoryRecordCreationFailures,
		tcpConnectionStatRecordCreationFailures,
		threadResourceRecordCreationFailures,
		slimThreadResourceRecordCreationFailures,
		loadAverageRecordCreationFailures;
	
	//Counters for how many times a raw metric Record could not be put to the output RecordQueue
	int diskstatRecordPutFailures,
		networkInterfaceRecordPutFailures,
		processResourceRecordPutFailures,
		slimProcessResourceRecordPutFailures,
		systemCpuRecordPutFailures,
		systemMemoryRecordPutFailures,
		tcpConnectionStatRecordPutFailures,
		threadResourceRecordPutFailures,
		slimThreadResourceRecordPutFailures,
		loadAverageRecordPutFailures;
	
	//Calls to produceRecord methods of each Record type that did not complete successfully.
	int diskstatRecordProduceRecordFailures,
		networkInterfaceRecordProduceRecordFailures,
		processResourceRecordProduceRecordFailures,
		slimProcessResourceRecordProduceRecordFailures,
		systemCpuRecordProduceRecordFailures,
		systemMemoryRecordProduceRecordFailures,
		tcpConnectionStatRecordProduceRecordFailures,
		threadResourceRecordProduceRecordFailures,
		slimThreadResourceRecordProduceRecordFailures,
		loadAverageRecordProduceRecordFailures;
	
	//Counters for how many times a raw metric was successfully sampled
	int diskstatRecordsCreated,
		networkInterfaceRecordsCreated,
		processResourceRecordsCreated,
		slimProcessResourceRecordsCreated,
		systemCpuRecordsCreated,
		systemMemoryRecordsCreated,
		tcpConnectionStatRecordsCreated,
		threadResourceRecordsCreated,
		slimThreadResourceRecordsCreated,
		loadAverageRecordsCreated;
	
	//Counters for time spent collecting each type of raw metric
	int diskstatRunningTime,
		networkInterfaceRunningTime,
		processResourceRunningTime,
		slimProcessResourceRunningTime,
		systemCpuRunningTime,
		systemMemoryRunningTime,
		tcpConnectionStatRunningTime,
		threadResourceRunningTime,
		slimThreadResourceRunningTime,
		loadAverageRunningTime;
	
	//Used to generate metrics about what the ProcRecordProducer is doing
	private RawRecordProducerStatusRecord mystatus;
	
	//The interval for reporting status on what the ProcRecordProducer is doing (e.g. put the mystatus record into the output queue and start on a new mystatus record);
	//Perhaps this needs to be configurable
	private static int statusIntervalSeconds=5;
	
	//This should be set true when ProcRecordProducer thread should exit.
	private boolean shouldExit=false;
	
	//Controls whether metrics about how this raw record producer is running will be generated.
	private boolean producerMetricsEnabled=false;
	
	//This holds the list of metrics to gather sorted by the time at which they should be gathered.
	private TreeSet<GatherMetricEvent> metricSchedule = new TreeSet<GatherMetricEvent>(new MetricEventComparator());
	
	//The number of clock ticks (jiffies) per second.  Typically 100 but custom kernels can be compiled otherwise
	private int clockTick;
	
	//When there is a failure gathering a metric, wait for this many milliseconds before trying again
	private long GATHER_METRIC_RETRY_INTERVAL = 1000l;
	
	//Manages the list of enabled ProcRecordProducer metrics
	private ProcMetricDescriptorManager enabledMetricManager;
	
	//RecordQueue for raw record producer stat records
	RecordQueue producerStatsQueue;
	
	//Producer name to use when putting output records to record queues
	String producerName;
	
	public String getProducerName(){
		return producerName;
	}
	public ProcRecordProducer(String producerName) {
		setClockTick();
		this.producerStatsQueue = null;
		this.producerName = producerName;
		this.producerMetricsEnabled=false;
		this.enabledMetricManager = new ProcMetricDescriptorManager();
	}
	
	public RecordQueue getProducerStatsQueue(){
		return producerStatsQueue;
	}
	public boolean enableProducerMetrics(RecordQueue producerStatsQueue){
		if(!producerMetricsEnabled && producerStatsQueue!= null){
			this.producerStatsQueue = producerStatsQueue;
			this.producerMetricsEnabled = true;
			return true;
		}
		return false;
	}
	
	public void disableProducerMetrics(){
		this.producerMetricsEnabled=false;
	}
	
	public void run() {
		long actionStartTime;
		GatherMetricEvent event = null;
		
		mystatus = new RawRecordProducerStatusRecord(producerName);
		diskstatRecordCreationFailures=0;
		networkInterfaceRecordCreationFailures=0;
		processResourceRecordCreationFailures=0;
		slimProcessResourceRecordCreationFailures=0;
		systemCpuRecordCreationFailures=0;
		systemMemoryRecordCreationFailures=0;
		tcpConnectionStatRecordCreationFailures=0;
		threadResourceRecordCreationFailures=0;
		slimThreadResourceRecordCreationFailures=0;
		loadAverageRecordCreationFailures=0;
		diskstatRecordProduceRecordFailures=0;
		networkInterfaceRecordProduceRecordFailures=0;
		processResourceRecordProduceRecordFailures=0;
		slimProcessResourceRecordProduceRecordFailures=0;
		systemCpuRecordProduceRecordFailures=0;
		systemMemoryRecordProduceRecordFailures=0;
		tcpConnectionStatRecordProduceRecordFailures=0;
		threadResourceRecordProduceRecordFailures=0;
		slimThreadResourceRecordProduceRecordFailures=0;
		loadAverageRecordProduceRecordFailures=0;
		diskstatRecordPutFailures=0;
		networkInterfaceRecordPutFailures=0;
		processResourceRecordPutFailures=0;
		slimProcessResourceRecordPutFailures=0;
		systemCpuRecordPutFailures=0;
		systemMemoryRecordPutFailures=0;
		tcpConnectionStatRecordPutFailures=0;
		threadResourceRecordPutFailures=0;
		slimThreadResourceRecordPutFailures=0;
		loadAverageRecordPutFailures=0;
		diskstatRecordsCreated=0;
		networkInterfaceRecordsCreated=0;
		processResourceRecordsCreated=0;
		slimProcessResourceRecordsCreated=0;
		systemCpuRecordsCreated=0;
		systemMemoryRecordsCreated=0;
		tcpConnectionStatRecordsCreated=0;
		threadResourceRecordsCreated=0;
		slimThreadResourceRecordsCreated=0;
		loadAverageRecordsCreated=0;
		diskstatRunningTime=0;
		networkInterfaceRunningTime=0;
		processResourceRunningTime=0;
		slimProcessResourceRunningTime=0;
		systemCpuRunningTime=0;
		systemMemoryRunningTime=0;
		tcpConnectionStatRunningTime=0;
		threadResourceRunningTime=0;
		slimThreadResourceRunningTime=0;
		loadAverageRunningTime=0;
		
		//Keep trying to generate requested metrics until explicitly requested to exit
		while(!shouldExit){
			//Report self metrics
			if(!shouldExit && ((System.currentTimeMillis() - mystatus.getTimestamp()) / 1000l) >= statusIntervalSeconds ){
				RawRecordProducerStatusRecord newRecord = null;
				try {
					newRecord = new RawRecordProducerStatusRecord(mystatus);
				} catch (Exception e){
					LOG.error("Failed to generate a RawRecordProducerStatusRecord");
					e.printStackTrace();
					newRecord = new RawRecordProducerStatusRecord(producerName);
				}
				mystatus.setQueuePutFailures(	diskstatRecordPutFailures + 
												networkInterfaceRecordPutFailures + 
												processResourceRecordPutFailures + 
												slimProcessResourceRecordPutFailures + 
												systemCpuRecordPutFailures + 
												systemMemoryRecordPutFailures + 
												tcpConnectionStatRecordPutFailures + 
												threadResourceRecordPutFailures +
												slimThreadResourceRecordPutFailures + 
												loadAverageRecordPutFailures);
				mystatus.setRecordsCreated(	diskstatRecordsCreated + 
											networkInterfaceRecordsCreated + 
											processResourceRecordsCreated + 
											slimProcessResourceRecordsCreated + 
											systemCpuRecordsCreated + 
											systemMemoryRecordsCreated + 
											tcpConnectionStatRecordsCreated + 
											threadResourceRecordsCreated +
											slimThreadResourceRecordsCreated +
											loadAverageRecordsCreated);
				mystatus.setRecordCreationFailures(	diskstatRecordCreationFailures + 
													networkInterfaceRecordCreationFailures + 
													processResourceRecordCreationFailures + 
													slimProcessResourceRecordCreationFailures + 
													systemCpuRecordCreationFailures + 
													systemMemoryRecordCreationFailures + 
													tcpConnectionStatRecordCreationFailures + 
													threadResourceRecordCreationFailures +
													slimThreadResourceRecordCreationFailures +
													loadAverageRecordCreationFailures);
				mystatus.setOtherFailures(	diskstatRecordProduceRecordFailures + 
											networkInterfaceRecordProduceRecordFailures + 
											processResourceRecordProduceRecordFailures + 
											slimProcessResourceRecordProduceRecordFailures + 
											systemCpuRecordProduceRecordFailures + 
											systemMemoryRecordProduceRecordFailures + 
											tcpConnectionStatRecordProduceRecordFailures +
											threadResourceRecordProduceRecordFailures + 
											slimThreadResourceRecordProduceRecordFailures +
											loadAverageRecordProduceRecordFailures);
				mystatus.setRunningTimems(	diskstatRunningTime + 
											networkInterfaceRunningTime + 
											processResourceRunningTime + 
											slimProcessResourceRunningTime + 
											systemCpuRunningTime + 
											systemMemoryRunningTime + 
											tcpConnectionStatRunningTime + 
											threadResourceRunningTime +
											slimThreadResourceRunningTime +
											loadAverageRunningTime);
											
				mystatus.addExtraInfo("diskstatRecordProduceRecordFailures", Integer.toString(diskstatRecordProduceRecordFailures));
				mystatus.addExtraInfo("networkInterfaceRecordProduceRecordFailures", Integer.toString(networkInterfaceRecordProduceRecordFailures));
				mystatus.addExtraInfo("processResourceRecordProduceRecordFailures", Integer.toString(processResourceRecordProduceRecordFailures));
				mystatus.addExtraInfo("slimProcessResourceRecordProduceRecordFailures", Integer.toString(slimProcessResourceRecordProduceRecordFailures));
				mystatus.addExtraInfo("systemCpuRecordProduceRecordFailures", Integer.toString(systemCpuRecordProduceRecordFailures));
				mystatus.addExtraInfo("systemMemoryRecordProduceRecordFailures", Integer.toString(systemMemoryRecordProduceRecordFailures));
				mystatus.addExtraInfo("tcpConnectionStatRecordProduceRecordFailures", Integer.toString(tcpConnectionStatRecordProduceRecordFailures));
				mystatus.addExtraInfo("threadResourceRecordProduceRecordFailures", Integer.toString(threadResourceRecordProduceRecordFailures));
				mystatus.addExtraInfo("slimThreadResourceRecordProduceRecordFailures", Integer.toString(slimThreadResourceRecordProduceRecordFailures));
				mystatus.addExtraInfo("loadAverageRecordProduceRecordFailures", Integer.toString(loadAverageRecordProduceRecordFailures));
				mystatus.addExtraInfo("diskstatRecordCreationFailures", Integer.toString(diskstatRecordCreationFailures));
				mystatus.addExtraInfo("networkInterfaceRecordCreationFailures", Integer.toString(networkInterfaceRecordCreationFailures));
				mystatus.addExtraInfo("processResourceRecordCreationFailures", Integer.toString(processResourceRecordCreationFailures));
				mystatus.addExtraInfo("slimProcessResourceRecordCreationFailures", Integer.toString(slimProcessResourceRecordCreationFailures));
				mystatus.addExtraInfo("systemCpuRecordCreationFailures", Integer.toString(systemCpuRecordCreationFailures));
				mystatus.addExtraInfo("systemMemoryRecordCreationFailures", Integer.toString(systemMemoryRecordCreationFailures));
				mystatus.addExtraInfo("tcpConnectionStatRecordCreationFailures", Integer.toString(tcpConnectionStatRecordCreationFailures));
				mystatus.addExtraInfo("threadResourceRecordCreationFailures", Integer.toString(threadResourceRecordCreationFailures));
				mystatus.addExtraInfo("slimThreadResourceRecordCreationFailures", Integer.toString(slimThreadResourceRecordCreationFailures));
				mystatus.addExtraInfo("loadAverageRecordCreationFailures", Integer.toString(loadAverageRecordCreationFailures));
				mystatus.addExtraInfo("diskstatRecordPutFailures", Integer.toString(diskstatRecordPutFailures));
				mystatus.addExtraInfo("networkInterfaceRecordPutFailures", Integer.toString(networkInterfaceRecordPutFailures));
				mystatus.addExtraInfo("processResourceRecordPutFailures", Integer.toString(processResourceRecordPutFailures));
				mystatus.addExtraInfo("slimProcessResourceRecordPutFailures", Integer.toString(slimProcessResourceRecordPutFailures));
				mystatus.addExtraInfo("systemCpuRecordPutFailures", Integer.toString(systemCpuRecordPutFailures));
				mystatus.addExtraInfo("systemMemoryRecordPutFailures", Integer.toString(systemMemoryRecordPutFailures));
				mystatus.addExtraInfo("tcpConnectionStatRecordPutFailures", Integer.toString(tcpConnectionStatRecordPutFailures));
				mystatus.addExtraInfo("threadResourceRecordPutFailures", Integer.toString(threadResourceRecordPutFailures));
				mystatus.addExtraInfo("slimThreadResourceRecordPutFailures", Integer.toString(slimThreadResourceRecordPutFailures));
				mystatus.addExtraInfo("loadAverageRecordPutFailures", Integer.toString(loadAverageRecordPutFailures));
				mystatus.addExtraInfo("diskstatRecordsCreated", Integer.toString(diskstatRecordsCreated));
				mystatus.addExtraInfo("networkInterfaceRecordsCreated", Integer.toString(networkInterfaceRecordsCreated));
				mystatus.addExtraInfo("processResourceRecordsCreated", Integer.toString(processResourceRecordsCreated));
				mystatus.addExtraInfo("slimProcessResourceRecordsCreated", Integer.toString(slimProcessResourceRecordsCreated));
				mystatus.addExtraInfo("systemCpuRecordsCreated", Integer.toString(systemCpuRecordsCreated));
				mystatus.addExtraInfo("systemMemoryRecordsCreated", Integer.toString(systemMemoryRecordsCreated));
				mystatus.addExtraInfo("tcpConnectionStatRecordsCreated", Integer.toString(tcpConnectionStatRecordsCreated));
				mystatus.addExtraInfo("threadResourceRecordsCreated", Integer.toString(threadResourceRecordsCreated));
				mystatus.addExtraInfo("slimThreadResourceRecordsCreated", Integer.toString(slimThreadResourceRecordsCreated));
				mystatus.addExtraInfo("loadAverageRecordsCreated", Integer.toString(loadAverageRecordsCreated));
				mystatus.addExtraInfo("diskstatRunningTime", Integer.toString(diskstatRunningTime));
				mystatus.addExtraInfo("networkInterfaceRunningTime", Integer.toString(networkInterfaceRunningTime));
				mystatus.addExtraInfo("processResourceRunningTime", Integer.toString(processResourceRunningTime));
				mystatus.addExtraInfo("slimProcessResourceRunningTime", Integer.toString(slimProcessResourceRunningTime));
				mystatus.addExtraInfo("systemCpuRunningTime", Integer.toString(systemCpuRunningTime));
				mystatus.addExtraInfo("systemMemoryRunningTime", Integer.toString(systemMemoryRunningTime));
				mystatus.addExtraInfo("tcpConnectionStatRunningTime", Integer.toString(tcpConnectionStatRunningTime));
				mystatus.addExtraInfo("threadResourceRunningTime", Integer.toString(threadResourceRunningTime));
				mystatus.addExtraInfo("slimThreadResourceRunningTime", Integer.toString(slimThreadResourceRunningTime));
				mystatus.addExtraInfo("diskstatRunningTime%", Double.toString(100d * ((double)diskstatRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("networkInterfaceRunningTime%", Double.toString(100d * ((double)networkInterfaceRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("processResourceRunningTime%", Double.toString(100d * ((double)processResourceRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("slimProcessResourceRunningTime%", Double.toString(100d * ((double)slimProcessResourceRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("systemCpuRunningTime%", Double.toString(100d * ((double)systemCpuRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("systemMemoryRunningTime%", Double.toString(100d * ((double)systemMemoryRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("tcpConnectionStatRunningTime%", Double.toString(100d * ((double)tcpConnectionStatRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("threadResourceRunningTime%", Double.toString(100d * ((double)threadResourceRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("slimThreadResourceRunningTime%", Double.toString(100d * ((double)slimThreadResourceRunningTime) / ((double)mystatus.getDurationms())));
				mystatus.addExtraInfo("loadAverageRunningTime%", Double.toString(100d * ((double)loadAverageRunningTime) / ((double)mystatus.getDurationms())));
				if(producerMetricsEnabled && !producerStatsQueue.put(producerName,mystatus)){
					LOG.error("Failed to put RecordProducerStatusRecord to output queue " + producerStatsQueue.getQueueName() + 
											" size:" + producerStatsQueue.queueSize() + " maxSize:" + producerStatsQueue.getQueueRecordCapacity() + 
											" producerName:" + producerName);
				} 
				mystatus = newRecord;
				diskstatRecordCreationFailures=0;
				networkInterfaceRecordCreationFailures=0;
				processResourceRecordCreationFailures=0;
				slimProcessResourceRecordCreationFailures=0;
				systemCpuRecordCreationFailures=0;
				systemMemoryRecordCreationFailures=0;
				tcpConnectionStatRecordCreationFailures=0;
				threadResourceRecordCreationFailures=0;
				slimThreadResourceRecordCreationFailures=0;
				loadAverageRecordCreationFailures=0;
				diskstatRecordProduceRecordFailures=0;
				networkInterfaceRecordProduceRecordFailures=0;
				processResourceRecordProduceRecordFailures=0;
				slimProcessResourceRecordProduceRecordFailures=0;
				systemCpuRecordProduceRecordFailures=0;
				systemMemoryRecordProduceRecordFailures=0;
				tcpConnectionStatRecordProduceRecordFailures=0;
				threadResourceRecordProduceRecordFailures=0;
				slimThreadResourceRecordProduceRecordFailures=0;
				loadAverageRecordProduceRecordFailures=0;
				diskstatRecordPutFailures=0;
				networkInterfaceRecordPutFailures=0;
				processResourceRecordPutFailures=0;
				slimProcessResourceRecordPutFailures=0;
				systemCpuRecordPutFailures=0;
				systemMemoryRecordPutFailures=0;
				tcpConnectionStatRecordPutFailures=0;
				threadResourceRecordPutFailures=0;
				slimThreadResourceRecordPutFailures=0;
				loadAverageRecordPutFailures=0;
				diskstatRecordsCreated=0;
				networkInterfaceRecordsCreated=0;
				processResourceRecordsCreated=0;
				slimProcessResourceRecordsCreated=0;
				systemCpuRecordsCreated=0;
				systemMemoryRecordsCreated=0;
				tcpConnectionStatRecordsCreated=0;
				threadResourceRecordsCreated=0;
				slimThreadResourceRecordsCreated=0;
				loadAverageRecordsCreated=0;
				diskstatRunningTime=0;
				networkInterfaceRunningTime=0;
				processResourceRunningTime=0;
				slimProcessResourceRunningTime=0;
				systemCpuRunningTime=0;
				systemMemoryRunningTime=0;
				tcpConnectionStatRunningTime=0;
				threadResourceRunningTime=0;
				slimThreadResourceRunningTime=0;
				loadAverageRunningTime=0;
			}
			
			//We need to check what's in metricSchedule, so synchronize on it since other threads might be modifying it at the same time
			boolean waitingForMetrics=true;
			while(waitingForMetrics && !shouldExit){
				synchronized(metricSchedule){
					//Read the next scheduled event from the schedule;
					try {
						event = metricSchedule.first();
						waitingForMetrics = false;
					} catch (NoSuchElementException e){}
				}
				//If there were no metrics in the schedule, sleep for a second
				if(waitingForMetrics){
					try{
						Thread.sleep(1000);
					} catch (Exception e) {}
				}
			}
			if(shouldExit) break;
			//If the time until the event should be executed is greater than 1 second from now, then sleep for a second and check again
			//This is useful when new metrics need to be gathered.  A new metric that needs to be gathered will have it's first sample
			//gathered with a delay of 1 second at most.
			//In contrast, if we didn't do this, if we were gathering metricA on a 5 second period, and right after we gathered metricA, we registered
			//metricB with 1 second period, then metricB wouldn't get serviced until about 5 seconds, after we service metricA.  
			if(event.getTargetTime() - System.currentTimeMillis() > 1000) {
				try{
					Thread.sleep(1000);
				} catch (Exception e) {}
			//We might have a metric to gather within the next 1 second, there is no backing out at this point, the metric must be collected
			} else {	
				//We may need to modify the metric schedule, so synchronize on it
				boolean retrievedEvent=false;
				synchronized(metricSchedule){
					//Check the next metric to gather in the schedule
					try {
						event = metricSchedule.first();
						retrievedEvent=true;
					} catch (NoSuchElementException e){}
				}
				//If the metric should be gathered within the next 1 second, then commit to gathering it this iteration.
				if(retrievedEvent && event.getTargetTime() - System.currentTimeMillis() <= 1000){
					//Track if we are able to gather the metric
					boolean gatheredMetric=false;
					
					//Sleep until it's time to gather the metric
					try {
						Thread.sleep(event.getTargetTime() - System.currentTimeMillis());
					} catch (Exception e) {}
					synchronized(metricSchedule){
						retrievedEvent=false;
						try {
							event = metricSchedule.first();
							retrievedEvent=true;
						} catch (NoSuchElementException e){}
						if(retrievedEvent && event.getTargetTime() <= System.currentTimeMillis()){
							actionStartTime = System.currentTimeMillis();
							try {
								//It's time to gather the metric...
								if(event.getMetricName().equals("Diskstat")){
									//Try to produce the record into the output queue
									gatheredMetric = generateDiskstatRecords(event.getRecordQueue());
								} else if(event.getMetricName().equals("NetworkInterface")) {
									gatheredMetric = generateNetworkInterfaceRecords(event.getRecordQueue());
								} else if(event.getMetricName().equals("ProcessResource")) {
									gatheredMetric = generateProcessResourceRecords(event.getRecordQueue());
								} else if(event.getMetricName().equals("SlimProcessResource")) {
									gatheredMetric = generateSlimProcessResourceRecords(event.getRecordQueue());
								} else if(event.getMetricName().equals("SystemCpu")) {
									gatheredMetric = generateSystemCpuRecord(event.getRecordQueue());
								} else if (event.getMetricName().equals("SystemMemory")) {
									gatheredMetric = generateSystemMemoryRecord(event.getRecordQueue());
								} else if (event.getMetricName().equals("TcpConnectionStat")) {
									gatheredMetric = generateTcpConnectionStatRecords(event.getRecordQueue());
								} else if (event.getMetricName().equals("ThreadResource")) {
									gatheredMetric = generateThreadResourceRecords(event.getRecordQueue());
								} else if (event.getMetricName().equals("SlimThreadResource")) {
									gatheredMetric = generateSlimThreadResourceRecords(event.getRecordQueue());
								} else if (event.getMetricName().equals("LoadAverage")) {
									gatheredMetric = generateLoadAverageRecord(event.getRecordQueue());
								} else 
									throw new Exception("GatherMetricEvent for unknown metric type:" + event.getMetricName());
							} catch (Exception e) {
								LOG.error("ProcRecordProducer: Caught an exception while gathering metric " + event.getMetricName());
								e.printStackTrace();
								gatheredMetric = false;
							} finally {
								if(event.getMetricName().equals("Diskstat"))
									diskstatRunningTime += System.currentTimeMillis() - actionStartTime;
								else if(event.getMetricName().equals("NetworkInterface")) 
									networkInterfaceRunningTime += System.currentTimeMillis() - actionStartTime;
								else if(event.getMetricName().equals("ProcessResource")) 
									processResourceRunningTime += System.currentTimeMillis() - actionStartTime;
								else if(event.getMetricName().equals("SlimProcessResource")) 
									slimProcessResourceRunningTime += System.currentTimeMillis() - actionStartTime;
								else if(event.getMetricName().equals("SystemCpu")) 
									systemCpuRunningTime += System.currentTimeMillis() - actionStartTime;
								else if (event.getMetricName().equals("SystemMemory")) 
									systemMemoryRunningTime += System.currentTimeMillis() - actionStartTime;
								else if (event.getMetricName().equals("TcpConnectionStat")) 
									tcpConnectionStatRunningTime += System.currentTimeMillis() - actionStartTime;
								else if (event.getMetricName().equals("ThreadResource")) 
									threadResourceRunningTime += System.currentTimeMillis() - actionStartTime;
								else if (event.getMetricName().equals("SlimThreadResource")) 
									slimThreadResourceRunningTime += System.currentTimeMillis() - actionStartTime;
								else if (event.getMetricName().equals("LoadAverage")) 
									loadAverageRunningTime += System.currentTimeMillis() - actionStartTime;
							}
							//Now that we've tried to gather the metric, we can remove the event from the schedule, regardless of whether we were successful
							metricSchedule.remove(metricSchedule.first());
							//If we failed to gather the metric...
							if(!gatheredMetric){
							//If the retry interval is less than the periodicity then try again after the retry interval has elapsed
								if (GATHER_METRIC_RETRY_INTERVAL < event.getPeriodicity())
									event.setTargetTime(event.getTargetTime() + GATHER_METRIC_RETRY_INTERVAL);
								//Otherwise, try again after the periodicity has elapsed
								else 
									event.setTargetTime(event.getTargetTime() + event.getPeriodicity());
							//If we successfully gathered the metric
							} else {
								event.setPreviousTime(actionStartTime);
								event.setTargetTime(event.getTargetTime() + event.getPeriodicity());
							}
							//Stuff happens and it's possible that we can't gather a metric in a reasonable amount of time, sometimes or all the time.
							//So rather than allowing this to spin at 100% CPU, we should insert a delay in gathering a given metric if the time at which
							//we want to gather the metric next to keep on schedule with periodicity is less than the current time.
							if(event.getTargetTime() <= System.currentTimeMillis())
								event.setTargetTime(System.currentTimeMillis() + event.getPeriodicity());
							//Add the event back on to the schedule with the new target time.
							metricSchedule.add(event);
							//Record how long it took to generate this metric
							mystatus.setRunningTimems(System.currentTimeMillis() - actionStartTime + mystatus.getRunningTimems());
						}
					}
				}
			}
		}
		//TODO: Do some shutdown stuff here...
	}
	
	public boolean producerMetricsEnabled(){
		return producerMetricsEnabled;
	}

	public static boolean isValidMetricName(String metricName){
		if(metricName.equals("Diskstat") || metricName.equals("NetworkInterface") || metricName.equals("ProcessResource") ||
				metricName.equals("SystemCpu") || metricName.equals("SystemMemory") || metricName.equals ("TcpConnectionStat") ||
				metricName.equals("ThreadResource") || metricName.equals("SlimThreadResource") || metricName.equals("SlimProcessResource") ||
				metricName.equals("LoadAverage"))
			return true;
		return false;
	}
	
	public String[] listEnabledMetrics(){
		synchronized(metricSchedule){
			String[] ret = new String[metricSchedule.size()];
			int pos=0;
			Iterator<GatherMetricEvent> i = metricSchedule.iterator();
			while(i.hasNext()){
				GatherMetricEvent e = i.next();
				ret[pos++] = "Metric:\"" + e.getMetricName() + "\"\tQueue:\"" + e.getRecordQueue().getQueueName() + "\"\tPeriodicity:" + e.getPeriodicity();
			}
			return ret;
		}
	}
	
	public boolean isMetricEnabled(String metricName, RecordQueue outputQueue, int periodicity){
		if(metricName==null || outputQueue == null){
			return false;
		}
		if(!enabledMetricManager.containsDescriptor(metricName, periodicity)){
			return false;
		}
		boolean found=false;
		synchronized(metricSchedule){
			Iterator<GatherMetricEvent> i = metricSchedule.iterator();
			while(i.hasNext()){
				GatherMetricEvent e = i.next();
				if(e.getMetricName().equals(metricName) && e.getRecordQueue().equals(outputQueue) && e.getPeriodicity() == periodicity){
					found=true;
					break;
				}
			}
		}
		if(!found){
			return false;
		}
		return true;
	}
	public boolean disableMetric(String metricName, RecordQueue outputQueue, int periodicity){
		if(outputQueue == null || metricName == null){
			LOG.error("Can't disable null metric");
			return false;
		}
		synchronized(enabledMetricManager){
			synchronized(metricSchedule){
				if(!enabledMetricManager.containsDescriptor(metricName, periodicity))
					return false;
				Iterator<GatherMetricEvent> i = metricSchedule.iterator();
				boolean foundEvent=false;
				GatherMetricEvent e = null;
				while(i.hasNext()){
					e = i.next();
					if(e.getMetricName().equals(metricName) && e.getRecordQueue().equals(outputQueue) && e.getPeriodicity() == periodicity){
						foundEvent=true;
						break;
					}
				}
				if(!foundEvent)
					return false;
				metricSchedule.remove(e);
				enabledMetricManager.removeDescriptor(metricName, periodicity);
			}
		}
		return true;
	}
	//Request this instance of ProcRecordProducer to produce the named type of Record with the specified periodicity and insert the Records into the named RecordQueue
	//This returns as long as the metric will be produced as requested.
	//If for any reason the metric can not be produced as requested, then this throws an exception.
    //When this throws an exception, the caller should presume that the requested metric will NOT be produced.
    //The exception text will explain why the metric couldn't be produced as requested.
	public void enableMetric(String metricName, RecordQueue outputQueue, int periodicity) throws Exception{		
		//Throw an exception if we were given an invalid periodicity
		if(periodicity < 1)
			throw new Exception("Invalid periodicity:" + periodicity + " - Periodicity for metric must be greater than 0");
			
		//Throw an exception if the name of the metric being requested is not valid
		if(!isValidMetricName(metricName))
			throw new Exception("Invalid metric name " + metricName);
		
		//Throw an exception if we weren't given an output queue.
		if(outputQueue == null){
			throw new Exception("outputQueue is null");
		}
		GatherMetricEvent event = null;
		if(!enabledMetricManager.containsDescriptor(metricName, periodicity)){
			event = new GatherMetricEvent(0l, 0l, metricName, outputQueue, periodicity);
			synchronized(metricSchedule){
				metricSchedule.add(event);
			}
			enabledMetricManager.addDescriptor(metricName, periodicity);
		}
	}

	private boolean generateThreadResourceRecords(RecordQueue outputQueue){
		boolean returnCode = true;
        try {
                FilenameFilter fnFilter = new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                                if(name.charAt(0) >= '1' && name.charAt(0) <= '9')
                                        return true;
                                return false;
                        }
                };
                File ppFile = new File("/proc");
                File[] pPaths = ppFile.listFiles(fnFilter);
                //For each process in /proc
                for (int pn = 0; pn<pPaths.length; pn++){
                        int ppid = Integer.parseInt(pPaths[pn].getName());
                        String taskPathStr = pPaths[pn].toString() + "/task";

                        File tpFile = new File(taskPathStr);
                        File[] tPaths = tpFile.listFiles(fnFilter);
                        if(tPaths != null) {
                                for (int x=0; x<tPaths.length; x++){
                                        String statPath = tPaths[x].toString() + "/stat";
                                        String ioPath = tPaths[x].toString() + "/io";
                                        int[] ret = null;
                                        try {
                                        	ret = ThreadResourceRecord.produceRecord(outputQueue, producerName, statPath, ioPath, ppid, clockTick);
                                        } catch (Exception e) {
                                        	ret = new int[] {1, 0, 0, 0};
                                        }
                                        if(ret[0] == 0){
                                        	threadResourceRecordsCreated += ret[1];
                                        	threadResourceRecordCreationFailures += ret[2];
                                        	threadResourceRecordPutFailures += ret[3];
                                        } else {
                                        	threadResourceRecordProduceRecordFailures++;
                                        	returnCode=false;
                                        }
                                }
                        }
                }
        } catch(Exception e){
        	LOG.error("Unexpected exception:");
        	e.printStackTrace();
        	return false;
        }
        return returnCode;
	}
	
	private boolean generateSlimThreadResourceRecords(RecordQueue outputQueue){
		boolean returnCode = true;
        try {
                FilenameFilter fnFilter = new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                                if(name.charAt(0) >= '1' && name.charAt(0) <= '9')
                                        return true;
                                return false;
                        }
                };
                File ppFile = new File("/proc");
                File[] pPaths = ppFile.listFiles(fnFilter);
                //For each process in /proc
                for (int pn = 0; pn<pPaths.length; pn++){
                        int ppid = Integer.parseInt(pPaths[pn].getName());
                        String taskPathStr = pPaths[pn].toString() + "/task";

                        File tpFile = new File(taskPathStr);
                        File[] tPaths = tpFile.listFiles(fnFilter);
                        if(tPaths != null) {
                                for (int x=0; x<tPaths.length; x++){
                                        String statPath = tPaths[x].toString() + "/stat";
                                        String ioPath = tPaths[x].toString() + "/io";
                                        int[] ret = null;
                                        try {
                                        	ret = SlimThreadResourceRecord.produceRecord(outputQueue, producerName, statPath, ioPath, ppid, clockTick);
                                        } catch (Exception e) {
                                        	ret = new int[] {1, 0, 0, 0};
                                        }
                                        if(ret[0] == 0){
                                        	slimThreadResourceRecordsCreated += ret[1];
                                        	slimThreadResourceRecordCreationFailures += ret[2];
                                        	slimThreadResourceRecordPutFailures += ret[3];
                                        } else {
                                        	slimThreadResourceRecordProduceRecordFailures++;
                                        	returnCode=false;
                                        }
                                }
                        }
                }
        } catch(Exception e){
        	LOG.error("Unexpected exception:");
        	e.printStackTrace();
        	return false;
        }
        return returnCode;
	}
	
	private boolean generateProcessResourceRecords(RecordQueue outputQueue) {
        boolean returnCode = true;
        try {
                FilenameFilter fnFilter = new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                                if(name.charAt(0) >= '1' && name.charAt(0) <= '9')
                                        return true;
                                return false;
                        }
                };
                File ppFile = new File("/proc");
                File[] pPaths = ppFile.listFiles(fnFilter);

                //For each process in /proc
                for (int pn = 0; pn<pPaths.length; pn++){
                        //Retrieve the process counters contained in /proc/[pid]/stat
                	String statPath = pPaths[pn].toString() + "/stat";
                	String ioPath = pPaths[pn].toString() + "/io";
                	int[] ret = null;
                	try {
                		ret = ProcessResourceRecord.produceRecord(outputQueue, producerName, statPath, ioPath, clockTick);
                	} catch (Exception e) {
                		ret = new int[]{1,0,0,0};
                	}
                	if(ret[0] == 0){
                    	processResourceRecordsCreated += ret[1];
                    	processResourceRecordCreationFailures += ret[2];
                    	processResourceRecordPutFailures += ret[3];
                    } else {
                    	processResourceRecordProduceRecordFailures++;
                    	returnCode = false;
                    }
                }
        } catch (Exception e) {}
       return returnCode;
	}
	
	private boolean generateSlimProcessResourceRecords(RecordQueue outputQueue) {
        boolean returnCode = true;
        try {
                FilenameFilter fnFilter = new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                                if(name.charAt(0) >= '1' && name.charAt(0) <= '9')
                                        return true;
                                return false;
                        }
                };
                File ppFile = new File("/proc");
                File[] pPaths = ppFile.listFiles(fnFilter);

                //For each process in /proc
                for (int pn = 0; pn<pPaths.length; pn++){
                        //Retrieve the process counters contained in /proc/[pid]/stat
                	String statPath = pPaths[pn].toString() + "/stat";
                	String ioPath = pPaths[pn].toString() + "/io";
                	int[] ret = null;
                	try {
                		ret = SlimProcessResourceRecord.produceRecord(outputQueue, producerName, statPath, ioPath, clockTick);
                	} catch (Exception e) {
                		ret = new int[]{1,0,0,0};
                	}
                	if(ret[0] == 0){
                    	slimProcessResourceRecordsCreated += ret[1];
                    	slimProcessResourceRecordCreationFailures += ret[2];
                    	slimProcessResourceRecordPutFailures += ret[3];
                    } else {
                    	slimProcessResourceRecordProduceRecordFailures++;
                    	returnCode = false;
                    }
                }
        } catch (Exception e) {}
       return returnCode;
	}

	private boolean generateNetworkInterfaceRecords(RecordQueue outputQueue) {
		boolean returnCode=true;
		try {
                for (NetworkInterface i : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                        if(i.getName().equals("lo")){
                                continue;
                        }
                        int[] ret = null;
                        try {
                          ret = NetworkInterfaceRecord.produceRecord(outputQueue, producerName, i.getName());
                        } catch (Exception e) {
                        	ret = new int[] {1, 0, 0, 0};
                        }
                        if(ret[0] == 0){
                        	networkInterfaceRecordsCreated += ret[1];
                        	networkInterfaceRecordCreationFailures += ret[2];
                        	networkInterfaceRecordPutFailures += ret[3];
                        } else {
                        	networkInterfaceRecordProduceRecordFailures++;
                        	returnCode = false;
                        }
                }
        } catch (Exception e) {
                LOG.error("Failed to list network interfaces");
                e.printStackTrace();
                return false;
        }
        return returnCode;
	}

	private boolean generateSystemMemoryRecord(RecordQueue outputQueue) {
		int[] ret = null;
		try {
			ret =  SystemMemoryRecord.produceRecord(outputQueue, producerName);
		} catch (Exception e) {
			ret = new int[] {1, 0, 0, 0};
		}
		if(ret[0] == 0){
        	systemMemoryRecordsCreated += ret[1];
        	systemMemoryRecordCreationFailures += ret[2];
        	systemMemoryRecordPutFailures += ret[3];
        	return true;
        } else {
        	systemMemoryRecordProduceRecordFailures++;
        	return false;
        }
	}

	private boolean generateDiskstatRecords(RecordQueue outputQueue) {
		int[] ret = null;
		try {
			ret =  DiskstatRecord.produceRecords(outputQueue, producerName);
		} catch (Exception e) {
			ret = new int[] {1, 0, 0, 0};
		}
		if(ret[0] == 0){
        	diskstatRecordsCreated += ret[1];
        	diskstatRecordCreationFailures += ret[2];
        	diskstatRecordPutFailures += ret[3];
        	return true;
        } else {
        	diskstatRecordProduceRecordFailures++;
        	return false;
        }
	}

	private boolean generateSystemCpuRecord(RecordQueue outputQueue) {
		int[] ret = null;
		try {
			ret = SystemCpuRecord.produceRecord(outputQueue, producerName);
		} catch (Exception e){
			ret = new int[] {1, 0, 0, 0};
		}
		if(ret[0] == 0){
        	systemCpuRecordsCreated += ret[1];
        	systemCpuRecordCreationFailures += ret[2];
        	systemCpuRecordPutFailures += ret[3];
        	return true;
        } else {
        	systemCpuRecordProduceRecordFailures++;
        	return false;
        }
	}

	private boolean generateLoadAverageRecord(RecordQueue outputQueue) {
		int[] ret = null;
		try {
			ret = LoadAverageRecord.produceRecord(outputQueue, producerName);
		} catch (Exception e){
			ret = new int[] {1, 0, 0, 0};
		}
		if(ret[0] == 0){
        	loadAverageRecordsCreated += ret[1];
        	loadAverageRecordCreationFailures += ret[2];
        	loadAverageRecordPutFailures += ret[3];
        	return true;
        } else {
        	loadAverageRecordProduceRecordFailures++;
        	return false;
        }
	}

	private boolean generateTcpConnectionStatRecords(RecordQueue outputQueue) {
		int[] ret = null;
		try {
			ret = TcpConnectionStatRecord.produceRecords(outputQueue, producerName);
		} catch (Exception e) {
			ret = new int[] {1, 0, 0, 0};
		}
        if(ret[0] == 0){
        	tcpConnectionStatRecordsCreated += ret[1];
        	tcpConnectionStatRecordCreationFailures += ret[2];
        	tcpConnectionStatRecordPutFailures += ret[3];
        	return true;
        } else {
        	tcpConnectionStatRecordProduceRecordFailures++;
        	return false;
        }
	}
	
	private void setClockTick(){
		//CLK_TCK is needed to calculate CPU usage of threads/processes based on utime and stime fields which are measured in this unit
		ProcessBuilder processBuilder = new ProcessBuilder(new String[]{"getconf", "CLK_TCK"});
		Process process = null;
		int ret = -1;
		try{
			process = processBuilder.start();
			ret = process.waitFor();
		} catch (Exception e) {
			LOG.error("Failed to wait for getconf to complete");
			e.printStackTrace();
			System.exit(1);
		}
		if(ret == 0){
			InputStream stdout = process.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			try {
				String line = br.readLine();
				if (line != null){
					clockTick = Integer.parseInt(line);
					if(clockTick < 1){
						LOG.error("Failed to retrieve sysconfig(\"CLK_TCK\")");
						System.exit(1);
					}
				}
			} catch (Exception e) {
				LOG.error("Failed to read getconf output");
				e.printStackTrace();
			}
		} else {
			LOG.error("Failed to run \"getconf CLK_TCK\"");
			System.exit(1);
		}
	}
	
	public void requestExit(){
		shouldExit=true;
	}
}
