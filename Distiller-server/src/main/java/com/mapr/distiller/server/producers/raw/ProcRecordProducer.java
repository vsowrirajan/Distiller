package com.mapr.distiller.server.producers.raw;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.TreeSet;
import java.io.File;
import java.net.NetworkInterface;
import java.lang.Integer;
import java.io.FilenameFilter;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.mapr.distiller.server.recordtypes.DiskstatRecord;
import com.mapr.distiller.server.recordtypes.NetworkInterfaceRecord;
import com.mapr.distiller.server.recordtypes.ProcessResourceRecord;
import com.mapr.distiller.server.recordtypes.SystemCpuRecord;
import com.mapr.distiller.server.recordtypes.SystemMemoryRecord;
import com.mapr.distiller.server.recordtypes.TcpConnectionStatRecord;
import com.mapr.distiller.server.recordtypes.ThreadResourceRecord;
import com.mapr.distiller.server.scheduler.GatherMetricEvent;
import com.mapr.distiller.server.scheduler.MetricEventComparator;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.SubscriptionRecordQueue;

public class ProcRecordProducer extends Thread {
	//This should be set true when ProcRecordProducer thread should exit.
	private boolean shouldExit=false;
	
	//This holds the list of metrics to gather sorted by the time at which they should be gathered.
	private TreeSet<GatherMetricEvent> metricSchedule = new TreeSet<GatherMetricEvent>(new MetricEventComparator());
	
	//The number of clock ticks (jiffies) per second.  Typically 100 but custom kernels can be compiled otherwise
	private int clockTick;
	
	//The length of time, in milliseconds, represented by a single clock tick (jiffy). Typically 10ms
	private Double clockTickms;
	
	//Mapping for RecordQueues to their names
	private ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap;
	
	//When there is a failure gathering a metric, wait for this many milliseconds before trying again
	private long GATHER_METRIC_RETRY_INTERVAL = 1000l;

	//RecordQueues to hold output records for all the different types of metrics this RecordProducer can produce
	private SubscriptionRecordQueue cpu_system = null, 			//Queue for "cpu.system" raw metrics, sourced from /proc/stat
									disk_system = null, 		//Queue for "disk.system" raw metrics, sourced from /proc/diskstats
									memory_system = null, 		//Queue for "memory.system" raw metrics, sourced from /proc/vmstat and /proc/meminfo
									network_interfaces = null,	//Queue for "network.interfaces" raw metrics, sourced from /sys/class/net/<iface>
									process_resources = null,	//Queue for "process.resources" raw metrics, sourced from /proc/[pid]/stat
									thread_resources = null,	//Queue for "thread.resources" raw metrics, sourced from /proc/[pid]/task/[tid]/stat
									tcp_connection_stats = null;//Queue for "tcp.connection.stats" raw metrics, sourced from /proc/net/tcp and /proc/[pid]/fd/*
	
	public ProcRecordProducer(ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap, ConcurrentHashMap<String, Integer> rawMetricList) {
		//setClockTick();
		this.nameToRecordQueueMap = nameToRecordQueueMap;
		Iterator<Map.Entry<String, Integer>> i = rawMetricList.entrySet().iterator();
		if(!i.hasNext()){
			System.err.println("No metrics requested from ProcRecordProducer");
		}
		while(i.hasNext()) {
			Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)i.next();
			String metricName = pair.getKey();
			int periodicity = pair.getValue().intValue();
			
			if(metricName.equals("cpu.system")){
				if(!initialize_cpu_system(periodicity)){
					System.err.println("Failed to initialize cpu.system metric");
					System.exit(1);
				}
			} else if (metricName.equals("disk.system")){
				if(!initialize_disk(periodicity)) {
					System.err.println("Failed to initialize disk.system metric");
					System.exit(1);
				}
			} else if (metricName.equals("memory.system")){
				if(!initialize_memory_system(periodicity)) {
					System.err.println("Failed to initialize memory.system metric");
					System.exit(1);
				}
			} else if (metricName.equals("network.interfaces")) {
				if(!initialize_network_interfaces(periodicity)) {
					System.err.println("Failed to initialize network.interfaces metric");
					System.exit(1);
				}
			} else if (metricName.equals("process.resources")) {
				if(!initialize_process_resources(periodicity)) {
					System.err.println("Failed to initialize process.resources metric");
					System.exit(1);
				}
			} else if (metricName.equals("thread.resources")) {
				if(!initialize_thread_resources(periodicity)) {
					System.err.println("Failed to initialize process.resources metric");
					System.exit(1);
				}
			} else if (metricName.equals("tcp.connection.stats")) {
				if(!initialize_tcp_connection_stats(periodicity)) {
					System.err.println("Failed to initialize process.resources metric");
					System.exit(1);
				}
			} else {
				System.err.println("Request received to gather an unknown metric: " + metricName);
			}
			System.err.println("Received request to gather metric " + metricName + " with periodicity " + periodicity + " ms");
		}
		
	}
	public void run() {
		long timeSpentCollectingMetrics=0l;
		long tStartTime = System.currentTimeMillis();
		
		//Keep trying to generate requested metrics until explicitly requested to exit
		while(!shouldExit){
			//Read the next scheduled event from the schedule;
			GatherMetricEvent event = metricSchedule.first();
			
			//Check how long until the event is supposed to be executed
			//The time member in the GatherMetricEvent indicates the time at which the metric was last gathered
			//The periodicity member indicates how long we try to wait (in ms) in between rounds of gathering the metric
			//The time the metric should be gathered next is equal to the time the metric was last gathered (event.time) plus the 
			//the periodicity.  Therefore the time we need to sleep can be calcaulted by substracting the time now from that target time
			long actionStartTime = System.currentTimeMillis();
			long sleepTime = event.getTargetTime() - actionStartTime;
			
			//If the time until the event should be executed is greater than 1 second from now, then sleep for a second and check again
			//TThis is useful when new metrics need to be gathered.  A new metric that needs to be gathered will have it's first sample
			//gathered with a delay of 1 second at most.
			//In contrast, if we didn't do this, if we were gathering metricA on a 5 second period, and right after we gathered metricA, we registered
			//metricB with 1 second period, then metricB wouldn't get serviced until about 5 seconds, after we service metricA.  
			if(sleepTime > 1000) {
				try{
					Thread.sleep(1000);
				} catch (Exception e) {
					System.err.println("interrupted while sleeping in ProcRecordProducer");
					e.printStackTrace();
				}
			//If the time until the metric is supposed to gathered is <=1 second
			} else {			
				//Remove the metric event from the schedule since we will re-add it with an updated time once it's been gathered this iteration
				metricSchedule.remove(event);
				
				//Sleep until it's time to gather the metric
				if(sleepTime > 0l){
					try{
						Thread.sleep(sleepTime);
					} catch (Exception e) {
						System.err.println("interrupted while sleeping in ProcRecordProducer");
						e.printStackTrace();
					}
				}
				
				actionStartTime = System.currentTimeMillis();
				//Gather the specific metric type
				
				/**
				 * Eventually, in this section of code, we should group together metrics that are gathered with periodicities that are
				 * identical or factors of each other.  E.g. if cpu.system is to be gathered once a second and process.resources to be
				 * gathered once every two seconds, we should schedule them such that at t=1 we gather cpu.system, at t=2 we gather
				 * cpu.system and cpu.resources
				 */
				if(event.getMetricName().equals("cpu.system")){	
					//If we successfully gather the record
					if(generateSystemCpuRecord(event.getPreviousTime(), actionStartTime)){
						//Update the event with the time we gathered the metric and the new target time
						event.setPreviousTime(actionStartTime);
						event.setTargetTime(actionStartTime + event.getPeriodicity());
					//If we failed to gather the record
					} else {
						//Schedule another attempt to gather the metric after the GATHER_METRIC_RETRY_INTERVAL
						System.err.println("Failed to generate cpu.system metric");
						event.setTargetTime(actionStartTime + GATHER_METRIC_RETRY_INTERVAL);
					}
					metricSchedule.add(event);
				} else if (event.getMetricName().equals("disk.system")) {
					if(generateDiskRecord(event.getPreviousTime(), actionStartTime)){
						event.setPreviousTime(actionStartTime);
						event.setTargetTime(actionStartTime + event.getPeriodicity());
					} else {
						System.err.println("Failed to generate disk.system metric");
						event.setTargetTime(actionStartTime + GATHER_METRIC_RETRY_INTERVAL);
					}
					metricSchedule.add(event);
				} else if (event.getMetricName().equals("memory.system")) {
					if(generateSystemMemoryRecord(event.getPreviousTime(), actionStartTime)){
						event.setPreviousTime(actionStartTime);
						event.setTargetTime(actionStartTime + event.getPeriodicity());
					} else {
						System.err.println("Failed to generate memory.system metric");
						event.setTargetTime(actionStartTime + GATHER_METRIC_RETRY_INTERVAL);
					}
					metricSchedule.add(event);
				} else if (event.getMetricName().equals("network.interfaces")) {
					if(generateNetworkInterfaceRecords(event.getPreviousTime(), actionStartTime)){
						event.setPreviousTime(actionStartTime);
						event.setTargetTime(actionStartTime + event.getPeriodicity());
					} else {
						System.err.println("Failed to generate network.interfaces metric");
						event.setTargetTime(actionStartTime + GATHER_METRIC_RETRY_INTERVAL);
					}
					metricSchedule.add(event);
				} else if (event.getMetricName().equals("process.resources")) {
					if(generateProcessResourceRecords(event.getPreviousTime(), actionStartTime)){
						event.setPreviousTime(actionStartTime);
						event.setTargetTime(actionStartTime + event.getPeriodicity());
					} else {
						System.err.println("Failed to generate process.resources metric");
						event.setTargetTime(actionStartTime + GATHER_METRIC_RETRY_INTERVAL);
					}
					metricSchedule.add(event);
				} else if (event.getMetricName().equals("thread.resources")) {
					if(generateThreadResourceRecords(event.getPreviousTime(), actionStartTime)){
						event.setPreviousTime(actionStartTime);
						event.setTargetTime(actionStartTime + event.getPeriodicity());
					} else {
						System.err.println("Failed to generate thread.resources metric");
						event.setTargetTime(actionStartTime + GATHER_METRIC_RETRY_INTERVAL);
					}
					metricSchedule.add(event);
				} else if (event.getMetricName().equals("tcp.connection.stats")) {
					if(generateTcpConnectionStatRecords(event.getPreviousTime(), actionStartTime)){
						event.setPreviousTime(actionStartTime);
						event.setTargetTime(actionStartTime + event.getPeriodicity());
					} else {
						System.err.println("Failed to generate tcp.connection.stats metric");
						event.setTargetTime(actionStartTime + GATHER_METRIC_RETRY_INTERVAL);
					}
					metricSchedule.add(event);
				} else {
					System.err.println("Request to gather an unknown metric type: " + event.getMetricName());
				}
				timeSpentCollectingMetrics += System.currentTimeMillis() - actionStartTime;
				long elapsedTime = System.currentTimeMillis() - tStartTime;
				System.err.println("Running for " + timeSpentCollectingMetrics + " ms out of " + elapsedTime + " ms, " + (100 * timeSpentCollectingMetrics / elapsedTime));
				if(elapsedTime > 60000l) {
					timeSpentCollectingMetrics=0l;
					tStartTime = System.currentTimeMillis();
				}
			}
		}
		
		//Time to shut down
		//cpu_system.unregisterProducer("/proc");
	}

	//We shouldn't be creating the record queue here.  Metrics should be able to dynamically be turned on/off, so we should check if
	//this queue already exists and re-use it if possible, and also ensure it's not already being produced into by some other producer.
	private boolean initialize_thread_resources(int periodicity){
		thread_resources = new SubscriptionRecordQueue("thread.resources", 131072);					//Probably shouldn't do this here
		thread_resources.registerProducer("/proc");
		GatherMetricEvent event = new GatherMetricEvent(0l, 0l, "thread.resources", periodicity);
		metricSchedule.add(event);
		nameToRecordQueueMap.put("thread.resources",  thread_resources);							//Probably shouldn't do this here
		return true;
	}
	private boolean generateThreadResourceRecords(long previousTime, long now){
		long st = System.currentTimeMillis();
		int outputRecordsGenerated=0;
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
						if(ThreadResourceRecord.produceRecord(thread_resources, statPath, ppid, clockTick))
							outputRecordsGenerated++;
					}
				}
			}
		} catch(Exception e){e.printStackTrace();}	
		//long dur = System.currentTimeMillis() - st;
		//System.err.println("Generated " + outputRecordsGenerated + " thread output records in " + dur + " ms, " + ((double)outputRecordsGenerated / (double)dur));
		return true;			
	}
	private boolean initialize_process_resources(int periodicity){
		process_resources = new SubscriptionRecordQueue("process.resources", 131072);
		process_resources.registerProducer("/proc");
		GatherMetricEvent event = new GatherMetricEvent(0l, 0l, "process.resources", periodicity);
		metricSchedule.add(event);
		nameToRecordQueueMap.put("process.resources",  process_resources);
		return true;
	}
	private boolean generateProcessResourceRecords(long previousTime, long now) {
		long st = System.currentTimeMillis();
		int outputRecordsGenerated=0;
		
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
				if(ProcessResourceRecord.produceRecord(process_resources, pPaths[pn].toString() + "/stat", clockTick))
					outputRecordsGenerated++;
			}
		} catch (Exception e) {}
		//long dur = System.currentTimeMillis() - st;
		//System.err.println("Generated " + outputRecordsGenerated + " process output records in " + dur + " ms, " + ((double)outputRecordsGenerated / (double)dur));
		return true;
	}
	private boolean initialize_network_interfaces(int periodicity){
		network_interfaces = new SubscriptionRecordQueue("network.interfaces", 600);
		network_interfaces.registerProducer("/proc");
		GatherMetricEvent event = new GatherMetricEvent(0l, 0l, "network.interfaces", periodicity);
		metricSchedule.add(event);
		nameToRecordQueueMap.put("network.interfaces",  network_interfaces);
		return true;
	}
	private boolean generateNetworkInterfaceRecords(long previousTime, long now) {
		try {
			for (NetworkInterface i : Collections.list(NetworkInterface.getNetworkInterfaces())) {
				if(i.getName().equals("lo")){
					continue;
				}
				NetworkInterfaceRecord.produceRecord(network_interfaces, i.getName());
			}
		} catch (Exception e) {
			System.err.println("Failed to list network interfaces");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	private boolean initialize_memory_system(int periodicity){
		memory_system = new SubscriptionRecordQueue("memory.system", 300);
		memory_system.registerProducer("/proc");
		GatherMetricEvent event = new GatherMetricEvent(0l, 0l, "memory.system", periodicity);
		metricSchedule.add(event);
		nameToRecordQueueMap.put("memory.system",  memory_system);
		return true;
	}	
	private boolean generateSystemMemoryRecord(long previousTime, long now) {
		return SystemMemoryRecord.produceRecord(memory_system);
	}
	private boolean initialize_disk(int periodicity){
		disk_system = new SubscriptionRecordQueue("disk.system", 4096);
		disk_system.registerProducer("/proc");
		GatherMetricEvent event = new GatherMetricEvent(0l, 0l, "disk.system", periodicity);
		metricSchedule.add(event);
		nameToRecordQueueMap.put("disk.system", disk_system);
		return true;
	}
	private boolean generateDiskRecord(long previousTime, long now) {
		return DiskstatRecord.produceRecords(disk_system);
	}
	private boolean initialize_cpu_system(int periodicity){
		cpu_system = new SubscriptionRecordQueue("cpu.system", 300);
		cpu_system.registerProducer("/proc");
		GatherMetricEvent event = new GatherMetricEvent(0l, 0l, "cpu.system", periodicity);
		metricSchedule.add(event);
		nameToRecordQueueMap.put("cpu.system", cpu_system);
		return true;
	}
	private boolean generateSystemCpuRecord(long previousTime, long now) {
		return SystemCpuRecord.produceRecord(cpu_system);
	}
	private boolean initialize_tcp_connection_stats(int periodicity){
		tcp_connection_stats = new SubscriptionRecordQueue("tcp.connection.stats", 65536);
		tcp_connection_stats.registerProducer("/proc");
		GatherMetricEvent event = new GatherMetricEvent(0l, 0l, "tcp.connection.stats", periodicity);
		metricSchedule.add(event);
		nameToRecordQueueMap.put("tcp.connection.stats", tcp_connection_stats);
		return true;
	}
	private boolean generateTcpConnectionStatRecords(long previousTime, long now) {
		return TcpConnectionStatRecord.produceRecords(tcp_connection_stats);

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
			System.err.println("Failed to wait for getconf to complete");
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
						System.err.println("Failed to retrieve sysconfig(\"CLK_TCK\")");
						System.exit(1);
					}
					clockTickms = 1000d / (double)clockTick;
				}
			} catch (Exception e) {
				System.err.println("Failed to read getconf output");
				e.printStackTrace();
			}
			System.err.println("Retrieved CLK_TLK=" + clockTick);
		} else {
			System.err.println("Failed to run \"getconf CLK_TCK\"");
			System.exit(1);
		}
	}
}
