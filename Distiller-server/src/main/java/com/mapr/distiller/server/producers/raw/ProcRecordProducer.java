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
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.datatypes.ProcMetricDescriptorManager;

public class ProcRecordProducer extends Thread {
	//This should be set true when ProcRecordProducer thread should exit.
	private boolean shouldExit=false;
	
	//This holds the list of metrics to gather sorted by the time at which they should be gathered.
	private TreeSet<GatherMetricEvent> metricSchedule = new TreeSet<GatherMetricEvent>(new MetricEventComparator());
	
	//The number of clock ticks (jiffies) per second.  Typically 100 but custom kernels can be compiled otherwise
	private int clockTick;
	
	//RecordQueueManager for the output RecordQueues that will be used by ProcRecordProducer when it has metrics enabled
	private RecordQueueManager queueManager;
	
	//When there is a failure gathering a metric, wait for this many milliseconds before trying again
	private long GATHER_METRIC_RETRY_INTERVAL = 1000l;
	
	//Manages the list of enabled ProcRecordProducer metrics
	private ProcMetricDescriptorManager enabledMetricManager;
	
	public ProcRecordProducer(RecordQueueManager queueManager) {
		//setClockTick();
		this.queueManager = queueManager;
		this.enabledMetricManager = new ProcMetricDescriptorManager();
	}
	public void run() {
		long timeSpentCollectingMetrics=0l;
		long tStartTime = System.currentTimeMillis();
		long actionStartTime;
		GatherMetricEvent event = null;
		//Keep trying to generate requested metrics until explicitly requested to exit
		while(!shouldExit){
			//We need to check whats in metricSchedule, so synchronize on it since other threads might be modifying it at the same time
			boolean waitingForMetrics=true;
			while(waitingForMetrics){
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
				synchronized(metricSchedule){
					//Check the next metric to gather in the schedule
					event = metricSchedule.first();
					//If the metric should be gathered within the next 1 second, then commit to gathering it this iteration.
					if(event.getTargetTime() - System.currentTimeMillis() <= 1000){
						//Track if we are able to gather the metric
						boolean gatheredMetric = false;

						//Sleep until it's time to gather the metric
						try {
							Thread.sleep(event.getTargetTime() - System.currentTimeMillis());
						} catch (Exception e) {}
						if(event.getTargetTime() >= System.currentTimeMillis()){
							actionStartTime = System.currentTimeMillis();
							try {
								//It's time to gather the metric...
								if(event.getMetricName().equals("Diskstat"))
									//Try to produce the record into the output queue
									gatheredMetric = generateDiskstatRecords(queueManager.getQueue(event.getMetricName()), event.getProducerName());
								else if(event.getMetricName().equals("NetworkInterface"))
									gatheredMetric = generateNetworkInterfaceRecords(queueManager.getQueue(event.getMetricName()), event.getProducerName());
								else if(event.getMetricName().equals("ProcessResource"))
									gatheredMetric = generateProcessResourceRecords(queueManager.getQueue(event.getMetricName()), event.getProducerName());
								else if(event.getMetricName().equals("SystemCpu"))
									gatheredMetric = generateSystemCpuRecord(queueManager.getQueue(event.getMetricName()), event.getProducerName());
								else if (event.getMetricName().equals("SystemMemory"))
									gatheredMetric = generateSystemMemoryRecord(queueManager.getQueue(event.getMetricName()), event.getProducerName());
								else if (event.getMetricName().equals("TcpConnectionStat"))
									gatheredMetric = generateTcpConnectionStatRecords(queueManager.getQueue(event.getMetricName()), event.getProducerName());
								else if (event.getMetricName().equals("ThreadResource"))
									gatheredMetric = generateThreadResourceRecords(queueManager.getQueue(event.getMetricName()), event.getProducerName());
								else 
									throw new Exception("GatherMetricEvent for unknown metric type:" + event.getMetricName());
							} catch (Exception e) {
								System.err.println("ProcRecordProducer: Caught an exception while gathering metric " + event.getMetricName());
								e.printStackTrace();
								gatheredMetric = false;
							}
							//Now that we've tried to gather the metric, we can remove the event from the schedule, regardless of whether we were successful
							metricSchedule.remove(metricSchedule.first());
							//If we failed to gather the metric...
							if(!gatheredMetric)
								//If the retry interval is less than the periodicity then try again after the retry interval has elapsed
								if (GATHER_METRIC_RETRY_INTERVAL < event.getPeriodicity())
									event.setTargetTime(event.getTargetTime() + GATHER_METRIC_RETRY_INTERVAL);
								//Otherwise, try again after the periodicity has elapsed
								else 
									event.setTargetTime(event.getTargetTime() + event.getPeriodicity());
							//If we successfully gathered the metric
							else {
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
							timeSpentCollectingMetrics += System.currentTimeMillis() - actionStartTime;
							long elapsedTime = System.currentTimeMillis() - tStartTime;
							System.err.println("Running for " + timeSpentCollectingMetrics + " ms out of " + elapsedTime + " ms, " + (100 * timeSpentCollectingMetrics / elapsedTime));
							if(elapsedTime > 60000l) {
								timeSpentCollectingMetrics=0l;
								tStartTime = System.currentTimeMillis();
							}
						}
					}
				}
			}
		}
		//TODO: Do some shutdown stuff here...
	}

	public boolean isValidMetricName(String metricName){
		if(metricName.equals("Diskstat") || metricName.equals("NetworkInterface") || metricName.equals("ProcessResource") ||
				metricName.equals("SystemCpu") || metricName.equals("SystemMemory") || metricName.equals ("TcpConnectionStat") ||
				metricName.equals("ThreadResource") )
			return true;
		return false;
	}
	
	public boolean disableMetric(String metricName, String queueName, int periodicity, int queueCapacity){
		String metricId = metricName + "#" + periodicity + "#" + System.identityHashCode(this);
		synchronized(enabledMetricManager){
			synchronized(queueManager){
				synchronized(metricSchedule){
					if(!enabledMetricManager.containsDescriptor(metricName, queueName, periodicity, queueCapacity))
						return false;
					if(!queueManager.queueExists(metricName))
						return false;
					if(!queueManager.checkForQueueProducer(queueName, metricId))
						return false;
					if(queueManager.getQueueCapacity(queueName) != queueCapacity)
						return false;
					Iterator<GatherMetricEvent> i = metricSchedule.iterator();
					boolean foundEvent=false;
					GatherMetricEvent e = null;
					while(i.hasNext()){
						e = i.next();
						if(e.getMetricName() == metricName && e.getQueueName() == queueName && e.getProducerName() == metricId && e.getPeriodicity() == periodicity){
							foundEvent=true;
							break;
						}
					}
					if(!foundEvent)
						return false;
					metricSchedule.remove(e);
					queueManager.unregisterProducer(queueName, metricId);
					//Note that if there are still consumers registered that the queue won't be deleted.
					//Consumers should try to call deleteQueue when they stop consuming... I think...
					queueManager.deleteQueue(queueName);
					enabledMetricManager.removeDescriptor(metricName, queueName, periodicity, queueCapacity);
				}
			}
		}
		return true;
	}
	//Request this instance of ProcRecordProducer to produce the named type of Record with the specified periodicity and insert the Records into the named RecordQueue
	//This returns as long as the metric will be produced as requested.
	//If for any reason the metric can not be produced as requested, then this throws an exception.
    //When this throws an exception, the caller should presume that the requested metric will NOT be produced.
    //The exception text will explain why the metric couldn't be produced as requested.
	public void enableMetric(String metricName, String queueName, int periodicity, int queueCapacity) throws Exception{
		boolean createdQueue=false;
		
		//Derive the ID to assign to the metric
		//This ID string will show up as the producer name in the output queue
		//The ID is unique to the instance ProcRecordProducer object, the metricName and the metric periodicity
		//The ID is NOT unique to the queueCapacity or the queueName.  The reason for this, if we are already creating the metric at the requested periodicity, we should just force the caller to use the existing output queue where the records are going.
		String metricId = metricName + "#" + periodicity + "#" + System.identityHashCode(this);
		
		//Throw an exception if we were given an invalid periodicity
		if(periodicity < 1)
			throw new Exception("Invalid periodicity:" + periodicity + " - Periodicity for metric must be greater than 0");
			
		//Throw an exception if the name of the metric being requested is not valid
		if(!isValidMetricName(metricName))
			throw new Exception("Invalid metric name " + metricName);
		
		//Synchronize on the enabledMetricManager since we can not allow other threads to enable/disable the same metric concurrently
		synchronized(enabledMetricManager){
			//Return true if the metric is already being gathered
			if(enabledMetricManager.containsDescriptor(metricName, queueName, periodicity, queueCapacity))
				return;
			//If we're here, this is a new metric being requested. (Though the same metric may currently be enabled, for instance, with an alternate periodicity)
			//Synchronize on the name to RecordQueue manager so we can see if the requested output queue already exists, without other threads creating/deleting queues in the mean time
			synchronized(queueManager){
				//Check if the output queue exists
				if(queueManager.queueExists(queueName)){
					//The output queue already exists, check if we can re-use it.
					//Check if it has the desired capacity
					if(queueManager.getQueueCapacity(queueName) != queueCapacity)
						throw new Exception("Requested to create queue " + queueName + " with capacity " + queueCapacity + " but queue already exists with capacity " + queueManager.getQueueCapacity(queueName));
					//Check if there are existing producers for this queue
					//In this case, I'm throwing an exception if something else is already producing into the queue
					//But I'm not sure thats the right behavior
					//I can't immediately discern that there are useful cases where we want this to produce into a queue something else is producing into at the same time
					//I can think of scenarios where people make mistakes in the config and do this accidentally so I think I'd like to default to not allowing it
					//If there is a legitimate need for it later then we will need to adjust this code.
					if(queueManager.getQueueProducers(queueName).length != 0)
						throw new Exception("Requested queue " + queueName + " for metric " + metricName + " already exists with " + queueManager.getQueueProducers(queueName).length + " registered producers");
					//OK, queue already exists with requested capacity and it has no registered producers, we can reuse it.
					//Of course, we did not check if there were existing consumers attached to the queue...
					//A consumer might start getting a different type of record from this queue than it got before, one it doens't know how to understand.
					//Hopefully consumers are written in such a way that they can notice and react to this gracefully somewhere in the call stack...
				//In this case, the queue doesn't exist, so we should create it.
				} else {
					if(!queueManager.createQueue(queueName, queueCapacity, 1))
						throw new Exception("RecordQueueManager failed to create queue " + queueName + " with capacity " + queueCapacity + " and 1 max producer");
					createdQueue = true;
				}
				//Register ProcRecordProducer as the producer for the output RecordQueue
				if(!queueManager.registerProducer(queueName, metricId)){
					//We failed to register with the queue, try to delete it if we created it...
					if(createdQueue)
						if(!queueManager.deleteQueue(queueName)){
							//This is bad, we created a queue a moment ago and are now failing to delete it...
							if(queueManager.queueExists(queueName)){
								//So it exists, we created it, but we can't delete it... log a scary error...
								System.err.println("ERROR: Failed to cleanup queue " + queueName + " while attempting to enable metric " + metricName + ", the RecordQueueManager may be inconsistent.");
								throw new Exception("Failed to enable metric due to unknown internal error");
							}
						}
					throw new Exception("Failed to register as a producer for RecordQueue " + queueName + " with " + queueManager.getQueueProducers(queueName).length + " registered and " + queueManager.getMaxQueueProducers(queueName) + " max producers");
				}
				//Output RecordQueue is created and we are registered as the producer, now we add the metric to the schedule so it can be gathered.
				GatherMetricEvent event = new GatherMetricEvent(0l, 0l, metricName, queueName, metricId, periodicity);
				//Synchronize on metricSchedule since we don't want to allow other things 
				synchronized(metricSchedule){
					metricSchedule.add(event);
				}
				//Mark the metric as enabled
				enabledMetricManager.addDescriptor(metricName, queueName, periodicity, queueCapacity);
				//All done.  Metric is scheduled to be collected, output RecordQueue is created with requested settings and and this is registered as the producer.	
			}
		}
	}

	private boolean generateThreadResourceRecords(RecordQueue outputQueue, String producerName){
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
                                        if(ThreadResourceRecord.produceRecord(outputQueue, producerName, statPath, ppid, clockTick))
                                                outputRecordsGenerated++;
                                }
                        }
                }
        } catch(Exception e){e.printStackTrace();}
        //long dur = System.currentTimeMillis() - st;
        //System.err.println("Generated " + outputRecordsGenerated + " thread output records in " + dur + " ms, " + ((double)outputRecordsGenerated / (double)dur));
        return true;
}
	
	private boolean generateProcessResourceRecords(RecordQueue outputQueue, String producerName) {
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
                        if(ProcessResourceRecord.produceRecord(outputQueue, producerName, pPaths[pn].toString() + "/stat", clockTick))
                                outputRecordsGenerated++;
                }
        } catch (Exception e) {}
        //long dur = System.currentTimeMillis() - st;
        //System.err.println("Generated " + outputRecordsGenerated + " process output records in " + dur + " ms, " + ((double)outputRecordsGenerated / (double)dur));
        return true;
}

	private boolean generateNetworkInterfaceRecords(RecordQueue outputQueue, String producerName) {
		try {
                for (NetworkInterface i : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                        if(i.getName().equals("lo")){
                                continue;
                        }
                        NetworkInterfaceRecord.produceRecord(outputQueue, producerName, i.getName());
                }
        } catch (Exception e) {
                System.err.println("Failed to list network interfaces");
                e.printStackTrace();
                return false;
        }
        return true;
	}

	private boolean generateSystemMemoryRecord(RecordQueue outputQueue, String producerName) {
		return SystemMemoryRecord.produceRecord(outputQueue, producerName);
	}
	
	private boolean generateDiskstatRecords(RecordQueue outputQueue, String producerName) {
		return DiskstatRecord.produceRecords(outputQueue, producerName);
	}

	private boolean generateSystemCpuRecord(RecordQueue outputQueue, String producerName) {
		return SystemCpuRecord.produceRecord(outputQueue, producerName);
	}

	private boolean generateTcpConnectionStatRecords(RecordQueue outputQueue, String producerName) {
		return TcpConnectionStatRecord.produceRecords(outputQueue, producerName);
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
