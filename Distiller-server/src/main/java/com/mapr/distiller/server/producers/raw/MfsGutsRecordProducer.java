package com.mapr.distiller.server.producers.raw;

import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;

public class MfsGutsRecordProducer extends Thread {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(MfsGutsRecordProducer.class);
	
	private class MfsId{
		int pid;
		long starttime;
		
		public boolean equals(MfsId id){
			return ( this.pid == id.pid && this.starttime == id.starttime );
		}
	}
	
	private String producerName;
	private RecordQueue outputQueue, producerStatsQueue;
	private MfsGutsStdoutRecordProducer mfsGutsStdoutRecordProducer;
	boolean shouldExit;
	
	public boolean mfsGutsStdoutRecordProducerIsAlive(){
		return ((mfsGutsStdoutRecordProducer == null) ? false : mfsGutsStdoutRecordProducer.isAlive());
	}
	
	public RecordQueue getOutputQueue(){
		return outputQueue;
	}
	
	public RecordQueue getProducerStatsQueue(){
		return producerStatsQueue;
	}
	
	public boolean producerMetricsEnabled(){
		return (mfsGutsStdoutRecordProducer != null && mfsGutsStdoutRecordProducer.producerMetricsEnabled());
	}
	
	public MfsGutsRecordProducer(RecordQueue outputQueue,String producerName){
		this.outputQueue = outputQueue;
		this.producerName = producerName;
		this.mfsGutsStdoutRecordProducer = null;
		this.producerStatsQueue = null;
		shouldExit=false;
	}
	
	public boolean enableProducerMetrics(RecordQueue producerStatsQueue){
		if(producerStatsQueue!= null){
			this.producerStatsQueue = producerStatsQueue;
			if(mfsGutsStdoutRecordProducer != null){
				return mfsGutsStdoutRecordProducer.enableProducerMetrics(producerStatsQueue);
			}
			return true;
		}
		return false;
	}
	
	public void disableProducerMetrics(){
		if(mfsGutsStdoutRecordProducer != null){
			mfsGutsStdoutRecordProducer.disableProducerMetrics();
		}
	}
	
	public void requestExit(){
		shouldExit=true;
		if(mfsGutsStdoutRecordProducer != null){
			mfsGutsStdoutRecordProducer.requestExit();
		}
	}
	
	private MfsId readMfsIdFromFile(String path) throws Exception {
		RandomAccessFile file = null;
		MfsId mfsId = new MfsId();
		try {
        	file = new RandomAccessFile(path, "r");
        	String line = file.readLine();
        	if(line.split("\\(", 2)[1].split("\\)", 2)[0].equals("mfs")){
        		mfsId.pid = Integer.parseInt(line.split("\\s+", 2)[0]);
        		mfsId.starttime = Integer.parseInt(line.split("\\)", 2)[1].trim().split("\\s+")[19]);
        		return mfsId;
        	} else {
        		throw new Exception("File " + path + " is not a MFS process stat file");
        	}
        } catch (Exception e) {
        	throw new Exception("Failed to read MFS ID from file " + path, e);
        } finally {
        	try{
        		file.close();
        	} catch (Exception e) {}
        }
	}
	
	private static int readIntFromFile(String path) throws Exception {
        RandomAccessFile file = null;
        try {
                file = new RandomAccessFile(path, "r");
                String line = file.readLine();
                return Integer.parseInt(line.trim().split("\\s+")[0]);
        } catch (Exception e) {
                throw new Exception("Caught an exception while reading int file " + path);
        } finally {
                try{
                        file.close();
                } catch (Exception e) {}
        }
	}
	
	public void run(){
		int mfsPid=-1;
		boolean mfsAlive=false, gutsAlive=false;
		long lastTimeMfsDownPrinted=0l, minTime=60000;
		//E.g.:
		// /opt/mapr/bin/guts flush:line time:none cpu:none net:none disk:none ssd:none cleaner:all fs:all kv:all btree:all rpc:db db:all dbrepl:all cache:none log:all resync:all io:small
		ProcessBuilder mfsGutsProcessBuilder = new ProcessBuilder("/opt/mapr/bin/guts", "flush:line", "time:none", "cpu:none", "net:none", "disk:none", "ssd:none", "cleaner:all", "fs:all", "kv:all", "btree:all", "rpc:db", "db:all", "dbrepl:all", "cache:none", "log:all", "resync:all", "io:small");
		
		while(!shouldExit){
			//First, find the PID of MFS, both to make sure it's running (so guts prints proper output) and 
			//so we can monitor for termination of the process since we need to restart guts when that happens
			try {
				mfsPid = readIntFromFile("/opt/mapr/pid/mfs.pid");
			} catch (Exception e) {
				mfsPid = -1;
			}
			if(mfsPid != -1){
				//We have the MFS PID, now check /proc/[pid]/stat to ensure the command name is "(mfs)" and retrieve the start time
				MfsId mfsId = null;
				try {
					mfsId = readMfsIdFromFile("/proc/" + mfsPid + "/stat");
					mfsAlive = true;
				} catch (Exception e){
					if(System.currentTimeMillis() > lastTimeMfsDownPrinted + minTime){
						LOG.info("Found MFS pid " + mfsPid + " from pid file, but could not identify MFS process running from /proc/" + mfsPid + "/stat, will try again...");
						lastTimeMfsDownPrinted = System.currentTimeMillis();
					}
					mfsAlive = false;
				}
				if(mfsAlive && !shouldExit){
					//If MFS is alive, then try to create an MFS guts process...
					Process mfsGutsProcess = null;
				    try {
				    	mfsGutsProcess = mfsGutsProcessBuilder.start();
				    	gutsAlive = true;
				    } catch (Exception e) {
				    	mfsGutsProcess.destroy();
				    	gutsAlive = false;
				    	LOG.error("Failed to launch guts process to monitor MFS, will retry...");
				    }
				    
				    if(gutsAlive){
				    	//If MFS guts is alive, then start processing it's output.
				    	mfsGutsStdoutRecordProducer = new MfsGutsStdoutRecordProducer(mfsGutsProcess.getInputStream(), outputQueue, producerStatsQueue, producerName);
				    	mfsGutsStdoutRecordProducer.start();
					    
					    //Loop once a second until one of the following:
					    // - MFS process no longer exists
					    // - Guts process no longer exists
					    // - Guts process writes to stderr
				    	// - MfsGutsStdoutRecordProducer stops running
					    while(true){
					    	//Check if MFS is running
					    	try {
					    		if(!mfsId.equals(readMfsIdFromFile("/proc/" + mfsPid + "/stat"))){
					    			LOG.warn("MFS process is no longer running with PID " + mfsId.pid + " and starttime " + mfsId.starttime);
					    			break;
					    		}
					    	} catch (Exception e) {
					    		LOG.error("Failed to determine if MFS process is still running with PID " + mfsId.pid + " and starttime " + mfsId.starttime);
					    		break;
					    	}
					    	//Check if MfsGutsStdoutRecordProducer is still running
					    	if(!mfsGutsStdoutRecordProducer.isAlive()){
					    		if(!shouldExit)
					    			LOG.debug("MfsGutsStdoutRecordProducer is not running.");
					    		break;
					    	}
					    	//Check if guts is running
					    	try {
					    		int gutsRetCode = mfsGutsProcess.exitValue();
					    		LOG.info("MFS guts process exited with return code " + gutsRetCode);
					    		break;
					    	} catch (IllegalThreadStateException e) {
					    		//Guts is still running, do nothing...
					    	} catch (Exception e) {
					    		//For any other exception while checking guts, consider it as a failure.
					    		break;
					    	}
					    	//Check if guts logged to stderr
					    	try {
					    		if(mfsGutsProcess.getErrorStream().available() != 0){
					    			LOG.error("Guts threw error");
					    			break;
					    		}
					    	} catch (Exception e) {
					    		break;
					    	}
					    	try {
								Thread.sleep(1000);
							} catch (Exception e){}
					    }
					    //Clean up here, stop/destroy guts process and stdout processor so they can be recreated next iteration.
					    mfsGutsStdoutRecordProducer.requestExit();
					    try {
					    	mfsGutsProcess.getErrorStream().close();
					    } catch (Exception e) {}
					    try {
					    	mfsGutsProcess.getInputStream().close();
					    } catch (Exception e) {}
					    mfsGutsProcess.destroy();
				    } else {
				    	try {
							Thread.sleep(1000);
						} catch (Exception e){}
				    } 
				} else {
					try {
						Thread.sleep(1000);
					} catch (Exception e){}
				}
			}
		}
		if(mfsGutsStdoutRecordProducer!=null){
			LOG.info("MfsGutsRecordProducer." + System.identityHashCode(this) + " is waiting for MfsGutsStdoutRecordProducer to exit");
			while(mfsGutsStdoutRecordProducer.isAlive()){
				try {
					Thread.sleep(1000);
				} catch (Exception e){}
			}
		}
		LOG.info("MfsGutsRecordProducer." + System.identityHashCode(this) + " exited.");
	}
}
