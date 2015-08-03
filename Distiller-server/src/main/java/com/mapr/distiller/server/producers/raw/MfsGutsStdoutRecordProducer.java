package com.mapr.distiller.server.producers.raw;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.queues.RecordQueueManager;
import com.mapr.distiller.server.recordtypes.MfsGutsRecord;
import com.mapr.distiller.server.recordtypes.RawRecordProducerStatusRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.regex.Pattern;

public class MfsGutsStdoutRecordProducer extends Thread{
	String producerName;
	RecordQueue outputQueue, producerStatsQueue;
	BufferedReader stdout;
	boolean shouldExit, producerMetricsEnabled;
	
	//Used to generate metrics about what the ProcRecordProducer is doing
	private RawRecordProducerStatusRecord mystatus;
		
	//The interval for reporting status on what the ProcRecordProducer is doing (e.g. put the mystatus record into the output queue and start on a new mystatus record);
	//Perhaps this needs to be configurable
	private static int statusIntervalSeconds=5;
	
	public MfsGutsStdoutRecordProducer(InputStream stdout, RecordQueue outputQueue, RecordQueue producerStatsQueue, String producerName){
		this.stdout = new BufferedReader(new InputStreamReader(stdout));
		this.outputQueue = outputQueue;
		this.producerName = producerName;
		this.producerStatsQueue = producerStatsQueue;
		if(producerStatsQueue != null){
			producerMetricsEnabled = true;
		} else {
			producerMetricsEnabled=false;
		}
		this.shouldExit = false;
	}

	public boolean enableProducerMetrics(RecordQueue producerStatsQueue){
		if(!producerMetricsEnabled && producerStatsQueue!= null){
			this.producerStatsQueue = producerStatsQueue;
			this.producerMetricsEnabled = true;
			return true;
		}
		return false;
	}
	
	public boolean producerMetricsEnabled(){
		return producerMetricsEnabled;
	}
	
	public void disableProducerMetrics(){
		this.producerMetricsEnabled=false;
	}
	
	public void requestExit(){
		shouldExit=true;
		try {
			stdout.close();
		} catch (Exception e) {}
	}
	
	public boolean isExpectedHeader(String line){
		if (line.equals("  rpc   lpc  rget  rgetR  tgetR  cget   vcM   vcL   vcH  bget    sg   spg  bskp  rput  rputR  tputR  cput   rsf  bucketWr  fl  ffl  sfl mcom fcom ccom  scr spcr   rsc    rscR  csc   bsc   ssc  spsc    spscR  ldbr  blkr  raSg  raSp  nAdv  raBl  rinc  cinc  rchk  rapp  rtlk  ctlk rbulkb rbulks rim   rip rom   rop rob rocm  cr  cdi  rdi  lkp   sl   vl   ul  dvl   ga   sa   rd  rdp   rl    r    w   sf   td  ksi  ksd   hb    cm  chit  ra   cp   uc   cf  ras  ucs  ses   pr   sr   ow   rr  vsr  raN  raM  raL  raJ  adU   shmem     write   lwrite   bwrite      read    lread  ikv  rkv  qi qd bq aq    di  ic    dd dc   dm mc bc  fc   bl flu icc  of edf       ior      iow   lf lff ls ilw lsc ltxn gs   bch   bcm   blb   blu  bin  bde bre bsp bme bco   rc  irc   brd  nrr  nbr  tbr thc  thtm  msgs   kbs res msgr  kbr "))
			return true;
		return false;
	}
	
	private boolean generateMfsGutsRecord(String line) {
		int[] ret = MfsGutsRecord.produceRecord(outputQueue, producerName, line);
		if(ret[0]==0){
			mystatus.setRecordsCreated(mystatus.getRecordsCreated() + ret[1]);
			mystatus.setRecordCreationFailures(mystatus.getRecordCreationFailures() + ret[2]);
			mystatus.setQueuePutFailures(mystatus.getQueuePutFailures() + ret[3]);
			return true;
		} else {
			mystatus.setOtherFailures(mystatus.getOtherFailures() + 1);;
			return false;
		}
	}
	
	public boolean isHeader(String line){
		return line.matches(".*[a-z].*");
	}
	
	public boolean isSample(String line){
		return !isHeader(line) && Pattern.matches(".*[0-9].*", line);
	}
	
	public void run() {
		
		mystatus = new RawRecordProducerStatusRecord(producerName);
		
		boolean verifiedHeader = false;
		while(!shouldExit){
			//Report self metrics
			if( ((System.currentTimeMillis() - mystatus.getTimestamp()) / 1000l) >= statusIntervalSeconds ){
				RawRecordProducerStatusRecord newRecord = null;
				try {
					newRecord = new RawRecordProducerStatusRecord(mystatus);
				} catch (Exception e){
					System.err.println("Failed to generate a RecordProducerStatusRecord");
					e.printStackTrace();
					newRecord = new RawRecordProducerStatusRecord(producerName);
				}
				if(producerMetricsEnabled && !producerStatsQueue.put(producerName,mystatus)){
						System.err.println("Failed to put RecordProducerStatusRecord to output queue " + producerStatsQueue.getQueueName() + 
												" size:" + producerStatsQueue.queueSize() + " maxSize:" + producerStatsQueue.maxQueueSize() + 
												" producerName:" + producerName);
				} 
				mystatus = newRecord;
			}
			String line = null;
			try {
				line = stdout.readLine();
			} catch (Exception e) {
				System.err.println("ERROR: Could not read from MFS guts stdout");
				break;
			}
			long startTime = System.currentTimeMillis();
			if(line != null){
				if(isHeader(line)){
					//This is all a hack... hard coded header and all.  See MapR bug 19735.
					if(!verifiedHeader){
						if(isExpectedHeader(line)){
							verifiedHeader = true;
						} else {
							System.err.println("ERROR: MFS guts did not produce the expected header");
							break;
						}
					}
				} else if(isSample(line)){
					if(verifiedHeader) {
						try {
							generateMfsGutsRecord(line);
						} catch (Exception e){
							System.err.println("Failed to produce a MfsGutsRecord");
							e.printStackTrace();
							break;
						}
					} else {
						//Somehow, we have a sample line but no header line was found.
						//We can't parse a sample line without knowing the header.
						break;
					}
				}
				//else skip it, because guts prints blank lines for whatever reason...
			} else {
				System.err.println("ERROR: Could not read from MFS guts stdout.");
				break;
			}
			
			mystatus.setRunningTimems(mystatus.getRunningTimems() + System.currentTimeMillis() - startTime);
			
		}
		try {
			stdout.close();
		} catch (Exception e){}
	}
}