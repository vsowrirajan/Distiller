package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class MfsGutsRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(MfsGutsRecord.class);
	private static final long serialVersionUID = Constants.SVUID_MFS_GUTS_RECORD;
	/**
	 * DERIVED VALUES
	 * These are variables that are not sourced directly from guts
	 * These values are computed only for records returned by calls to a diff function of this class
	 */
	private double 	dbWriteOpsRate, dbReadOpsRate, dbRowWritesRate, dbRowReadsRate, 
					fsWriteOpsRate, fsReadOpsRate, cleanerOpsRate, kvstoreOpsRate, 
					btreeReadOpsRate, btreeWriteOpsRate, rpcRate, pcRate, slowReadsRate, verySlowReadsRate;
	
	/**
	 * RAW VALUES
	 * These are variables whose values are sourced directly from guts
	 */
	private BigInteger 	dbResvFree, logSpaceFree, slowReads, verySlowReads, dbWriteOpsInProgress, 
						dbReadOpsInProgress, dbWriteOpsCompleted, dbReadOpsCompleted, dbRowWritesCompleted, 
						dbRowReadsCompleted, fsWriteOpsCompleted, fsReadOpsCompleted, cleanerOpsCompleted, 
						kvstoreOpsCompleted, btreeReadOpsCompleted, btreeWriteOpsCompleted, rpcCompleted, pcCompleted;
	private String rawLine;
	
	@Override
	public String getRecordType(){
		return Constants.MFS_GUTS_RECORD;
	}
	

	/**
	 * CONSTRUCTORS
	 */
	public MfsGutsRecord(MfsGutsRecord rec1, MfsGutsRecord rec2) throws Exception{
		MfsGutsRecord oldRecord, newRecord;
		
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential MfsGutsRecord from input records with matching timestamp values");

		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		
		if(oldRecord.getPreviousTimestamp()==-1l && newRecord.getPreviousTimestamp()==-1l){
			//Both records are raw records, presume they represent consecutive samples produced by the guts binary
			this.setPreviousTimestamp(oldRecord.getTimestamp());
			this.setTimestamp(newRecord.getTimestamp());
			this.dbResvFree = newRecord.getDbResvFree().multiply(new BigInteger(Long.toString(this.getDurationms())));
			this.logSpaceFree = newRecord.getLogSpaceFree().multiply(new BigInteger(Long.toString(this.getDurationms())));
			this.slowReads = newRecord.getSlowReads();
			this.verySlowReads = newRecord.getVerySlowReads();
			this.dbWriteOpsInProgress = newRecord.getDbWriteOpsInProgress().multiply(new BigInteger(Long.toString(this.getDurationms())));
			this.dbReadOpsInProgress = newRecord.getDbReadOpsInProgress().multiply(new BigInteger(Long.toString(this.getDurationms())));
			this.dbWriteOpsCompleted = newRecord.getDbWriteOpsCompleted();
			this.dbReadOpsCompleted = newRecord.getDbReadOpsCompleted();
			this.dbRowWritesCompleted = newRecord.getDbRowWritesCompleted();
			this.dbRowReadsCompleted = newRecord.getDbRowReadsCompleted();
			this.fsWriteOpsCompleted = newRecord.getFsWriteOpsCompleted();
			this.fsReadOpsCompleted = newRecord.getFsReadOpsCompleted();
			this.cleanerOpsCompleted = newRecord.getCleanerOpsCompleted();
			this.kvstoreOpsCompleted = newRecord.getKvstoreOpsCompleted();
			this.btreeReadOpsCompleted = newRecord.getBtreeReadOpsCompleted();
			this.btreeWriteOpsCompleted = newRecord.getBtreeWriteOpsCompleted();
			this.rpcCompleted = newRecord.getRpcCompleted();
			this.pcCompleted = newRecord.getPcCompleted();
		} else if(oldRecord.getPreviousTimestamp()!=-1l && newRecord.getPreviousTimestamp()!=-1l){
			//Both records are differential
			if(oldRecord.getPreviousTimestamp() == newRecord.getPreviousTimestamp()){
				//Both records are differential, and the previous timestamps are the same.
				//In this case, calculate a differential record based on the time from the timestamp of the old record to the timestap of the new
				this.setPreviousTimestamp(oldRecord.getTimestamp());
				this.setTimestamp(newRecord.getTimestamp());
				this.dbResvFree = newRecord.getDbResvFree().subtract(oldRecord.getDbResvFree());
				this.logSpaceFree = newRecord.getLogSpaceFree().subtract(oldRecord.getLogSpaceFree());
				this.slowReads = newRecord.getSlowReads().subtract(oldRecord.getSlowReads());
				this.verySlowReads = newRecord.getVerySlowReads().subtract(oldRecord.getVerySlowReads());
				this.dbWriteOpsInProgress = newRecord.getDbWriteOpsInProgress().subtract(oldRecord.getDbWriteOpsInProgress());
				this.dbReadOpsInProgress = newRecord.getDbReadOpsInProgress().subtract(oldRecord.getDbReadOpsInProgress());
				this.dbWriteOpsCompleted = newRecord.getDbWriteOpsCompleted().subtract(oldRecord.getDbWriteOpsCompleted());
				this.dbReadOpsCompleted = newRecord.getDbReadOpsCompleted().subtract(oldRecord.getDbReadOpsCompleted());
				this.dbRowWritesCompleted = newRecord.getDbRowWritesCompleted().subtract(oldRecord.getDbRowWritesCompleted());
				this.dbRowReadsCompleted = newRecord.getDbRowReadsCompleted().subtract(oldRecord.getDbRowReadsCompleted());
				this.fsWriteOpsCompleted = newRecord.getFsWriteOpsCompleted().subtract(oldRecord.getFsWriteOpsCompleted());
				this.fsReadOpsCompleted = newRecord.getFsReadOpsCompleted().subtract(oldRecord.getFsReadOpsCompleted());
				this.cleanerOpsCompleted = newRecord.getCleanerOpsCompleted().subtract(oldRecord.getCleanerOpsCompleted());
				this.kvstoreOpsCompleted = newRecord.getKvstoreOpsCompleted().subtract(oldRecord.getKvstoreOpsCompleted());
				this.btreeReadOpsCompleted = newRecord.getBtreeReadOpsCompleted().subtract(oldRecord.getBtreeReadOpsCompleted());
				this.btreeWriteOpsCompleted = newRecord.getBtreeWriteOpsCompleted().subtract(oldRecord.getBtreeWriteOpsCompleted());
				this.rpcCompleted = newRecord.getRpcCompleted().subtract(oldRecord.getRpcCompleted());
				this.pcCompleted = newRecord.getPcCompleted().subtract(oldRecord.getPcCompleted());
			} else if(oldRecord.getTimestamp()==newRecord.getPreviousTimestamp() ){
				//Both records are differential, and they are chronologically consecutive.
				//In this case, merge the records
				this.setPreviousTimestamp(oldRecord.getPreviousTimestamp());
				this.setTimestamp(newRecord.getTimestamp());
				this.dbResvFree = oldRecord.getDbResvFree().add(newRecord.getDbResvFree());
				this.logSpaceFree = oldRecord.getLogSpaceFree().add(newRecord.getLogSpaceFree());
				this.slowReads = oldRecord.getSlowReads().add(newRecord.getSlowReads());
				this.verySlowReads = oldRecord.getVerySlowReads().add(newRecord.getVerySlowReads());
				this.dbWriteOpsInProgress = oldRecord.getDbWriteOpsInProgress().add(newRecord.getDbWriteOpsInProgress());
				this.dbReadOpsInProgress = oldRecord.getDbReadOpsInProgress().add(newRecord.getDbReadOpsInProgress());
				this.dbWriteOpsCompleted = oldRecord.getDbWriteOpsCompleted().add(newRecord.getDbWriteOpsCompleted());
				this.dbReadOpsCompleted = oldRecord.getDbReadOpsCompleted().add(newRecord.getDbReadOpsCompleted());
				this.dbRowWritesCompleted = oldRecord.getDbRowWritesCompleted().add(newRecord.getDbRowWritesCompleted());
				this.dbRowReadsCompleted = oldRecord.getDbRowReadsCompleted().add(newRecord.getDbRowReadsCompleted());
				this.fsWriteOpsCompleted = oldRecord.getFsWriteOpsCompleted().add(newRecord.getFsWriteOpsCompleted());
				this.fsReadOpsCompleted = oldRecord.getFsReadOpsCompleted().add(newRecord.getFsReadOpsCompleted());
				this.cleanerOpsCompleted = oldRecord.getCleanerOpsCompleted().add(newRecord.getCleanerOpsCompleted());
				this.kvstoreOpsCompleted = oldRecord.getKvstoreOpsCompleted().add(newRecord.getKvstoreOpsCompleted());
				this.btreeReadOpsCompleted = oldRecord.getBtreeReadOpsCompleted().add(newRecord.getBtreeReadOpsCompleted());
				this.btreeWriteOpsCompleted = oldRecord.getBtreeWriteOpsCompleted().add(newRecord.getBtreeWriteOpsCompleted());
				this.rpcCompleted = oldRecord.getRpcCompleted().add(newRecord.getRpcCompleted());
				this.pcCompleted = oldRecord.getPcCompleted().add(newRecord.getPcCompleted());
			} else {
				throw new Exception("Can not compare MfsGutsRecords with timestamps: (" + oldRecord.getPreviousTimestamp() + 
										"," + oldRecord.getTimestamp() + ") (" + newRecord.getPreviousTimestamp() + 
										"," + newRecord.getTimestamp() + ")");
			}
		} else if(oldRecord.getPreviousTimestamp()!=-1l && newRecord.getPreviousTimestamp()==-1l){
			//The old record is a differential while the new record is a raw record. 
			//Presume the raw record covers the time from the timestamp of the old record to the timestamp of the new record
			this.setPreviousTimestamp(oldRecord.getPreviousTimestamp());
			this.setTimestamp(newRecord.getTimestamp());
			this.dbResvFree = oldRecord.getDbResvFree().add(newRecord.getDbResvFree().multiply(new BigInteger(Long.toString(newRecord.getTimestamp() - oldRecord.getTimestamp()))));
			this.logSpaceFree = oldRecord.getLogSpaceFree().add(newRecord.getLogSpaceFree().multiply(new BigInteger(Long.toString(newRecord.getTimestamp() - oldRecord.getTimestamp()))));
			this.slowReads = oldRecord.getSlowReads().add(newRecord.getSlowReads());
			this.verySlowReads = oldRecord.getVerySlowReads().add(newRecord.getVerySlowReads());
			this.dbWriteOpsInProgress = oldRecord.getDbWriteOpsInProgress().add(newRecord.getDbWriteOpsInProgress().multiply(new BigInteger(Long.toString(newRecord.getTimestamp() - oldRecord.getTimestamp()))));
			this.dbReadOpsInProgress = oldRecord.getDbReadOpsInProgress().add(newRecord.getDbReadOpsInProgress().multiply(new BigInteger(Long.toString(newRecord.getTimestamp() - oldRecord.getTimestamp()))));
			this.dbWriteOpsCompleted = oldRecord.getDbWriteOpsCompleted().add(newRecord.getDbWriteOpsCompleted());
			this.dbReadOpsCompleted = oldRecord.getDbReadOpsCompleted().add(newRecord.getDbReadOpsCompleted());
			this.dbRowWritesCompleted = oldRecord.getDbRowWritesCompleted().add(newRecord.getDbRowWritesCompleted());
			this.dbRowReadsCompleted = oldRecord.getDbRowReadsCompleted().add(newRecord.getDbRowReadsCompleted());
			this.fsWriteOpsCompleted = oldRecord.getFsWriteOpsCompleted().add(newRecord.getFsWriteOpsCompleted());
			this.fsReadOpsCompleted = oldRecord.getFsReadOpsCompleted().add(newRecord.getFsReadOpsCompleted());
			this.cleanerOpsCompleted = oldRecord.getCleanerOpsCompleted().add(newRecord.getCleanerOpsCompleted());
			this.kvstoreOpsCompleted = oldRecord.getKvstoreOpsCompleted().add(newRecord.getKvstoreOpsCompleted());
			this.btreeReadOpsCompleted = oldRecord.getBtreeReadOpsCompleted().add(newRecord.getBtreeReadOpsCompleted());
			this.btreeWriteOpsCompleted = oldRecord.getBtreeWriteOpsCompleted().add(newRecord.getBtreeWriteOpsCompleted());
			this.rpcCompleted = oldRecord.getRpcCompleted().add(newRecord.getRpcCompleted());
			this.pcCompleted = oldRecord.getPcCompleted().add(newRecord.getPcCompleted());
		} else {
			throw new Exception("Can not compare MfsGutsRecords with timestamps: (" + oldRecord.getPreviousTimestamp() + 
									"," + oldRecord.getTimestamp() + ") (" + newRecord.getPreviousTimestamp() + 
									"," + newRecord.getTimestamp() + ")");
		}
	
		this.dbWriteOpsRate = this.dbWriteOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.dbReadOpsRate = this.dbReadOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.dbRowWritesRate = this.dbRowWritesCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.dbRowReadsRate = this.dbRowReadsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.fsWriteOpsRate = this.fsWriteOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.fsReadOpsRate = this.fsReadOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.cleanerOpsRate = this.cleanerOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.kvstoreOpsRate = this.kvstoreOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.btreeReadOpsRate = this.btreeReadOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.btreeWriteOpsRate = this.btreeWriteOpsCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.rpcRate = this.rpcCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.pcRate = this.pcCompleted.doubleValue() * 1000d / (double)this.getDurationms();
		this.slowReadsRate = this.slowReads.doubleValue() * 1000d / (double)this.getDurationms();
		this.verySlowReadsRate = this.verySlowReads.doubleValue() * 1000d / (double)this.getDurationms();
	}
	public MfsGutsRecord(String line, int version) throws Exception {
		super(System.currentTimeMillis());
		this.dbWriteOpsRate=-1d;
		this.dbReadOpsRate=-1d;
		this.dbRowWritesRate=-1d;
		this.dbRowReadsRate=-1d;
		this.fsWriteOpsRate=-1d;
		this.fsReadOpsRate=-1d;
		this.cleanerOpsRate=-1d;
		this.kvstoreOpsRate=-1d;
		this.btreeReadOpsRate=-1d;
		this.btreeWriteOpsRate=-1d;
		this.rpcRate=-1d;
		this.pcRate=-1d;
		this.rawLine = null;
		
		if(version==-1){
			this.rawLine = line;
		} else if(version==0){
			String[] parts = line.trim().split("\\s+");
			
			//Guts fields: rsf
	        this.dbResvFree = new BigInteger(parts[17]);

	        //Guts fields: ls
	        this.logSpaceFree = new BigInteger(parts[131]);

	        //Guts fields: sr
	        this.slowReads = new BigInteger(parts[85]);

	        //Guts fields: vsr
	        this.verySlowReads = new BigInteger(parts[88]);

	        //Guts fields: cput ccom cinc
	        this.dbWriteOpsInProgress = (new BigInteger(parts[16])).add(new BigInteger(parts[25])).add(new BigInteger(parts[42]));

	        //Guts fields: cget csc cinc ctlk
	        this.dbReadOpsInProgress = (new BigInteger(parts[5])).add(new BigInteger(parts[30])).add(new BigInteger(parts[42])).add(new BigInteger(parts[46]));

	        //Guts fields: rput bucketWr(0) fl ffl sfl mcom fcom scr spcr rinc rchk rapp rbulkb rbulks rip rop rob
	        this.dbWriteOpsCompleted = (new BigInteger(parts[13])).add(new BigInteger(parts[18])).add(new BigInteger(parts[20])).add(new BigInteger(parts[21])).add(new BigInteger(parts[22])).add(new BigInteger(parts[23])).add(new BigInteger(parts[24])).add(new BigInteger(parts[26])).add(new BigInteger(parts[27])).add(new BigInteger(parts[41])).add(new BigInteger(parts[43])).add(new BigInteger(parts[44])).add(new BigInteger(parts[47])).add(new BigInteger(parts[48])).add(new BigInteger(parts[50])).add(new BigInteger(parts[52])).add(new BigInteger(parts[53]));

	        //Guts fields: rget tgetR bget sg spg rsc bsc ssc spsc ldbr blkr raSg raSp nAdv raBl rinc rchk rtlk
	        this.dbReadOpsCompleted = (new BigInteger(parts[2])).add(new BigInteger(parts[4])).add(new BigInteger(parts[9])).add(new BigInteger(parts[10])).add(new BigInteger(parts[11])).add(new BigInteger(parts[28])).add(new BigInteger(parts[31])).add(new BigInteger(parts[32])).add(new BigInteger(parts[33])).add(new BigInteger(parts[35])).add(new BigInteger(parts[36])).add(new BigInteger(parts[37])).add(new BigInteger(parts[38])).add(new BigInteger(parts[39])).add(new BigInteger(parts[40])).add(new BigInteger(parts[41])).add(new BigInteger(parts[43])).add(new BigInteger(parts[45]));

	        //Guts fields: rputR tputR
	        this.dbRowWritesCompleted = (new BigInteger(parts[14])).add(new BigInteger(parts[15]));

	        //Guts fields: rgetR rscR spscR
	        this.dbRowReadsCompleted = (new BigInteger(parts[3])).add(new BigInteger(parts[29])).add(new BigInteger(parts[34]));

	        //Guts fields: rpc
	        this.rpcCompleted = new BigInteger(parts[0]);

	        //Guts fields: rpc lpc
	        this.pcCompleted = (new BigInteger(parts[0])).add(new BigInteger(parts[1]));

	        //Guts fields: bin bde bre bsp bme bco
	        this.btreeWriteOpsCompleted = (new BigInteger(parts[140])).add(new BigInteger(parts[141])).add(new BigInteger(parts[142])).add(new BigInteger(parts[143])).add(new BigInteger(parts[144])).add(new BigInteger(parts[145]));

	        //Guts fields: blu
	        this.btreeReadOpsCompleted = new BigInteger(parts[139]);

	        //Guts fields: ikv rkv
	        this.kvstoreOpsCompleted = (new BigInteger(parts[106])).add(new BigInteger(parts[107]));

	        //Guts fields: qi qd bq aq di ic dd dc dm mc bc fc
	        this.cleanerOpsCompleted = (new BigInteger(parts[108])).add(new BigInteger(parts[109])).add(new BigInteger(parts[110])).add(new BigInteger(parts[111])).add(new BigInteger(parts[112])).add(new BigInteger(parts[113])).add(new BigInteger(parts[114])).add(new BigInteger(parts[115])).add(new BigInteger(parts[116])).add(new BigInteger(parts[117])).add(new BigInteger(parts[118])).add(new BigInteger(parts[119]));

	        //Guts fields: lkp ga rd rdp rl r
	        this.fsReadOpsCompleted = (new BigInteger(parts[58])).add(new BigInteger(parts[63])).add(new BigInteger(parts[65])).add(new BigInteger(parts[66])).add(new BigInteger(parts[67])).add(new BigInteger(parts[68]));

	        //Guts fields: cr cdi rdi sl vl ul dvl sa w sf td ksi ksd ltxn
	        this.fsWriteOpsCompleted = (new BigInteger(parts[55])).add(new BigInteger(parts[56])).add(new BigInteger(parts[57])).add(new BigInteger(parts[59])).add(new BigInteger(parts[60])).add(new BigInteger(parts[61])).add(new BigInteger(parts[62])).add(new BigInteger(parts[64])).add(new BigInteger(parts[69])).add(new BigInteger(parts[70])).add(new BigInteger(parts[71])).add(new BigInteger(parts[72])).add(new BigInteger(parts[73])).add(new BigInteger(parts[134]));		

		} else if (version==1){
			String[] parts = line.trim().split("\\s+");
			
			//Guts fields: rsf
	        this.dbResvFree = new BigInteger(parts[17]);

	        //Guts fields: ls
	        this.logSpaceFree = new BigInteger(parts[131]);

	        //Guts fields: sr
	        this.slowReads = new BigInteger(parts[85]);

	        //Guts fields: vsr
	        this.verySlowReads = new BigInteger(parts[88]);

	        //Guts fields: cput ccom cinc
	        this.dbWriteOpsInProgress = (new BigInteger(parts[16])).add(new BigInteger(parts[25])).add(new BigInteger(parts[42]));

	        //Guts fields: cget csc cinc ctlk
	        this.dbReadOpsInProgress = (new BigInteger(parts[5])).add(new BigInteger(parts[30])).add(new BigInteger(parts[42])).add(new BigInteger(parts[46]));

	        //Guts fields: rput bucketWr(0) fl ffl sfl mcom fcom scr spcr rinc rchk rapp rbulkb rbulks rip rop rob
	        this.dbWriteOpsCompleted = (new BigInteger(parts[13])).add(new BigInteger(parts[18])).add(new BigInteger(parts[20])).add(new BigInteger(parts[21])).add(new BigInteger(parts[22])).add(new BigInteger(parts[23])).add(new BigInteger(parts[24])).add(new BigInteger(parts[26])).add(new BigInteger(parts[27])).add(new BigInteger(parts[41])).add(new BigInteger(parts[43])).add(new BigInteger(parts[44])).add(new BigInteger(parts[47])).add(new BigInteger(parts[48])).add(new BigInteger(parts[50])).add(new BigInteger(parts[52])).add(new BigInteger(parts[53]));

	        //Guts fields: rget tgetR bget sg spg rsc bsc ssc spsc ldbr blkr raSg raSp nAdv raBl rinc rchk rtlk
	        this.dbReadOpsCompleted = (new BigInteger(parts[2])).add(new BigInteger(parts[4])).add(new BigInteger(parts[9])).add(new BigInteger(parts[10])).add(new BigInteger(parts[11])).add(new BigInteger(parts[28])).add(new BigInteger(parts[31])).add(new BigInteger(parts[32])).add(new BigInteger(parts[33])).add(new BigInteger(parts[35])).add(new BigInteger(parts[36])).add(new BigInteger(parts[37])).add(new BigInteger(parts[38])).add(new BigInteger(parts[39])).add(new BigInteger(parts[40])).add(new BigInteger(parts[41])).add(new BigInteger(parts[43])).add(new BigInteger(parts[45]));

	        //Guts fields: rputR tputR
	        this.dbRowWritesCompleted = (new BigInteger(parts[14])).add(new BigInteger(parts[15]));

	        //Guts fields: rgetR rscR spscR
	        this.dbRowReadsCompleted = (new BigInteger(parts[3])).add(new BigInteger(parts[29])).add(new BigInteger(parts[34]));

	        //Guts fields: rpc
	        this.rpcCompleted = new BigInteger(parts[0]);

	        //Guts fields: rpc lpc
	        this.pcCompleted = (new BigInteger(parts[0])).add(new BigInteger(parts[1]));

	        //Guts fields: bin bde bre bsp bme bco
	        this.btreeWriteOpsCompleted = (new BigInteger(parts[143])).add(new BigInteger(parts[144])).add(new BigInteger(parts[145])).add(new BigInteger(parts[146])).add(new BigInteger(parts[147])).add(new BigInteger(parts[148]));

	        //Guts fields: blu
	        this.btreeReadOpsCompleted = new BigInteger(parts[142]);

	        //Guts fields: ikv rkv lkv kln skv
	        this.kvstoreOpsCompleted = (new BigInteger(parts[106])).add(new BigInteger(parts[107])).add(new BigInteger(parts[108])).add(new BigInteger(parts[109])).add(new BigInteger(parts[110]));

	        //Guts fields: qi qd bq aq di ic dd dc dm mc bc fc
	        this.cleanerOpsCompleted = (new BigInteger(parts[108])).add(new BigInteger(parts[112])).add(new BigInteger(parts[113])).add(new BigInteger(parts[114])).add(new BigInteger(parts[115])).add(new BigInteger(parts[116])).add(new BigInteger(parts[117])).add(new BigInteger(parts[118])).add(new BigInteger(parts[119])).add(new BigInteger(parts[120])).add(new BigInteger(parts[121])).add(new BigInteger(parts[122]));

	        //Guts fields: lkp ga rd rdp rl r
	        this.fsReadOpsCompleted = (new BigInteger(parts[58])).add(new BigInteger(parts[63])).add(new BigInteger(parts[65])).add(new BigInteger(parts[66])).add(new BigInteger(parts[67])).add(new BigInteger(parts[68]));

	        //Guts fields: cr cdi rdi sl vl ul dvl sa w sf td ksi ksd ltxn
	        this.fsWriteOpsCompleted = (new BigInteger(parts[55])).add(new BigInteger(parts[56])).add(new BigInteger(parts[57])).add(new BigInteger(parts[59])).add(new BigInteger(parts[60])).add(new BigInteger(parts[61])).add(new BigInteger(parts[62])).add(new BigInteger(parts[64])).add(new BigInteger(parts[69])).add(new BigInteger(parts[70])).add(new BigInteger(parts[71])).add(new BigInteger(parts[72])).add(new BigInteger(parts[73])).add(new BigInteger(parts[137]));		

		} else {
			throw new Exception("Unknown MFS guts version: " + version);
		}
	}
	
	/**
	 * PRODUCE RECORD METHODS
	 */
	public static int[] produceRecord(RecordQueue outputQueue, String producerName, String line, int version){
		int[] ret = new int[] {0, 0, 0, 0};
		MfsGutsRecord rec = null;
		try{
			rec = new MfsGutsRecord(line, version);
		} catch (Exception e) {
			LOG.error("Failed to generate a MfsGutsRecord");
			e.printStackTrace();
			ret[2]=1;
		}
		if(rec != null && !outputQueue.put(producerName, rec)){
			ret[3] = 1;
			LOG.error("Failed to put MfsGutsRecord into output queue " + outputQueue.getQueueName() + 
					" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.getQueueRecordCapacity() + 
					" producerName:" + producerName);
		} else {
			ret[1] = 1;
		}
		return ret;
	}

	/**
	 * OTHER METHODS
	 */
 	public String toString(){
		return super.toString() + " MfsGuts rsf:" + dbResvFree + " lsf:" + logSpaceFree + " fsro:" + fsReadOpsCompleted + " fswo:" + fsWriteOpsCompleted + 
									" btro:" + btreeReadOpsCompleted + " btwo:" + btreeWriteOpsCompleted;
	}

 	public BigInteger getDbResvFree(){
 		return dbResvFree;
 	}
 	
 	public BigInteger getLogSpaceFree(){
 		return logSpaceFree;
 	}

 	public BigInteger getSlowReads(){
 		return slowReads;
 	}

	public BigInteger getVerySlowReads(){
		return verySlowReads;
	}

	public BigInteger getDbWriteOpsInProgress(){
		return dbWriteOpsInProgress;
	}

	public BigInteger getDbReadOpsInProgress(){
		return dbReadOpsInProgress;
	}

	public BigInteger getDbWriteOpsCompleted(){
		return dbWriteOpsCompleted;
	}

	public BigInteger getDbReadOpsCompleted(){
		return dbReadOpsCompleted;
	}

	public BigInteger getDbRowWritesCompleted(){
		return dbRowWritesCompleted;
	}

	public BigInteger getDbRowReadsCompleted(){
		return dbRowReadsCompleted;
	}

	public BigInteger getFsWriteOpsCompleted(){
		return fsWriteOpsCompleted;
	}

	public BigInteger getFsReadOpsCompleted(){
		return fsReadOpsCompleted;
	}

	public BigInteger getCleanerOpsCompleted(){
		return cleanerOpsCompleted;
	}

	public BigInteger getKvstoreOpsCompleted(){
		return kvstoreOpsCompleted;
	}

	public BigInteger getBtreeReadOpsCompleted(){
		return btreeReadOpsCompleted;
	}

	public BigInteger getBtreeWriteOpsCompleted(){
		return btreeWriteOpsCompleted;
	}

	public BigInteger getRpcCompleted(){
		return rpcCompleted;
	}

	public BigInteger getPcCompleted(){
		return pcCompleted;
	}
	
	public double getDbWriteOpsRate(){
		return dbWriteOpsRate;
	}

	public double getDbReadOpsRate(){
		return dbReadOpsRate;
	}

	public double getDbRowWritesRate(){
		return dbRowWritesRate;
	}

	public double getDbRowReadsRate(){
		return dbRowReadsRate;
	}

	public double getFsWriteOpsRate(){
		return fsWriteOpsRate;
	}

	public double getFsReadOpsRate(){
		return fsReadOpsRate;
	}

	public double getCleanerOpsRate(){
		return cleanerOpsRate;
	}

	public double getKvstoreOpsRate(){
		return kvstoreOpsRate;
	}

	public double getBtreeReadOpsRate(){
		return btreeReadOpsRate;
	}

	public double getBtreeWriteOpsRate(){
		return btreeWriteOpsRate;
	}

	public double getRpcRate(){
		return rpcRate;
	}

	public double getPcRate(){
		return pcRate;
	}

	public double getSlowReadsRate(){
		return slowReadsRate;
	}

	public double getVerySlowReadsRate(){
		return verySlowReadsRate;
	}

}
