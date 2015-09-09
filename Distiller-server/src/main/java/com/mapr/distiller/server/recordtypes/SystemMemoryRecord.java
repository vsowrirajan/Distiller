package com.mapr.distiller.server.recordtypes;

import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class SystemMemoryRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(SystemMemoryRecord.class);
	private static final long serialVersionUID = Constants.SVUID_SYSTEM_MEMORY_RECORD;
	/**
	 * A general note about this class, if the amount of memory or swap space changes while this process is running, some stats can be inaccurate until the process is restarted.  
	 */
	
	/**
	 * DERIVED VALUES
	 * These are variables that are not sourced directly from /proc
	 * These values are computed only for records returned by calls to a diff function of this class
	 */
	private double freeMemPct;
	private BigInteger freeMemByteMilliseconds;
	
	@Override
	public String getRecordType(){
		return Constants.SYSTEM_MEMORY_RECORD;
	}
	

	/**
	 * RAW VALUES
	 * These are variables whose values are sourced directly from /proc when produceRecord is called
	 */
	//Values from /proc/meminfo
	private BigInteger MemTotal, MemFree, Buffers, Cached, SwapCached, Active,
				Inactive, Active_anon_, Inactive_anon_, Active_file_, Inactive_file_,
				Unevictable, Mlocked, SwapTotal, SwapFree, Dirty, Writeback,
				AnonPages, Mapped, Shmem, Slab, SReclaimable, SUnreclaim,
				KernelStack, PageTables, NFS_Unstable, Bounce, WritebackTmp, 
				CommitLimit, Committed_AS, VmallocTotal, VmallocUsed, VmallocChunk,
				HardwareCorrupted, AnonHugePages, HugePages_Total, HugePages_Free, 
				HugePages_Rsvd, HugePages_Surp, Hugepagesize, DirectMap4k, DirectMap2M,
				DirectMap1G;
	//Values read from /proc/vmstat
	private BigInteger nr_dirty, pswpin, pswpout, pgmajfault, allocstall;

	/**
	 * CONSTRUCTORS
	 */
	public SystemMemoryRecord() throws Exception {
		super(System.currentTimeMillis());
		this.freeMemPct=-1d;
		this.freeMemByteMilliseconds=null;
		String[] parts;
		String line;
		RandomAccessFile proc_meminfo = null, proc_vmstat = null;
		try{
			proc_meminfo = new RandomAccessFile("/proc/meminfo", "r");
			proc_vmstat = new RandomAccessFile("/proc/vmstat", "r");
			while ( (line = proc_meminfo.readLine()) != null) {
				parts = line.trim().split("\\s+");
				BigInteger val = null;
				if(parts.length >= 2){
					val = new BigInteger(parts[1]);
					if(parts.length == 3){
						if(	parts[2].equals("kB") || 
							parts[2].equals("k") || 
							parts[2].equals("K") || 
							parts[2].equals("KB") ) {
							val = val.multiply(new BigInteger("1024"));
						} else if( 	parts[2].equals("mB") ||
									parts[2].equals("MB") ||
									parts[2].equals("m") ||
									parts[2].equals("M") ) {
							val = val.multiply(new BigInteger("1048576"));
						} else {
							throw new Exception("Unknown unit type in /proc/meminfo " + parts[2]);
						}
					} else if (parts.length > 3) {
						throw new Exception("Expecting lines with 2 or 3 fields in /proc/meminfo but found " + parts.length);
					}
					setValueByName(parts[0].split(":")[0], val);
				} else {
					throw new Exception("Expecting lines with 2 or 3 fields in /proc/meminfo but found " + parts.length);
				}
			}
			while ( (line = proc_vmstat.readLine()) != null) {
				parts = line.trim().split("\\s+");
				BigInteger val = null;
				if(parts.length == 2){
					val = new BigInteger(parts[1]);
					setValueByName(parts[0], val);
				} else {
					throw new Exception("Expecting lines with 2 fields in /proc/vmstat but found " + parts.length);
				}
			}
		} catch (Exception e) {	
			throw new Exception("Failed to generate SystemMemoryRecord", e);
		} finally {
			try {
				proc_meminfo.close();
			} catch (Exception e){}
			try {
				proc_vmstat.close();
			} catch (Exception e){}
		}
	}
	public SystemMemoryRecord(SystemMemoryRecord rec1, SystemMemoryRecord rec2) throws Exception {
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential SystemMemoryRecord from input records with matching timestamp values");

		//Figure out the time sequence of the records
		SystemMemoryRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		
		//Check if these are raw SystemMemoryRecords
		if(rec1.getFreeMemPct()==-1d && rec2.getFreeMemPct()==-1d){
			//Check if any counters have rolled over, if so, skip this record.
			if (newRecord.get_pgmajfault().compareTo(oldRecord.get_pgmajfault()) == -1 ||
				newRecord.get_pswpin().compareTo(oldRecord.get_pswpin()) == -1 ||
				newRecord.get_pswpout().compareTo(oldRecord.get_pswpout()) == -1 ||
				newRecord.get_allocstall().compareTo(oldRecord.get_allocstall()) == -1 )
			{
				throw new Exception("Can not generate differential record from raw input records where counters have rolled over between samples. " + 
						" old: " + oldRecord.toString() + " new: " + newRecord.toString());
			}
			
			//Copied values
			setTimestamp(newRecord.getTimestamp());				//Set the end timestamp for this record to the timestamp of the newer record
			setPreviousTimestamp(oldRecord.getTimestamp());		//Set the start timestamp for this record to the timestamp of the older record.
			this.MemTotal = newRecord.getMemTotal();			//Copy the MemTotal from either input record, presume it doesn't change while the system is still running, this should really be in a separate class...
			this.SwapTotal = newRecord.getSwapTotal();			//Copy the SwapTotal from either input record with the same caveat as mentioned for MemTotal
			
			//These values are set as the mean value between the two input records.  These are range-bounded values.
			//The upper bound on these values is relative to MemTotal and the lowerbound is 0.  
			//With raw SystemMemoryRecords, the value of these fields represents the in-range value for the given metric observed at the point in time at which the raw record was generated.
			//We know the value in the range it was at for the start (oldRecord) and for the end (newRecord) so lets presume it uses the mean amount between those over this period of time.
			//That is of course just speculation, memory usage in Linux is not tracked the way CPU usage is tracked (e.g. through jiffy counters), so this is the best we can do until/unless the kernel supports better.
			this.MemFree = newRecord.getMemFree().add(oldRecord.getMemFree()).divide(new BigInteger("2"));		
			this.SwapFree = newRecord.getSwapFree().add(oldRecord.getSwapFree()).divide(new BigInteger("2"));	
			this.nr_dirty = newRecord.get_nr_dirty().add(oldRecord.get_nr_dirty()).divide(new BigInteger("2"));
			
			//In raw SystemMemoryRecords, these fields are monotonically increasing counters.
			//The only thing interesting when comparing values observed from a monotonically increasing counter is the amount of the increase.
			//This the following values represent the difference between the most recently observed value for the counter and the previously observed value.
			this.pgmajfault = newRecord.get_pgmajfault().subtract(oldRecord.get_pgmajfault());
			this.pswpin = newRecord.get_pswpin().subtract(oldRecord.get_pswpin());
			this.pswpout = newRecord.get_pswpout().subtract(oldRecord.get_pswpout());
			this.allocstall = newRecord.get_allocstall().subtract(oldRecord.get_allocstall());
			
			this.freeMemPct = MemFree.doubleValue() / MemTotal.doubleValue();											//The mean MemFree of the input records expressed as a percentage of MemTotal.  ...Actually, it's just divided by the MemTotal.  E.g. the acceptable values are 0<=V<=1.  Should it be multiplied by 100?
			this.freeMemByteMilliseconds = MemFree.multiply(new BigInteger(Long.toString(getDurationms())));	//The amount of free memory over the elapsed time expressed in a jiffy-like manner
			
		//Check if these are differential SystemMemoryRecords
		} else if(rec1.getFreeMemPct()!=-1d && rec2.getFreeMemPct()!=-1d){
			//Check if these are chronologically consecutive differential records
			if(oldRecord.getTimestamp() == newRecord.getPreviousTimestamp()){
				//Copied values
				setTimestamp(newRecord.getTimestamp());					//Set the end timestamp to the timestamp of the newer record
				setPreviousTimestamp(oldRecord.getPreviousTimestamp());	//Set the start timestamp to the start timestamp of the older record
				this.MemTotal = newRecord.getMemTotal();				//Copy the MemTotal from either input record
				this.SwapTotal = newRecord.getSwapTotal();				//Copy the SwapTotal from either input record
		
				//In differential SystemMemoryRecords, these values represent the best-guess of the average amount of memory used during the duration of time represented by the record.
				//Since we are combining two such records, we should compute a new best-guess of the average amount used over the new total duration.
				//To do this, express the value in the older and newer records in a CPU usage/jiffy-like form, add them, then divide by the aggregate duration.
				this.MemFree = newRecord.getMemFree().multiply(new BigInteger(Long.toString(newRecord.getDurationms()))).add(
						oldRecord.getMemFree().multiply(new BigInteger(Long.toString(oldRecord.getDurationms())))).divide(
						new BigInteger(Long.toString(getDurationms())));
				this.SwapFree = newRecord.getSwapFree().multiply(new BigInteger(Long.toString(newRecord.getDurationms()))).add(
						oldRecord.getSwapFree().multiply(new BigInteger(Long.toString(oldRecord.getDurationms())))).divide(
						new BigInteger(Long.toString(getDurationms())));
				this.nr_dirty = newRecord.get_nr_dirty().multiply(new BigInteger(Long.toString(newRecord.getDurationms()))).add(
						oldRecord.get_nr_dirty().multiply(new BigInteger(Long.toString(oldRecord.getDurationms())))).divide(
						new BigInteger(Long.toString(getDurationms())));
		
				//In differential SystemMemoryRecords, these values represent the number of times a condition occurred over the period of time.
				//Add the values together from older and newer records to get the aggregate sum.
				this.pgmajfault = newRecord.get_pgmajfault().add(oldRecord.get_pgmajfault());
				this.pswpin = newRecord.get_pswpin().add(oldRecord.get_pswpin());
				this.pswpout = newRecord.get_pswpout().add(oldRecord.get_pswpout());
				this.allocstall = newRecord.get_allocstall().add(oldRecord.get_allocstall());
				
				this.freeMemPct = MemFree.doubleValue() / MemTotal.doubleValue();
				this.freeMemByteMilliseconds = MemFree.multiply(new BigInteger(Long.toString(getDurationms())));	//The amount of free memory over the elapsed time expressed in a jiffy-like manner
			}
			//Check if these are cumulative records
			else if (oldRecord.getPreviousTimestamp() == newRecord.getPreviousTimestamp()){
				setTimestamp(newRecord.getTimestamp());
				setPreviousTimestamp(oldRecord.getTimestamp());
				this.MemTotal = newRecord.getMemTotal();
				this.SwapTotal = newRecord.getSwapTotal();
				this.MemFree = newRecord.getMemFree().multiply(new BigInteger(Long.toString(newRecord.getDurationms()))).subtract(
								oldRecord.getMemFree().multiply(new BigInteger(Long.toString(oldRecord.getDurationms())))).divide(
								new BigInteger(Long.toString(this.getDurationms())));
				this.SwapFree = newRecord.getSwapFree().multiply(new BigInteger(Long.toString(newRecord.getDurationms()))).subtract(
								oldRecord.getSwapFree().multiply(new BigInteger(Long.toString(oldRecord.getDurationms())))).divide(
								new BigInteger(Long.toString(this.getDurationms())));
				this.nr_dirty = newRecord.get_nr_dirty().multiply(new BigInteger(Long.toString(newRecord.getDurationms()))).subtract(
								oldRecord.get_nr_dirty().multiply(new BigInteger(Long.toString(oldRecord.getDurationms())))).divide(
								new BigInteger(Long.toString(this.getDurationms())));
				this.pgmajfault = newRecord.get_pgmajfault().subtract(oldRecord.get_pgmajfault());
				this.pswpin = newRecord.get_pswpin().subtract(oldRecord.get_pswpin());
				this.pswpout = newRecord.get_pswpout().subtract(oldRecord.get_pswpout());
				this.allocstall = newRecord.get_allocstall().subtract(oldRecord.get_allocstall());
				this.freeMemPct = MemFree.doubleValue() / MemTotal.doubleValue();
				this.freeMemByteMilliseconds = MemFree.multiply(new BigInteger(Long.toString(getDurationms())));
			} 
			//Otherwise, throw an exception because these records can't be compared/merged/whatever
			else {
				throw new Exception ("Can not generate differential SystemMemoryRecord from non-consecutive differential SystemMemoryRecords, " + 
									 " old pt:" + oldRecord.getPreviousTimestamp() + 
									 " ts:" + oldRecord.getTimestamp() + 
									 " new pt:" + newRecord.getPreviousTimestamp() + 
									 " ts:" + newRecord.getTimestamp()
									);
			}
			
						
		//Otherwise, we are being asked to merge one raw record with one derived record and that is not valid.
		} else {
			throw new Exception("Can not generate differential SystemMemoryRecord from a raw record and a differential record");
		}

	}
	

	/**
	 * PRODUCE RECORD METHODS
	 */
	public static int[] produceRecord(RecordQueue outputQueue, String producerName){
		int[] ret = new int[] {0, 0, 0, 0};
		SystemMemoryRecord record = null;
		try {
			record = new SystemMemoryRecord();
		} catch (Exception e) {
			LOG.error("Failed to generate a SystemMemoryRecord");
			e.printStackTrace();
			ret[2] = 1;
		}
		if(record != null && !outputQueue.put(producerName, record)){
			ret[3] = 1;
			LOG.error("Failed to put SystemMemoryRecord into output queue " + outputQueue.getQueueName() + 
					" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.getQueueRecordCapacity() + 
					" producerName:" + producerName);
		} else {
			ret[1]=1;
		}		
		return ret;
	}


	/**
	 * OTHER METHODS
	 */
 	public boolean setValueByName(String name, BigInteger value){
		if (name.equals("MemTotal")) { MemTotal = value; }
		else if (name.equals("MemFree")) { MemFree = value; }
		else if (name.equals("Buffers")) { Buffers = value; }
		else if (name.equals("Cached")) { Cached = value; }
		else if (name.equals("SwapCached")) { SwapCached = value; }
		else if (name.equals("Active")) { Active = value; }
		else if (name.equals("Inactive")) { Inactive = value; }
		else if (name.equals("Active(anon)")) { Active_anon_ = value; }
		else if (name.equals("Inactive(anon)")) { Inactive_anon_ = value; }
		else if (name.equals("Active(file)")) { Active_file_ = value; }
		else if (name.equals("Inactive(file)")) { Inactive_file_ = value; }
		else if (name.equals("Unevictable")) { Unevictable = value; }
		else if (name.equals("Mlocked")) { Mlocked = value; }
		else if (name.equals("SwapTotal")) { SwapTotal = value; }
		else if (name.equals("SwapFree")) { SwapFree = value; }
		else if (name.equals("Dirty")) { Dirty = value; }
		else if (name.equals("Writeback")) { Writeback = value; }
		else if (name.equals("AnonPages")) { AnonPages = value; }
		else if (name.equals("Mapped")) { Mapped = value; }
		else if (name.equals("Shmem")) { Shmem = value; }
		else if (name.equals("Slab")) { Slab = value; }
		else if (name.equals("SReclaimable")) { SReclaimable = value; }
		else if (name.equals("SUnreclaim")) { SUnreclaim = value; }
		else if (name.equals("KernelStack")) { KernelStack = value; }
		else if (name.equals("PageTables")) { PageTables = value; }
		else if (name.equals("NFS_Unstable")) { NFS_Unstable = value; }
		else if (name.equals("Bounce")) { Bounce = value; }
		else if (name.equals("WritebackTmp")) { WritebackTmp = value; }
		else if (name.equals("CommitLimit")) { CommitLimit = value; }
		else if (name.equals("Committed_AS")) { Committed_AS = value; }
		else if (name.equals("VmallocTotal")) { VmallocTotal = value; }
		else if (name.equals("VmallocUsed")) { VmallocUsed = value; }
		else if (name.equals("VmallocChunk")) { VmallocChunk = value; }
		else if (name.equals("HardwareCorrupted")) { HardwareCorrupted = value; }
		else if (name.equals("AnonHugePages")) { AnonHugePages = value; }
		else if (name.equals("HugePages_Total")) { HugePages_Total = value; }
		else if (name.equals("HugePages_Free")) { HugePages_Free = value; }
		else if (name.equals("HugePages_Rsvd")) { HugePages_Rsvd = value; }
		else if (name.equals("HugePages_Surp")) { HugePages_Surp = value; }
		else if (name.equals("Hugepagesize")) { Hugepagesize = value; }
		else if (name.equals("DirectMap4k")) { DirectMap4k = value; }
		else if (name.equals("DirectMap2M")) { DirectMap2M = value; }
		else if (name.equals("DirectMap1G")) { DirectMap1G = value; }
		else if (name.equals("nr_dirty")) { nr_dirty = value; }
		else if (name.equals("pswpin")) { pswpin = value; }
		else if (name.equals("pswpout")) { pswpout = value; }
		else if (name.equals("pgmajfault")) { pgmajfault = value; }
		else if (name.equals("allocstall")) { allocstall = value; }

		else { return false; }
		return true;
	}
 	public BigInteger getMemTotal(){
 		return MemTotal;
 	}
 	public double getFreeMemPct(){
 		return freeMemPct;
 	}
 	public BigInteger getMemFree(){
 		return MemFree;
 	}
 	public BigInteger getSwapTotal(){
 		return SwapTotal;
 	}
 	public BigInteger getSwapFree(){
 		return SwapFree;
 	}
 	public BigInteger get_nr_dirty(){
 		return nr_dirty;
 	}
 	public BigInteger get_pswpin(){
 		return pswpin;
 	}
 	public BigInteger get_pswpout(){
 		return pswpout;
 	}
 	public BigInteger get_pgmajfault(){
 		return pgmajfault;
 	}
 	public BigInteger get_allocstall(){
 		return allocstall;
 	}
 	public String toString(){
 		DecimalFormat doubleDisplayFormat = new DecimalFormat("#0.00");
 		if(getPreviousTimestamp()==-1)
 			return super.toString() + " " + Constants.SYSTEM_MEMORY_RECORD + " Free:" + MemFree +
				" Mem:" + MemTotal +
				" SwapUsed:" + SwapTotal.subtract(SwapFree) + 
				" nr_dirty:" + nr_dirty + 
				" pswpin:" + pswpin + 
				" pswpout:" + pswpout + 
				" allocstall:" + allocstall;
 		else
 			return super.toString() + " " + Constants.SYSTEM_MEMORY_RECORD + " Free:" + MemFree + " " + doubleDisplayFormat.format(freeMemPct) + "%" + 
 				" Mem:" + MemTotal +
 				" SwapUsed:" + SwapTotal.subtract(SwapFree) + 
 				" nr_dirty:" + nr_dirty + 
 				" pswpin:" + pswpin + 
 				" pswpout:" + pswpout + 
 				" allocstall:" + allocstall;
	}
}

