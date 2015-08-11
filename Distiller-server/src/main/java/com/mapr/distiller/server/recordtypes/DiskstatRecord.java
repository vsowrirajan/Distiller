package com.mapr.distiller.server.recordtypes;

import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigInteger;

import com.mapr.distiller.server.queues.RecordQueue;

public class DiskstatRecord extends Record {
	/**
	 * DERIVED VALUES
	 */
	private double 	readOperationRate, 			//The rate at which the kernel is responding to read system API calls for the block device
					diskReadOperationRate, 		//The rate at which the block device is responding to read calls from the kernel
					readByteRate,				//The number of bytes per second read from the block device
					writeOperationRate, 		//Same as the read counterpart, but for writes.
					diskWriteOperationRate, 	//Same as the read counterpart, but for writes.
					writeByteRate,				//Same as the read counterpart, but for writes.
					utilizationPct,				//The percentage of time the disk device had outstanding IO requests.
												//For devices backed by a single disk/spindle, a 100% utilization indicates the disk has bottlenecked.
					averageOperationsInProgress,//The average number of IO system API calls in progress in the kernel
					averageWaitTime,			//The average amount of time taken from when an IO system API call was issued to the kernel
												//to when the kernel responded.  
					averageServiceTime;			//The average amount of time taken from when the kernel issued a request to the device to when
												//the device responded.
					

	/**
	 * RAW VALUES
	 */
	private int major_number, minor_number, hw_sector_size;
	private String device_name;
	private BigInteger 	reads_completed_successfully, 	//The number of read requests (system API calls) that have completed 
														//successfully against the block device.
						reads_merged, 					//The number of read requests (system API calls) that could be combined
														//before sending the request to the block device.  E.g. an application 
														//may call a read system API to read 8KB at offset X, and then another 
														//read system API call to read 8KB at offset X+8KB, in which case, the
														//system can decide to execute a 16KB read to the block device at offset 
														//X and then use the resulting data to respond to both of the read 
														//system API calls.  In turn, reads_completed_successfully - reads_merged
														//is equal to the actual number of read requests sent to the block device
														//by the kernel.
						sectors_read,					//The number sectors read from the block device.  Sector size (in bytes) 
														//is available from /sys/block/<device>/queue/hw_sector_size
														//Thus, bytes read from disk equals sectors_read multiplied by hw_sector_size
														//In turn, average read request for read system API calls is equal to 
														//(sectors_read * hw_sector_size) / reads_completed_successfully
														//And the average read request size issued by the kernel to the block device
														//is equal to (sectors_read * hw_sector_size) /
														//(reads_completed_successfully - reads_merged)
						time_spent_reading,				//??? Need to investigate this further to understand exact functionality
														//I'm currently not sure whether multiple concurrent reads waiting to be
														//serviced will all be incrementing this counter, or if this counter is 
														//incremented solely for the time that the disk device is actually servicing
														//one specific read request.
						writes_completed, 				//Like reads_completed, but for writes
						writes_merged, 					//Like it's read counterpart.
						sectors_written, 				//Like it's read counterpart.
						time_spent_writing,				//Like it's read counterpart.
						IOs_currently_in_progress, 		//Number of IO requests that have been submitted via system API call but
														//which have not yet returned.
						time_spent_doing_IOs, 			//The amount of time the disk device has had outstanding IO requests to it.
														//This is the amount of time that IOs_currently_in_progress is >0
						weighted_time_spent_doing_IOs;	//The aggregate amount of time that all IO requests to the block device were
														//outstanding.  E.g. if you submit 10 IO requests at the same time, and each
														//request takes the block device 10ms to process, then it takes 100ms total
														//before all requests complete.  So time_spent_doing_IOs would be 100ms.
														//But for the first 10ms, there are 10 outstanding requests, so the aggregate
														//time spent for those requests is already 100ms, then when the first request
														//completes and the second is being service, there are 9 IOs in progress, so
														//that totals to 90ms of wait time.  In all, by the time all 10 IOs are finished
														//the aggregate amount of wait time will be 550ms.  So weighted_time_spent_doing_IOs
														//will increase by 550ms while time_spent_doing_IOs will increase by 100ms.
														//Also, if a block device is capable of performing multiple concurrent IOs
														//(such as a block device thats actually a JBOD behind the scenes)
														//then it's possible that those 10 requests issued at the same time complete in, say,
														//50ms, with 2 requests completing every 10ms.

	/**
	 * CONSTRUCTORS
	 */
	public DiskstatRecord(DiskstatRecord rec1, DiskstatRecord rec2) throws Exception{
		DiskstatRecord oldRecord, newRecord;
		
		//Check the input records to ensure they can be diff'd.
		if(	rec1.getReadOperationRate()!=-1d || rec2.getReadOperationRate()!=-1d || rec1.getPreviousTimestamp()!=-1l || rec2.getPreviousTimestamp()!=-1l)
			throw new Exception("Differential DiskstatRecord can only be generated from raw DiskstatRecords");
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential DiskstatRecord from input records with matching timestamp values");
		if(		rec1.get_major_number() != rec2.get_major_number() || rec1.get_minor_number() != rec2.get_minor_number() || 
				rec1.get_hw_sector_size() != rec2.get_hw_sector_size() || !rec1.get_device_name().equals(rec2.get_device_name()) )
			throw new Exception("Can not generate differential DiskstatRecord from DiskstatRecords form different devices");
		
		//Organize the input records.
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		
		//Copied values:
		this.setTimestamp(newRecord.getTimestamp());
		this.setPreviousTimestamp(oldRecord.getTimestamp());
		this.major_number = rec1.get_major_number();
		this.minor_number = rec1.get_minor_number();
		this.hw_sector_size = rec1.get_hw_sector_size();
		this.device_name = rec1.get_device_name();
		this.IOs_currently_in_progress = rec1.get_IOs_currently_in_progress();
		
		//Differential values:
		this.reads_completed_successfully = newRecord.get_reads_completed_successfully().subtract(oldRecord.get_reads_completed_successfully());
		this.reads_merged = newRecord.get_reads_merged().subtract(oldRecord.get_reads_merged());
		this.sectors_read = newRecord.get_sectors_read().subtract(oldRecord.get_sectors_read());
		this.time_spent_reading = newRecord.get_time_spent_reading().subtract(oldRecord.get_time_spent_reading());
		this.writes_completed = newRecord.get_writes_completed().subtract(oldRecord.get_writes_completed());
		this.writes_merged = newRecord.get_writes_merged().subtract(oldRecord.get_writes_merged());
		this.sectors_written = newRecord.get_sectors_written().subtract(oldRecord.get_sectors_written());
		this.time_spent_writing = newRecord.get_time_spent_writing().subtract(oldRecord.get_time_spent_writing());
		this.time_spent_doing_IOs = newRecord.get_time_spent_doing_IOs().subtract(oldRecord.get_time_spent_doing_IOs());
		this.weighted_time_spent_doing_IOs = newRecord.get_weighted_time_spent_doing_IOs().subtract(oldRecord.get_weighted_time_spent_doing_IOs());
		
		//Derived values:
		this.readOperationRate = this.reads_completed_successfully.doubleValue() * 1000d / ((double)this.getDurationms()); 
		this.diskReadOperationRate = this.reads_completed_successfully.subtract(this.reads_merged).doubleValue() * 1000d / ((double)this.getDurationms());
		this.readByteRate = this.sectors_read.doubleValue() *  this.hw_sector_size * 1000d / ((double)this.getDurationms());
		this.writeOperationRate = this.writes_completed.doubleValue() * 1000d / ((double)this.getDurationms()); 
		this.diskWriteOperationRate = this.writes_completed.subtract(this.reads_merged).doubleValue() * 1000d / ((double)this.getDurationms());
		this.writeByteRate = this.sectors_written.doubleValue() *  this.hw_sector_size * 1000d / ((double)this.getDurationms());
		this.utilizationPct = this.time_spent_doing_IOs.doubleValue() / ((double)this.getDurationms());
		this.averageServiceTime = this.time_spent_doing_IOs.doubleValue() / this.reads_completed_successfully.add(this.writes_completed).doubleValue();;
		this.averageWaitTime = this.weighted_time_spent_doing_IOs.doubleValue() / this.reads_completed_successfully.add(this.writes_completed).doubleValue();
		this.averageOperationsInProgress = this.weighted_time_spent_doing_IOs.doubleValue() / ((double)this.getDurationms());
	}
	public DiskstatRecord(long now, String[] parts, int hw_sector_size) throws Exception {
		//This constructor uses "now" as the timestamp value for the record and parses values for all other raw values from the "parts" array.
		//This constructor will throw an ArrayIndexOutOfBoundsException if the "parts" array is not of the expected length
		//This constructor will throw various other types of exceptions if any of the value strings can't be parsed, e.g. NumberFormatException
		//Obviously, when the constructor throws an exception, we should consider this a failure to gather a metric sample and that should be
		//percolated up the call stack to whatever requested that a DiskstatRecord be generated
		super(now);
		this.readOperationRate=-1d;
		this.diskReadOperationRate=-1d;
		this.readByteRate=-1d;
		this.writeOperationRate=-1d;
		this.diskWriteOperationRate=-1d;
		this.writeByteRate=-1d;
		this.utilizationPct=-1d;
		this.averageServiceTime=-1d;
		this.hw_sector_size = hw_sector_size;
		this.major_number = Integer.parseInt(parts[0]);
		this.minor_number = Integer.parseInt(parts[1]);
		this.device_name = parts[2];
		this.reads_completed_successfully = new BigInteger(parts[3]);
		this.reads_merged = new BigInteger(parts[4]);
		this.sectors_read = new BigInteger(parts[5]);
		this.time_spent_reading = new BigInteger(parts[6]);
		this.writes_completed = new BigInteger(parts[7]);
		this.writes_merged = new BigInteger(parts[8]);
		this.sectors_written = new BigInteger(parts[9]);
		this.time_spent_writing = new BigInteger(parts[10]);
		this.IOs_currently_in_progress = new BigInteger(parts[11]);
		this.time_spent_doing_IOs = new BigInteger(parts[12]);
		this.weighted_time_spent_doing_IOs = new BigInteger(parts[13]);
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	//ret[0] - 0 indicates method completed successfully, 1 indicates method failed to run, this is different from failing to create a record.
	//ret[1] - records created and put to the output queue
	//ret[2] - failed record creation attempts
	//ret[3] - outputQueue put failures
	public static int[] produceRecords(RecordQueue outputQueue, String producerName){
		int[] ret = new int[] {0, 0, 0, 0};
		//This method should be called to generate DiskstatRecords for all physical block devices on the system, one for each device.
		//The DiskstatRecords generated by this method are put into the disk_system record queue.
		//This method will return true if DiskstatRecords were successfully generated for all physical block devices.
		//If we fail to generate even one record for one device then this method will immediately return false.
		RandomAccessFile proc_diskstats = null;
		
		boolean returnCode = true;
		
		//Open the "/proc/diskstats" file for reading
		try {
			proc_diskstats = new RandomAccessFile("/proc/diskstats", "r");
		
			//Timestamp that will be used to indicate the time this iteration of records was generated
			long now = System.currentTimeMillis();
			
			String line = null;
			//Read the file line by line and process each line, either discarding the line or processing it into a record
			//Each line represents a block device
			while ( (line = proc_diskstats.readLine()) != null) {
				String[] parts = line.trim().split("\\s+");
				//Each line is expected to have 14 fields based on kernels we currently support
				//This is basically a lazy check against actually inspecting the procfs/kernel implementation
				//If someone were to build their own OS image it's possible we still have 14 fields but they don't mean the same things...
				//Not worrying about that for now...
				if(parts.length < 14){
					System.err.println("Unexpected format found in /proc/diskstats, length:" + parts.length);
					System.err.println(line);
					return new int[] {1, 0, 0, 0};
				}
				//Check if it's a physical device, we only want stats for physical devices
				String sys_device_path = "/sys/block/" + parts[2].replaceAll("/", "!") + "/device";
				File f = new File(sys_device_path);
				if(f.exists()){	
					//It's a physical device...
					//Gather the sector size so we know the write units to calculate IO rates in bytes per second
					int hw_sector_size=0;
					RandomAccessFile hw_sector_sizeFile = null;
	                try {
	                        hw_sector_sizeFile = new RandomAccessFile("/sys/block/" + parts[2].replaceAll("/", "!") + "/queue/hw_sector_size", "r");
	                        String l = hw_sector_sizeFile.readLine();
	                        hw_sector_size = Integer.parseInt(l.trim().split("\\s+")[0]);
	                } catch (Exception e) {
	                		System.err.println("Failed to read hw_sector_size from /sys/block/" + parts[2].replaceAll("/", "!") + "/queue/hw_sector_size");
	                        return new int[] {1, 0, 0, 0};
	                } finally {
	                        try{
	                                hw_sector_sizeFile.close();
	                        } catch (Exception e) {}
	                }

					//Generate the Diskstat record
	                DiskstatRecord record = null;
					try{
						record = new DiskstatRecord(now, parts, hw_sector_size);
					} catch (Exception e) {
						//The constructor may throw an exception, we catch this here and do NOT rethrow it.
						//We just set that we failed to gather a record and thus the call to this method will return false.
						//We catch this exception here so that we can continue trying to process the remaining lines in the diskstat file.
						System.err.println("Failed to generate a DiskstatRecord");
						e.printStackTrace();
						ret[2]++;
					}
					if(record != null && !outputQueue.put(producerName, record)){
						ret[3]++;
						System.err.println("Failed to put SystemMemoryRecord into output queue " + outputQueue.getQueueName() + 
								" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.getQueueRecordCapacity() + 
								" producerName:" + producerName);
					} else {
						ret[1]++;
					}
					
				} 
			}
		} catch (Exception e) {
			System.err.println("Failed reading disksstats");
			e.printStackTrace();
			ret[0]++;
		} finally {
			try {
				proc_diskstats.close();
			} catch (Exception e) {}
		}
		return ret;
	}
	
	/**
	 * OTHER METHODS
	 */
	public String toString(){
		return super.toString() + " Diskstat dev:" + device_name + 
				" rcs " + reads_completed_successfully + 
				" rm " + reads_merged + 
				" sr " + sectors_read + 
				" tsr " + time_spent_reading + 
				" wc " + writes_completed + 
				" wm " + writes_merged + 
				" sw " + sectors_written +
				" tsw " + time_spent_writing + 
				" cip " + IOs_currently_in_progress + 
				" ts " + time_spent_doing_IOs + 
				" wts " + weighted_time_spent_doing_IOs;
	}
	public BigInteger get_IOs_currently_in_progress(){
		return IOs_currently_in_progress;
	}
	public BigInteger get_reads_completed_successfully(){
		return reads_completed_successfully;
	}
	public BigInteger get_reads_merged(){
		return reads_merged;
	}
	public BigInteger get_sectors_read(){
		return sectors_read;
	}
	public BigInteger get_sectors_written(){
		return sectors_written;
	}
	public BigInteger get_time_spent_doing_IOs(){
		return time_spent_doing_IOs;
	}
	public BigInteger get_time_spent_reading(){
		return time_spent_reading;
	}
	public BigInteger get_time_spent_writing(){
		return time_spent_writing;
	}
	public BigInteger get_weighted_time_spent_doing_IOs(){
		return weighted_time_spent_doing_IOs;
	}
	public BigInteger get_writes_completed(){
		return writes_completed;
	}
	public BigInteger get_writes_merged(){
		return writes_merged;
	}
	public int get_major_number(){
		return major_number;
	}
	public int get_minor_number(){
		return minor_number;
	}
	public int get_hw_sector_size(){
		return hw_sector_size;
	}
	public String get_device_name(){
		return device_name;
	}
	public double getReadOperationRate(){
		return readOperationRate;
	}
	public double getDiskReadOperationRate(){
		return diskReadOperationRate;
	}
	public double getReadByteRate(){
		return readByteRate;
	}
	public double getWriteOperationRate(){
		return writeOperationRate;
	}
	public double getDiskWriteOperationRate(){
		return diskWriteOperationRate;
	}
	public double getWriteByteRate(){
		return writeByteRate;
	}
	public double getUtilizationPct(){
		return utilizationPct;
	}
	public double getAverageOperationsInProgress(){
		return averageOperationsInProgress;
	}
	public double getAverageServiceTime(){
		return averageServiceTime;
	}
	public double getAverageWaitTime(){
		return averageWaitTime;
	}

}
