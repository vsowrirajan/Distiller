package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.io.RandomAccessFile;

import com.mapr.distiller.server.queues.RecordQueue;

public class SystemCpuRecord extends Record {
	/**
	 * DERIVED VALUES
	 * These are variables that are not sourced directly from /proc
	 * These values are computed only for records returned by calls to a diff function of this class
	 */
	private double idleCpuUtilPct, iowaitCpuUtilPct;
	
	/**
	 * RAW VALUES
	 */
	private BigInteger cpu_user, cpu_nice, cpu_sys, cpu_idle, cpu_iowait, cpu_hardirq, cpu_softirq, cpu_steal, cpu_other, total_jiffies;
	
	/**
	 * CONSTRUCTORS
	 */
	public SystemCpuRecord() throws Exception {
		//This constructor takes "proc_stat" which should be the /proc/stat file opened for read
		super(System.currentTimeMillis());
		this.idleCpuUtilPct=-1d;
		this.iowaitCpuUtilPct=-1d;
		RandomAccessFile proc_stat = null;
		try {
			proc_stat = new RandomAccessFile("/proc/stat", "r");
			String[] parts = proc_stat.readLine().split("\\s+");

			if (parts.length < 9) {
				throw new Exception("First line of /proc/stat expected to have 9 or more fields, found " + parts.length);
			}
			if (!parts[0].equals("cpu")) {
				throw new Exception("First line of /proc/stat expected to start with \"cpu\"");
			}
			
			this.cpu_user = new BigInteger(parts[1]);
			this.cpu_nice = new BigInteger(parts[2]);
			this.cpu_sys = new BigInteger(parts[3]);
			this.cpu_idle = new BigInteger(parts[4]);
			this.cpu_iowait = new BigInteger(parts[5]);
			this.cpu_hardirq = new BigInteger(parts[6]);
			this.cpu_softirq = new BigInteger(parts[7]);
			this.cpu_steal = new BigInteger(parts[8]);
			this.cpu_other = new BigInteger("0");
			for(int x=9; x<parts.length; x++){
				this.cpu_other = this.cpu_other.add(new BigInteger(parts[x]));
			}
			this.total_jiffies = 	
					this.cpu_user.add(
					this.cpu_nice.add(
					this.cpu_sys.add(
					this.cpu_idle.add(
					this.cpu_iowait.add(
					this.cpu_hardirq.add(
					this.cpu_softirq.add(
					this.cpu_steal.add(
					this.cpu_other))))))));
		} catch (Exception e) {
			throw new Exception("Failed to generate SystemCpuRecord", e);
		} finally {
			try {
				proc_stat.close();
			} catch (Exception e) {}
		}
	}
	public SystemCpuRecord(SystemCpuRecord rec1, SystemCpuRecord rec2) throws Exception {
		if(rec1.getIdleCpuUtilPct()!=-1d || rec2.getIdleCpuUtilPct()!=-1d || rec1.getPreviousTimestamp()!=-1l || rec2.getPreviousTimestamp()!=-1l)
			throw new Exception("Differential SystemCpuRecord can only be generated from raw SystemCpuRecords");
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential SystemCpuRecord from input records with matching timestamp values");
		if(rec1.get_total_jiffies().equals(rec2.get_total_jiffies()))
			throw new Exception("Can not generate differential SystemCpuRecord from input records with matching total_jiffies");
		
		SystemCpuRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		
		this.setTimestamp(newRecord.getTimestamp());
		this.setPreviousTimestamp(oldRecord.getTimestamp());
		this.cpu_user = newRecord.get_cpu_user().subtract(oldRecord.get_cpu_user());
		this.cpu_nice = newRecord.get_cpu_nice().subtract(oldRecord.get_cpu_nice());
		this.cpu_sys = newRecord.get_cpu_sys().subtract(oldRecord.get_cpu_sys());
		this.cpu_idle = newRecord.get_cpu_idle().subtract(oldRecord.get_cpu_idle());
		this.cpu_iowait = newRecord.get_cpu_iowait().subtract(oldRecord.get_cpu_iowait());
		this.cpu_hardirq = newRecord.get_cpu_hardirq().subtract(oldRecord.get_cpu_hardirq());
		this.cpu_softirq = newRecord.get_cpu_softirq().subtract(oldRecord.get_cpu_softirq());
		this.cpu_steal = newRecord.get_cpu_steal().subtract(oldRecord.get_cpu_steal());
		this.cpu_other = newRecord.get_cpu_other().subtract(oldRecord.get_cpu_other());
		this.total_jiffies = newRecord.get_total_jiffies().subtract(oldRecord.get_total_jiffies());

		//Count iowait as idle time since other things could be done during idle time if so needed.
		//We will track iowait % usage separately
		//This var should be used to decide if the system is running low on free CPU capacity
		//Since iowait means there is no useful work to be done by the CPU, we count it idle.
		//That allows this metric to represent true load on the CPUs.
		this.idleCpuUtilPct = this.cpu_idle.add(this.cpu_iowait).doubleValue() / 	//The number of jiffies consumed in idle//iowait
									this.total_jiffies.doubleValue();							//Divided by the total elapsed jiffies

		//The IO wait time indicates how much running processes are bottlenecking on IO.
		//You can use this in conjunction with the iowaitCpuUtilPct for threads/processes to understand how things are bottlenecking on IO
		this.iowaitCpuUtilPct = this.cpu_iowait.doubleValue() / this.total_jiffies.doubleValue();
				
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	public static int[] produceRecord(RecordQueue outputQueue, String producerName){
		int[] ret = new int[] {0, 0, 0, 0};
		SystemCpuRecord record = null;
		try {
			record = new SystemCpuRecord();
		} catch (Exception e) {
			System.err.println("Failed to generate a SystemCpuRecord");
			e.printStackTrace();
			ret[2] = 1;
		}
		if(record != null && !outputQueue.put(producerName, record)){
			ret[3] = 1;
			System.err.println("Failed to put SystemCpuRecord into output queue " + outputQueue.getQueueName() + 
					" size:" + outputQueue.queueSize() + " maxSize:" + outputQueue.maxQueueSize() + 
					" producerName:" + producerName);
		} else {
			ret[1]=1;
		}	
		return ret;
	}
	
	/**
	 * OTHER METHODS
	 */
	public String toString(){
		return super.toString() + " SystemCPU user:" + cpu_user + 
				" nice:" + cpu_nice +
				" sys:" + cpu_sys + 
				" idle:" + cpu_idle + 
				" iowait:" + cpu_iowait + 
				" hard:" + cpu_hardirq + 
				" soft:" + cpu_softirq + 
				" steal:" + cpu_steal + 
				" other:" + cpu_other +
				" jiffies:" + total_jiffies + 
				" idleCpuUtilPct:" + idleCpuUtilPct + 
				" iowaitCpuUtilPct:" + iowaitCpuUtilPct;
	}
	public BigInteger get_cpu_user(){
		return cpu_user;
	}
	public BigInteger get_cpu_nice(){
		return cpu_nice;
	}
	public BigInteger get_cpu_sys(){
		return cpu_sys;
	}
	public BigInteger get_cpu_idle(){
		return cpu_idle;
	}
	public BigInteger get_cpu_iowait(){
		return cpu_iowait;
	}
	public BigInteger get_cpu_hardirq(){
		return cpu_hardirq;
	}
	public BigInteger get_cpu_softirq(){
		return cpu_softirq;
	}
	public BigInteger get_cpu_steal(){
		return cpu_steal;
	}
	public BigInteger get_cpu_other(){
		return cpu_other;
	}
	public BigInteger get_total_jiffies(){
		return total_jiffies;
	}
	public double getIdleCpuUtilPct(){
		return idleCpuUtilPct;
	}
	public double getIowaitCpuUtilPct(){
		return iowaitCpuUtilPct;
	}
}
