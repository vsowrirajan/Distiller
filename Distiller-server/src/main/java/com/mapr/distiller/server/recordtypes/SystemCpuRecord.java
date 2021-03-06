package com.mapr.distiller.server.recordtypes;

import java.math.BigInteger;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.Constants;

public class SystemCpuRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(SystemCpuRecord.class);
	private static final long serialVersionUID = Constants.SVUID_SYSTEM_CPU_RECORD;
	/**
	 * DERIVED VALUES
	 * These are variables that are not sourced directly from /proc
	 * These values are computed only for records returned by calls to a diff function of this class
	 */
	private double idleCpuUtilPct, iowaitCpuUtilPct;
	
	/**
	 * RAW VALUES
	 */
	//For system aggregate
	private BigInteger cpu_user, cpu_nice, cpu_sys, cpu_idle, cpu_iowait, cpu_hardirq, cpu_softirq, cpu_steal, cpu_other, total_jiffies;
	//For single core
	private BigInteger[] acpu_user, acpu_nice, acpu_sys, acpu_idle, acpu_iowait, acpu_hardirq, acpu_softirq, acpu_steal, acpu_other, atotal_jiffies;

	private double idleCpuUtilPctExcluding2, iowaitCpuUtilPctExcluding2, idleCpuUtilPctExcluding4, iowaitCpuUtilPctExcluding4;
	@Override
	public String getRecordType(){
		return Constants.SYSTEM_CPU_RECORD;
	}
	

	/**
	 * CONSTRUCTORS
	 */
	public SystemCpuRecord() throws Exception {
		//This constructor takes "proc_stat" which should be the /proc/stat file opened for read
		super(System.currentTimeMillis());
		this.idleCpuUtilPct=-1d;
		this.iowaitCpuUtilPct=-1d;
		this.idleCpuUtilPctExcluding2=-1d;
		this.iowaitCpuUtilPctExcluding2=-1d;
		this.idleCpuUtilPctExcluding4=-1d;
		this.iowaitCpuUtilPctExcluding4=-1d;
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
			
			//Now process single cpu lines
			LinkedList<String[]> cpuLines = new LinkedList<String[]>();
			String line = null;
			while((line = proc_stat.readLine()) != null){
				if(line.startsWith("cpu")){
					cpuLines.add(line.split("\\s+"));
				}
			}
			this.acpu_user = new BigInteger[cpuLines.size()];
			this.acpu_nice = new BigInteger[cpuLines.size()];
			this.acpu_sys = new BigInteger[cpuLines.size()];
			this.acpu_idle = new BigInteger[cpuLines.size()];
			this.acpu_iowait = new BigInteger[cpuLines.size()];
			this.acpu_hardirq = new BigInteger[cpuLines.size()];
			this.acpu_softirq = new BigInteger[cpuLines.size()];
			this.acpu_steal = new BigInteger[cpuLines.size()];
			this.acpu_other = new BigInteger[cpuLines.size()];
			this.atotal_jiffies = new BigInteger[cpuLines.size()];
			int x=0;
			while(cpuLines.size()!=0){
				parts = cpuLines.get(0);
				this.acpu_user[x] = new BigInteger(parts[1]);
				this.acpu_nice[x] = new BigInteger(parts[2]);
				this.acpu_sys[x] = new BigInteger(parts[3]);
				this.acpu_idle[x] = new BigInteger(parts[4]);
				this.acpu_iowait[x] = new BigInteger(parts[5]);
				this.acpu_hardirq[x] = new BigInteger(parts[6]);
				this.acpu_softirq[x] = new BigInteger(parts[7]);
				this.acpu_steal[x] = new BigInteger(parts[8]);
				this.acpu_other[x] = new BigInteger("0");
				for(int y=9; y<parts.length; y++){
					this.acpu_other[x] = this.acpu_other[x].add(new BigInteger(parts[y]));
				}
				this.atotal_jiffies[x] = 	
						this.acpu_user[x].add(
						this.acpu_nice[x].add(
						this.acpu_sys[x].add(
						this.acpu_idle[x].add(
						this.acpu_iowait[x].add(
						this.acpu_hardirq[x].add(
						this.acpu_softirq[x].add(
						this.acpu_steal[x].add(
						this.acpu_other[x]))))))));
				x++;
				cpuLines.remove(0);
			}
		} catch (Exception e) {
			throw new Exception("Failed to generate SystemCpuRecord", e);
		} finally {
			try {
				proc_stat.close();
			} catch (Exception e) {}
		}
	}
	public SystemCpuRecord(SystemCpuRecord rec1, SystemCpuRecord rec2) throws Exception {
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential SystemCpuRecord from input records with matching timestamp values");
		if(rec1.get_total_jiffies().equals(rec2.get_total_jiffies()) && rec1.getPreviousTimestamp() == -1)
			throw new Exception("Can not generate differential SystemCpuRecord from raw input records with matching total_jiffies");
		if( ( rec1.getPreviousTimestamp()==-1 && rec2.getPreviousTimestamp()!=-1 ) || 
			( rec2.getPreviousTimestamp()==-1 && rec1.getPreviousTimestamp()!=-1 ) )
		{
			throw new Exception("Can not generate differential SystemCpuRecord from one raw input record and one differential input record");
		}
		if(rec1.get_acpu_hardirq().length != rec2.get_acpu_hardirq().length)
			throw new Exception("Can not generate differential SysemCpuRecord form input records with differing numbers of CPU cores");
		SystemCpuRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = rec1;
			newRecord = rec2;
		} else {
			oldRecord = rec2;
			newRecord = rec1;
		}
		
		if(oldRecord.getPreviousTimestamp() != -1 && newRecord.getPreviousTimestamp() != -1 && oldRecord.getTimestamp() != newRecord.getPreviousTimestamp())
			throw new Exception("Can not generate differential SystemCpuRecords from non-chronologically consecutive differential input records");

		int numCores = newRecord.get_acpu_hardirq().length;
		this.acpu_user = new BigInteger[numCores];
		this.acpu_nice = new BigInteger[numCores];
		this.acpu_sys = new BigInteger[numCores];
		this.acpu_idle = new BigInteger[numCores];
		this.acpu_iowait = new BigInteger[numCores];
		this.acpu_hardirq = new BigInteger[numCores];
		this.acpu_softirq = new BigInteger[numCores];
		this.acpu_steal = new BigInteger[numCores];
		this.acpu_other = new BigInteger[numCores];
		this.atotal_jiffies = new BigInteger[numCores];
		this.setTimestamp(newRecord.getTimestamp());
		if(oldRecord.getTimestamp() == newRecord.getPreviousTimestamp()){
			this.setPreviousTimestamp(oldRecord.getPreviousTimestamp());
			this.cpu_user = newRecord.get_cpu_user().add(oldRecord.get_cpu_user());
			this.cpu_nice = newRecord.get_cpu_nice().add(oldRecord.get_cpu_nice());
			this.cpu_sys = newRecord.get_cpu_sys().add(oldRecord.get_cpu_sys());
			this.cpu_idle = newRecord.get_cpu_idle().add(oldRecord.get_cpu_idle());
			this.cpu_iowait = newRecord.get_cpu_iowait().add(oldRecord.get_cpu_iowait());
			this.cpu_hardirq = newRecord.get_cpu_hardirq().add(oldRecord.get_cpu_hardirq());
			this.cpu_softirq = newRecord.get_cpu_softirq().add(oldRecord.get_cpu_softirq());
			this.cpu_steal = newRecord.get_cpu_steal().add(oldRecord.get_cpu_steal());
			this.cpu_other = newRecord.get_cpu_other().add(oldRecord.get_cpu_other());
			this.total_jiffies = newRecord.get_total_jiffies().add(oldRecord.get_total_jiffies());
			for (int x=0; x<numCores; x++){
				this.acpu_user[x] = newRecord.get_acpu_user()[x].add(oldRecord.get_acpu_user()[x]);
				this.acpu_nice[x] = newRecord.get_acpu_nice()[x].add(oldRecord.get_acpu_nice()[x]);
				this.acpu_sys[x] = newRecord.get_acpu_sys()[x].add(oldRecord.get_acpu_sys()[x]);
				this.acpu_idle[x] = newRecord.get_acpu_idle()[x].add(oldRecord.get_acpu_idle()[x]);
				this.acpu_iowait[x] = newRecord.get_acpu_iowait()[x].add(oldRecord.get_acpu_iowait()[x]);
				this.acpu_hardirq[x] = newRecord.get_acpu_hardirq()[x].add(oldRecord.get_acpu_hardirq()[x]);
				this.acpu_softirq[x] = newRecord.get_acpu_softirq()[x].add(oldRecord.get_acpu_softirq()[x]);
				this.acpu_steal[x] = newRecord.get_acpu_steal()[x].add(oldRecord.get_acpu_steal()[x]);
				this.acpu_other[x] = newRecord.get_acpu_other()[x].add(oldRecord.get_acpu_other()[x]);
				this.atotal_jiffies[x] = 	
						this.acpu_user[x].add(
						this.acpu_nice[x].add(
						this.acpu_sys[x].add(
						this.acpu_idle[x].add(
						this.acpu_iowait[x].add(
						this.acpu_hardirq[x].add(
						this.acpu_softirq[x].add(
						this.acpu_steal[x].add(
						this.acpu_other[x]))))))));		
			}
		} else {
			if (newRecord.get_cpu_user().compareTo(oldRecord.get_cpu_user()) == -1 || 
				newRecord.get_cpu_nice().compareTo(oldRecord.get_cpu_nice()) == -1 || 
				newRecord.get_cpu_sys().compareTo(oldRecord.get_cpu_sys()) == -1 || 
				newRecord.get_cpu_idle().compareTo(oldRecord.get_cpu_idle()) == -1 || 
				newRecord.get_cpu_hardirq().compareTo(oldRecord.get_cpu_hardirq()) == -1 || 
				newRecord.get_cpu_softirq().compareTo(oldRecord.get_cpu_softirq()) == -1 || 
				newRecord.get_cpu_steal().compareTo(oldRecord.get_cpu_steal()) == -1 || 
				newRecord.get_cpu_other().compareTo(oldRecord.get_cpu_other()) == -1 || 
				newRecord.get_total_jiffies().compareTo(oldRecord.get_total_jiffies()) ==-1 ){		
				throw new Exception("Can not generate differential record from raw input records where counters have rolled over between samples. " + 
						" old: " + oldRecord.toString() + " new: " + newRecord.toString());
			} 
			//This check is here because IOWait counters can go backwards... derp...
			//https://lkml.org/lkml/2014/5/7/559
			if( newRecord.get_cpu_iowait().compareTo(oldRecord.get_cpu_iowait()) == -1 &&
				oldRecord.get_cpu_iowait().subtract(newRecord.get_cpu_iowait()).compareTo(new BigInteger("1024")) == 1 ) {
				throw new Exception("Can not generate differential record from raw input records where counters have rolled over between samples. " + 
						" old: " + oldRecord.toString() + " new: " + newRecord.toString());
				
			}
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
			for (int x=0; x<numCores; x++){
				this.acpu_user[x] = newRecord.get_acpu_user()[x].subtract(oldRecord.get_acpu_user()[x]);
				this.acpu_nice[x] = newRecord.get_acpu_nice()[x].subtract(oldRecord.get_acpu_nice()[x]);
				this.acpu_sys[x] = newRecord.get_acpu_sys()[x].subtract(oldRecord.get_acpu_sys()[x]);
				this.acpu_idle[x] = newRecord.get_acpu_idle()[x].subtract(oldRecord.get_acpu_idle()[x]);
				this.acpu_iowait[x] = newRecord.get_acpu_iowait()[x].subtract(oldRecord.get_acpu_iowait()[x]);
				this.acpu_hardirq[x] = newRecord.get_acpu_hardirq()[x].subtract(oldRecord.get_acpu_hardirq()[x]);
				this.acpu_softirq[x] = newRecord.get_acpu_softirq()[x].subtract(oldRecord.get_acpu_softirq()[x]);
				this.acpu_steal[x] = newRecord.get_acpu_steal()[x].subtract(oldRecord.get_acpu_steal()[x]);
				this.acpu_other[x] = newRecord.get_acpu_other()[x].subtract(oldRecord.get_acpu_other()[x]);
				this.atotal_jiffies[x] = 	
						this.acpu_user[x].add(
						this.acpu_nice[x].add(
						this.acpu_sys[x].add(
						this.acpu_idle[x].add(
						this.acpu_iowait[x].add(
						this.acpu_hardirq[x].add(
						this.acpu_softirq[x].add(
						this.acpu_steal[x].add(
						this.acpu_other[x]))))))));		
			}
		}
		
		
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
		if(this.acpu_hardirq.length<3){
			this.idleCpuUtilPctExcluding2=-1d;
			this.iowaitCpuUtilPctExcluding2=-1d;
			this.idleCpuUtilPctExcluding4=-1d;
			this.iowaitCpuUtilPctExcluding4=-1d;
		} else {
			BigInteger ex2IdleJiffies = new BigInteger("0");
			BigInteger ex2IowaitJiffies = new BigInteger("0");
			BigInteger ex2TotalJiffies = new BigInteger("0");
			for(int x=2; x<this.acpu_hardirq.length; x++){
				ex2IdleJiffies = ex2IdleJiffies.add(this.acpu_idle[x]);
				ex2IowaitJiffies = ex2IowaitJiffies.add(this.acpu_iowait[x]);
				ex2TotalJiffies = ex2TotalJiffies.add(this.atotal_jiffies[x]);
			}
			this.idleCpuUtilPctExcluding2 = ex2IdleJiffies.add(ex2IowaitJiffies).doubleValue() / ex2TotalJiffies.doubleValue();
			this.iowaitCpuUtilPctExcluding2 = ex2IowaitJiffies.doubleValue() / ex2TotalJiffies.doubleValue();
			if(this.acpu_hardirq.length<5){
				this.idleCpuUtilPctExcluding4=-1d;
				this.iowaitCpuUtilPctExcluding4=-1d;
			} else {
				BigInteger ex4IdleJiffies = new BigInteger("0");
				BigInteger ex4IowaitJiffies = new BigInteger("0");
				BigInteger ex4TotalJiffies = new BigInteger("0");
				for(int x=4; x<this.acpu_hardirq.length; x++){
					ex4IdleJiffies = ex4IdleJiffies.add(this.acpu_idle[x]);
					ex4IowaitJiffies = ex4IowaitJiffies.add(this.acpu_iowait[x]);
					ex4TotalJiffies = ex4TotalJiffies.add(this.atotal_jiffies[x]);
				}
				this.idleCpuUtilPctExcluding4 = ex4IdleJiffies.add(ex4IowaitJiffies).doubleValue() / ex4TotalJiffies.doubleValue();
				this.iowaitCpuUtilPctExcluding4 = ex4IowaitJiffies.doubleValue() / ex4TotalJiffies.doubleValue();
			}
		}
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
			LOG.error("Failed to generate a SystemCpuRecord", e);
			ret[2] = 1;
		}
		if(record != null && !outputQueue.put(producerName, record)){
			ret[3] = 1;
			LOG.error("Failed to put SystemCpuRecord into output queue " + outputQueue.getQueueName() + 
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
	public String toString(){
		DecimalFormat doubleDisplayFormat = new DecimalFormat("#0.00");
		return super.toString() + " " + Constants.SYSTEM_CPU_RECORD + " user:" + cpu_user + 
				" nice:" + cpu_nice +
				" sys:" + cpu_sys + 
				" idle:" + cpu_idle + 
				" iowait:" + cpu_iowait + 
				" hard:" + cpu_hardirq + 
				" soft:" + cpu_softirq + 
				" steal:" + cpu_steal + 
				" other:" + cpu_other +
				" jiffies:" + total_jiffies + 
				" idle%:" + doubleDisplayFormat.format(idleCpuUtilPct) + 
				" iowait%:" + doubleDisplayFormat.format(iowaitCpuUtilPct) + 
				" idle%-2:" + doubleDisplayFormat.format(idleCpuUtilPctExcluding2) + 
				" iowait%-2:" + doubleDisplayFormat.format(iowaitCpuUtilPctExcluding2) + 
				" numCPUs:" + acpu_user.length;
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
	public double getIdleCpuUtilPctExcluding2(){
		return idleCpuUtilPctExcluding2;
	}
	public double getIowaitCpuUtilPctExcluding2(){
		return iowaitCpuUtilPctExcluding2;
	}
	public double getIdleCpuUtilPctExcluding4(){
		return idleCpuUtilPctExcluding4;
	}
	public double getIowaitCpuUtilPctExcluding4(){
		return iowaitCpuUtilPctExcluding4;
	}
	public BigInteger[] get_acpu_user(){
		return acpu_user;
	}
	public BigInteger[] get_acpu_nice(){
		return acpu_nice;
	}
	public BigInteger[] get_acpu_sys(){
		return acpu_sys;
	}
	public BigInteger[] get_acpu_idle(){
		return acpu_idle;
	}
	public BigInteger[] get_acpu_iowait(){
		return acpu_iowait;
	}
	public BigInteger[] get_acpu_hardirq(){
		return acpu_hardirq;
	}
	public BigInteger[] get_acpu_softirq(){
		return acpu_softirq;
	}
	public BigInteger[] get_acpu_steal(){
		return acpu_steal;
	}
	public BigInteger[] get_acpu_other(){
		return acpu_other;
	}
	public BigInteger[] get_atotal_jiffies(){
		return atotal_jiffies;
	}

}
