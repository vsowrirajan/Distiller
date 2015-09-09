package com.mapr.distiller.server.recordtypes;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.utils.Constants;

public class ProcessEfficiencyRecord extends Record {
	private static final Logger LOG = LoggerFactory
			.getLogger(ProcessEfficiencyRecord.class);
	private static final long serialVersionUID = Constants.SVUID_PROCESS_EFFICIENCY_RECORD;
	
	private String comm;
	private int pid, averageReadIOSize, averageWriteIOSize, averageIOSize;
	private long starttime;
	private static final BigInteger biZero = new BigInteger("0");
	
	/**
	 * Representations of the memory used by a process related to disk I/O.
	 * 
	 * An example of how this can be useful, a process may create an in-memory cache
	 * that it uses to respond to client read requests.  Controlling for other
	 * factors, plotting this value against the size of the in-memory cache should
	 * result in a visual representation of a hyperbolic function.  
	 * 
	 * E.g. as the size of the cache approaches 0, so too
	 * should the memoryBytesPerIOByte, as the size of the cache approaches the
	 * size of the data set being read, the IO bytes should go to 0 since all ops
	 * are answered from cache and in turn memoryBytesPerIOByte should go to
	 * infinity. An appropriate amount of cache can then be allocated based
	 * on empirical evidence of the affect of the cache size on the disk IO rate.
	 * 
	 * Such information would also likely be correlated against cache hit rates to
	 * make the best decision.
	 * 
	 * The following values relate the rss field of a differential 
	 * ProcessResourceRecord to the IO related info sourced from /proc/[pid]/io
	 */
	private BigDecimal memoryBytesPerReadChar, memoryBytesPerWriteChar, memoryBytesPerReadCall, 
						memoryBytesPerWriteCall, memoryBytesPerReadByte, memoryBytesPerWriteByte,
						memoryBytesPerIOChar, memoryBytesPerIOCall, memoryBytesPerIOByte;
	private BigDecimal ioCharsPerMemoryByte, ioCallsPerMemoryByte, ioBytesPerMemoryByte;
	/**
	 * Representations of the CPU used by a process related to disk I/O
	 * 
	 * An example of how this can be useful, the code for a map/reduce job may be 
	 * in development and there is a focus on implementing the job code to be
	 * most efficient for CPU consumption because the cluster is already running
	 * short of CPU resources. In such a case, it may be desirable to run the job
	 * repeatedly against the same input data set, with the expectation that the
	 * output data set should in turn always be the same, while controlling for
	 * all factors and changing only the implementation of the job code.
	 * 
	 * For more CPU efficient code implementations, cpuTicksPerIOByte should be
	 * lower compared to less CPU efficient implementations.  Of course, to be
	 * thorough, one would also need to watch memoryBytesPerIOByte to ensure
	 * that some increase in CPU efficiency for IO does not come at the cost
	 * of an unacceptable decrease in memory efficiency for IO as tracked in
	 * memoryBytePerIOByte.
	 */
	private BigDecimal cpuTicksPerReadChar, cpuTicksPerWriteChar, cpuTicksPerReadCall, 
						cpuTicksPerWriteCall, cpuTicksPerReadByte, cpuTicksPerWriteByte,
						cpuTicksPerIOChar, cpuTicksPerIOCall, cpuTicksPerIOByte;
	private BigDecimal ioCharsPerCpuTick, ioCallsPerCpuTick, ioBytesPerCpuTick;
	
	/**
	 * The previous two sections relate CPU <--> IO and Mem <--> IO, so this
	 * section finishes the trifecta, relating CPU <--> Mem.
	 * 
	 * Of course, the IO being referenced is solely disk IO.  Network IO
	 * is not tracked per process/thread (yet). So these relations are
	 * inherently incomplete for applications that can bottleneck on 
	 * network (which is basically any application that uses the
	 * network...).  Such is life, until the Linux kernel evolves 
	 * further.
	 * 
	 * Anyway, this section, as stated above, relates CPU <--> Mem, which
	 * may be useful, for example, when tuning the amount of heap space given
	 * to a JVM.  Perhaps there is a java app that reads input, applies
	 * processing, and writes output (kind of like this java app).  When the
	 * amount of heap space given to the app is too small, the JVM will use 
	 * more CPU resources to perform garbage collection.  To figure out 
	 * how much memory to give the JVM, a test could be repeated controlling
	 * for all factors and varying only JVM heap size.  In general, when heap
	 * size does not make java GC is an issue, the application should show 
	 * relatively predictable/constant cpuUtilPctPerMemoryByte.  But as the heap
	 * gets too low, more CPU is spent doing GC while (presuming the JVM
	 * doesn't free entirely and the system as a whole doesn't fall short
	 * of CPU) the IO byte rate stays constant.  So the app is doing the
	 * same rate of IO, but consuming significantly more CPU and
	 * significantly less memory.  It would be reasonable to assume then,
	 * provided other variables were successfully controlled, that the 
	 * increase in CPU consumption is directly related to the decrease
	 * in memory consumption.
	 * 
	 * When plotting heap size as x-axis against cpuUtilPctPerMemoryByte as y-axis,
	 * for the example scenario, you would expect to a visual representation of
	 * a sigmoid function with the mid point representing an ideal heap size.
	 */
	private BigDecimal cpuUtilPctPerMemoryByte;
	
	@Override
	public String getRecordType(){
		return Constants.PROCESS_EFFICIENCY_RECORD;
	}
	
	@Override
	public String getValueForQualifier(String qualifier) throws Exception {
		switch(qualifier){
		case "pid":
			return Integer.toString(pid);
		
		case "comm":
			return comm;
			
		default:
			throw new Exception("Qualifier " + qualifier + " is not valid for this record type");
		}
	}
	
	public ProcessEfficiencyRecord(ProcessResourceRecord r) throws Exception{
		super(r);
		if(r.getPreviousTimestamp()==-1){
			throw new Exception("ProcessEfficiencyRecord can not be produced from a raw ProcessResourceRecord");
		}
		this.comm = r.get_comm();
		this.pid = r.get_pid();
		this.starttime = r.get_starttime();
		
		this.ioBytesPerCpuTick = (new BigDecimal(r.get_io_bytes())).divide(new BigDecimal(r.getCpuTicks()));
		this.ioCallsPerCpuTick = (new BigDecimal(r.get_io_calls())).divide(new BigDecimal(r.getCpuTicks()));
		this.ioCharsPerCpuTick = (new BigDecimal(r.get_io_char())).divide(new BigDecimal(r.getCpuTicks()));
		
		this.ioBytesPerMemoryByte = (new BigDecimal(r.get_io_bytes())).divide(new BigDecimal(r.get_rss()), 8, BigDecimal.ROUND_HALF_UP);
		this.ioCallsPerMemoryByte = (new BigDecimal(r.get_io_bytes())).divide(new BigDecimal(r.get_rss()), 8, BigDecimal.ROUND_HALF_UP);
		this.ioCharsPerMemoryByte = (new BigDecimal(r.get_io_bytes())).divide(new BigDecimal(r.get_rss()), 8, BigDecimal.ROUND_HALF_UP);
		
		this.averageIOSize = r.getAverageIOSize();
		this.averageReadIOSize = r.getAverageIOReadSize();
		this.averageWriteIOSize = r.getAverageIOWriteSize();
		
		if(r.get_io_bytes().equals(biZero)){
			this.memoryBytesPerIOByte = null;
		} else {
			this.memoryBytesPerIOByte = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_io_bytes()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_io_calls().equals(biZero)){
			this.memoryBytesPerIOCall = null;
		} else {
			this.memoryBytesPerIOCall = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_io_calls()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_io_char().equals(biZero)){
			this.memoryBytesPerIOChar = null;
		} else {
			this.memoryBytesPerIOChar = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_io_char()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_read_bytes().equals(biZero)){
			this.memoryBytesPerReadByte = null;
		} else {
			this.memoryBytesPerReadByte = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_read_bytes()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_syscr().equals(biZero)){
			this.memoryBytesPerReadCall = null;
		} else {
			this.memoryBytesPerReadCall = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_syscr()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_rchar().equals(biZero)){
			this.memoryBytesPerReadChar = null;
		} else {
			this.memoryBytesPerReadChar = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_rchar()), 8, BigDecimal.ROUND_HALF_UP);
		}
		if (r.get_write_bytes().equals(biZero)){
			this.memoryBytesPerWriteByte = null;
		} else {
			this.memoryBytesPerWriteByte = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_write_bytes()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_syscw().equals(biZero)){
			this.memoryBytesPerWriteCall = null;
		} else {
			this.memoryBytesPerWriteCall= (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_syscw()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_wchar().equals(biZero)){
			this.memoryBytesPerWriteChar = null;
		} else {
			this.memoryBytesPerWriteChar = (new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_wchar()), 8, BigDecimal.ROUND_HALF_UP);
		} 
		
		if (r.get_io_bytes().equals(biZero)){
			this.cpuTicksPerIOByte = null;
		} else {
			this.cpuTicksPerIOByte = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_io_bytes()), 8, BigDecimal.ROUND_HALF_UP);
		} 
		
		if (r.get_io_calls().equals(biZero)){
			this.cpuTicksPerIOCall = null;
		} else {
			this.cpuTicksPerIOCall = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_io_calls()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_io_char().equals(biZero)){
			this.cpuTicksPerIOChar = null;
		} else {
			this.cpuTicksPerIOChar = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_io_char()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_read_bytes().equals(biZero)){
			this.cpuTicksPerReadByte = null;
		} else {
			this.cpuTicksPerReadByte = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_read_bytes()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_syscr().equals(biZero)){
			this.cpuTicksPerReadCall = null;
		} else {
			this.cpuTicksPerReadCall = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_syscr()), 8, BigDecimal.ROUND_HALF_UP);
		} 
		
		if (r.get_rchar().equals(biZero)){
			this.cpuTicksPerReadChar = null;
		} else {
			this.cpuTicksPerReadChar = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_rchar()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_write_bytes().equals(biZero)){
			this.cpuTicksPerWriteByte = null;
		} else {
			this.cpuTicksPerWriteByte = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_write_bytes()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		if (r.get_syscw().equals(biZero)){
			this.cpuTicksPerWriteCall = null;
		} else {
			this.cpuTicksPerWriteCall = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_syscw()), 8, BigDecimal.ROUND_HALF_UP);
		} 
		
		if (r.get_wchar().equals(biZero)){
			this.cpuTicksPerWriteChar = null;
		} else {
			this.cpuTicksPerWriteChar = (new BigDecimal(r.getCpuTicks())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP).divide(new BigDecimal(r.get_wchar()), 8, BigDecimal.ROUND_HALF_UP);
		}
		
		this.cpuUtilPctPerMemoryByte = (new BigDecimal(r.getCpuUtilPct())).divide((new BigDecimal(r.get_rss())).divide(new BigDecimal(Long.toString(r.getDurationms())), 8, BigDecimal.ROUND_HALF_UP), 8, BigDecimal.ROUND_HALF_UP);
	}

	@Override
	public String toString() {
		/*
		return super.toString() + " " + Constants.PROCESS_RESOURCE_RECORD + " comm:" + comm + " pid:" + pid + " starttime:" + starttime + 
				" memoryBytesPer iob:" + memoryBytesPerIOByte + " ioc:" + memoryBytesPerIOCall + " ioch: " + memoryBytesPerIOChar + 
				" rb:" + memoryBytesPerReadByte + " rc:" + memoryBytesPerReadCall + " rch: " + memoryBytesPerReadChar + " wb:" + 
				memoryBytesPerWriteByte + " wc:" + memoryBytesPerWriteCall + " wch:" + memoryBytesPerWriteChar + " cpuTicksPer iob:" + 
				cpuTicksPerIOByte + " ioc:" + cpuTicksPerIOCall + " ioch:" + cpuTicksPerIOChar + " rb:" + cpuTicksPerReadByte + 
				" rc:" + cpuTicksPerReadCall + " rch:" + cpuTicksPerReadChar + " wb:" + cpuTicksPerWriteByte + " wc:" + 
				cpuTicksPerWriteCall + " wch:" + cpuTicksPerWriteChar + " cpuUtil%PerMemByte:" + cpuUtilPctPerMemoryByte;
		*/
		return super.toString() + " " + Constants.PROCESS_RESOURCE_RECORD + " comm:" + comm + " pid:" + pid + 
				" perCpuTick ioB:" + ioBytesPerCpuTick + " ioC:" + ioCallsPerCpuTick + 
				" perMemByte ioB:" + ioBytesPerMemoryByte + " ioC:" + ioCallsPerMemoryByte + 
				" cpuUtil%PerMemByte:" + cpuUtilPctPerMemoryByte + " avgSize io:" + averageIOSize + " ior:" + averageReadIOSize + 
				" iow:" + averageWriteIOSize;


	}
	
	public String get_comm(){
		return comm;
	}
	public int get_pid(){
		return pid;
	}
	public long get_starttime(){
		return starttime;
	}
	public BigDecimal getmemoryBytesPerIOByte(){
		return memoryBytesPerIOByte;
	}
	public BigDecimal getmemoryBytesPerIOCall(){
		return memoryBytesPerIOCall;
	}
	public BigDecimal getmemoryBytesPerIOChar(){
		return memoryBytesPerIOChar;
	}
	public BigDecimal getmemoryBytesPerReadByte(){
		return memoryBytesPerReadByte;
	}
	public BigDecimal getmemoryBytesPerReadCall(){
		return memoryBytesPerReadCall;
	}
	public BigDecimal getmemoryBytesPerReadChar(){
		return memoryBytesPerReadChar;
	}
	public BigDecimal getmemoryBytesPerWriteByte(){
		return memoryBytesPerWriteByte;
	}
	public BigDecimal getmemoryBytesPerWriteCall(){
		return memoryBytesPerWriteCall;
	}
	public BigDecimal getmemoryBytesPerWriteChar(){
		return memoryBytesPerWriteChar;
	}
	public BigDecimal getcpuTicksPerIOByte(){
		return cpuTicksPerIOByte;
	}
	public BigDecimal getcpuTicksPerIOCall(){
		return cpuTicksPerIOCall;
	}
	public BigDecimal getcpuTicksPerIOChar(){
		return cpuTicksPerIOChar;
	}
	public BigDecimal getcpuTicksPerReadByte(){
		return cpuTicksPerReadByte;
	}
	public BigDecimal getcpuTicksPerReadCall(){
		return cpuTicksPerReadCall;
	}
	public BigDecimal getcpuTicksPerReadChar(){
		return cpuTicksPerReadChar;
	}
	public BigDecimal getcpuTicksPerWriteByte(){
		return cpuTicksPerWriteByte;
	}
	public BigDecimal getcpuTicksPerWriteCall(){
		return cpuTicksPerWriteCall;
	}
	public BigDecimal getcpuTicksPerWriteChar(){
		return cpuTicksPerWriteChar;
	}
	public BigDecimal getcpuUtilPctPerMemoryByte(){
		return cpuUtilPctPerMemoryByte;
	}

}
