package com.mapr.distiller.server.recordtypes;

import java.io.RandomAccessFile;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.queues.RecordQueue;

public class NetworkInterfaceRecord extends Record {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(NetworkInterfaceRecord.class);
	
	/**
	 * DERIVED VALUES
	 */
	private double 	rxPacketsPerSecond, rxBytesPerSecond, rxUtilizationPct,
					txPacketsPerSecond, txBytesPerSecond, txUtilizationPct;
					
	/**
	 * RAW VALUES
	 */
	private String name, duplex;
	private boolean fullDuplex;
	private int carrier, speed, tx_queue_len;
	private BigInteger collisions, rx_bytes, rx_dropped, rx_errors, rx_packets, tx_bytes, tx_dropped, tx_errors, tx_packets;	

	/**
	 * CONSTRUCTORS
	 */
	public NetworkInterfaceRecord(NetworkInterfaceRecord rec1, NetworkInterfaceRecord rec2) throws Exception{
		NetworkInterfaceRecord oldRecord, newRecord;
		
		//Check the input records to ensure they can be diff'd.
		if(rec1.getRxPacketsPerSecond()!=-1d || rec2.getRxPacketsPerSecond()!=-1d || rec1.getPreviousTimestamp()!=-1l || rec2.getPreviousTimestamp()!=-1l)
			throw new Exception("Differential DiskstatRecord can only be generated from raw DiskstatRecords");
		if(rec1.getTimestamp() == rec2.getTimestamp())
			throw new Exception("Can not generate differential DiskstatRecord from input records with matching timestamp values");
		//if(rec1.getCarrier()!=1 || rec2.getCarrier()!=1)
		//	throw new Exception("Can not generate differential DiskstatRecord from input records where carrier was not detected");
		if(rec1.getSpeed() != rec2.getSpeed() || rec1.getSpeed()<1)
			throw new Exception("Can not generate differential DiskstatRecord from input records where speed does not match");
		if(rec1.getFullDuplex() != rec2.getFullDuplex())
			throw new Exception("Can not generate differential DiskstatRecord from input records where duplex does not match");
		if(!rec1.getName().equals(rec2.getName()))
			throw new Exception("Can not generate differential DiskstatRecord from input records where interface name does not match");
		
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
		this.name = newRecord.getName();
		this.duplex = newRecord.getDuplex();
		this.fullDuplex = newRecord.getFullDuplex();
		this.carrier = newRecord.getCarrier();
		this.speed = newRecord.getSpeed();
		this.tx_queue_len = newRecord.get_tx_queue_len();
		
		//Differential values:
		this.collisions = newRecord.get_collisions().subtract(oldRecord.get_collisions());
		this.rx_bytes = newRecord.get_rx_bytes().subtract(oldRecord.get_rx_bytes());
		this.rx_errors = newRecord.get_rx_errors().subtract(oldRecord.get_rx_errors());
		this.rx_dropped = newRecord.get_rx_dropped().subtract(oldRecord.get_rx_dropped());
		this.rx_packets = newRecord.get_rx_packets().subtract(oldRecord.get_rx_packets());
		this.tx_bytes = newRecord.get_tx_bytes().subtract(oldRecord.get_tx_bytes());
		this.tx_errors = newRecord.get_tx_errors().subtract(oldRecord.get_tx_errors());
		this.tx_dropped = newRecord.get_tx_dropped().subtract(oldRecord.get_tx_dropped());
		this.tx_packets = newRecord.get_tx_packets().subtract(oldRecord.get_tx_packets());
		
		//Derived values:
		this.rxPacketsPerSecond = 1000d * this.rx_packets.doubleValue() / this.getDurationms();
		this.rxBytesPerSecond = 1000d * this.rx_bytes.doubleValue() / this.getDurationms();
		this.txPacketsPerSecond = 1000d * this.tx_packets.doubleValue() / this.getDurationms();
		this.txBytesPerSecond = 1000d * this.tx_bytes.doubleValue() / this.getDurationms();
		if(this.fullDuplex){
			this.rxUtilizationPct = this.rx_bytes.doubleValue() / ((double)this.speed * 131.072d * (double)this.getDurationms());
			this.txUtilizationPct = this.tx_bytes.doubleValue() / ((double)this.speed * 131.072d * (double)this.getDurationms());
		} else {
			this.rxUtilizationPct = this.rx_bytes.add(this.tx_bytes).doubleValue() / ((double)this.speed * 131.072d * (double)this.getDurationms());
			this.txUtilizationPct = this.rxUtilizationPct;
		}
	}
	public NetworkInterfaceRecord(String ifName) throws Exception {
		//This constructor takes "ifName", which should be the name of a network interface.
		//The raw values of the record are set according to content under /sys/class/net/ifName
		//If we fail to read in one of the raw values then this constructor throws an exception.
		//If this constructor throws an exception, the upstream should consider it as a failure to generate a record for the interface.
		super(System.currentTimeMillis());
		this.rxPacketsPerSecond=-1d;
		this.rxBytesPerSecond=-1d;
		this.rxUtilizationPct=-1d;
		this.txPacketsPerSecond=-1d;
		this.txBytesPerSecond=-1d;
		this.txUtilizationPct=-1d;
		String basePath = "/sys/class/net/" + ifName + "/";
		this.name = ifName;
		this.duplex = readStringFromFile(basePath + "duplex");
		if(this.duplex.equals("full")){
			this.fullDuplex = true;
		} else {
			this.fullDuplex = false;
		}
		this.carrier = readIntFromFile(basePath + "carrier");
		this.speed = readIntFromFile(basePath + "speed");
		this.tx_queue_len = readIntFromFile(basePath + "tx_queue_len");
		this.collisions = readBigIntegerFromFile(basePath + "statistics/collisions");
		this.rx_bytes = readBigIntegerFromFile(basePath + "statistics/rx_bytes");
		this.rx_dropped = readBigIntegerFromFile(basePath + "statistics/rx_dropped");
		this.rx_errors = readBigIntegerFromFile(basePath + "statistics/rx_errors");
		this.rx_packets = readBigIntegerFromFile(basePath + "statistics/rx_packets");
		this.tx_bytes = readBigIntegerFromFile(basePath + "statistics/tx_bytes");
		this.tx_dropped = readBigIntegerFromFile(basePath + "statistics/tx_dropped");
		this.tx_errors = readBigIntegerFromFile(basePath + "statistics/tx_errors");
		this.tx_packets = readBigIntegerFromFile(basePath + "statistics/tx_packets");
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	public static int[] produceRecord(RecordQueue outputQueue, String producerName, String ifName){
		int[] ret = new int[] {0, 0, 0, 0};
		NetworkInterfaceRecord record = null;
		try{
			record = new NetworkInterfaceRecord(ifName);
		} catch (Exception e) {
			LOG.error("Failed to generate a NetworkInterfaceRecord");
			e.printStackTrace();
			ret[2] = 1;
		}
		if(record != null && !outputQueue.put(producerName, record)){
			ret[3]=1;
			LOG.error("Failed to put NetworkInterfaceRecord into output queue " + outputQueue.getQueueName() + 
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
	private static String readStringFromFile(String path) throws Exception {
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(path, "r");
			String line = file.readLine();
			return line.trim().split("\\s+")[0];
		} catch (Exception e) {
			throw new Exception("Caught an exception while reading String from file " + path, e);
		} finally {
			try{
				file.close();
			} catch (Exception e) {}
		}
	}
	private static BigInteger readBigIntegerFromFile(String path) throws Exception {
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(path, "r");
			String line = file.readLine();
			return new BigInteger(line.trim().split("\\s+")[0]);
		} catch (Exception e) {
			throw new Exception("Caught an exception while reading BigInteger file " + path);
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
	public String toString() {
		return super.toString() + " network.interface name:" + name + " dup:" + fullDuplex + 
				" car:" + carrier + " sp:" + speed + " ql:" + tx_queue_len + " co:" + collisions + 
				" rx b:" + rx_bytes + " d:" + rx_dropped + " e:" + rx_errors + " p:" + rx_packets + 
				" tx b:" + tx_bytes + " d:" + tx_dropped + " e:" + tx_errors + " p:" + tx_packets;
	}
	public double getRxPacketsPerSecond(){
		return rxPacketsPerSecond;
	}
	public double getRxBytesPerSecond(){
		return rxBytesPerSecond;
	}
	public double getRxUtilizationPct(){
		return rxUtilizationPct;
	}
	public double getTxPacketsPerSecond(){
		return txPacketsPerSecond;
	}
	public double getTxBytesPerSecond(){
		return txBytesPerSecond;
	}
	public double getTxUtilizationPct(){
		return txUtilizationPct;
	}
	public String getName(){
		return name;
	}
	public String getDuplex(){
		return duplex;
	}
	public boolean getFullDuplex(){
		return fullDuplex;
	}
	public int getCarrier(){
		return carrier;
	}
	public int getSpeed(){
		return speed;
	}
	public int get_tx_queue_len(){
		return tx_queue_len;
	}
	public BigInteger get_collisions(){
		return collisions;
	}
	public BigInteger get_rx_bytes(){
		return rx_bytes;
	}
	public BigInteger get_rx_dropped(){
		return rx_dropped;
	}
	public BigInteger get_rx_errors(){
		return rx_errors;
	}
	public BigInteger get_rx_packets(){
		return rx_packets;
	}
	public BigInteger get_tx_bytes(){
		return tx_bytes;
	}
	public BigInteger get_tx_dropped(){
		return tx_dropped;
	}
	public BigInteger get_tx_errors(){
		return tx_errors;
	}
	public BigInteger get_tx_packets(){
		return tx_packets;
	}	
	
	@Override
	public String getValueForQualifier(String qualifier) throws Exception {
		switch(qualifier){
		case "name":
			return name;
		default:
			throw new Exception("Qualifier " + qualifier + " is not valid for this record type");
		}
	}

}
