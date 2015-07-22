package com.mapr.distiller.server.recordtypes;

import java.io.RandomAccessFile;
import java.math.BigInteger;
import com.mapr.distiller.server.queues.RecordQueue;

public class NetworkInterfaceRecord extends Record {
	/**
	 * DERIVED VALUES
	 */

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
	public NetworkInterfaceRecord(String ifName) throws Exception {
		//This constructor takes "ifName", which should be the name of a network interface.
		//The raw values of the record are set according to content under /sys/class/net/ifName
		//If we fail to read in one of the raw values then this constructor throws an exception.
		//If this constructor throws an exception, the upstream should consider it as a failure to generate a record for the interface.
		super(System.currentTimeMillis());
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
	public static boolean produceRecord(RecordQueue network_interfaces, String ifName){
		try{
			network_interfaces.put(new NetworkInterfaceRecord(ifName));
		} catch (Exception e) {
			System.err.println("Failed to generate a NetworkInterfaceRecord");
			e.printStackTrace();
			return false;
		}
		return  true;
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
			throw new Exception("Caught an exception while reading BigInteger file " + path);
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
}
