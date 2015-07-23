package com.mapr.distiller.server.recordtypes;

import java.io.File;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import com.mapr.distiller.server.queues.RecordQueue;

public class TcpConnectionStatRecord extends Record {
	/**
	 * DERIVED VALUES
	 */

	/**
	 * RAW VALUES
	 */
	private long localIp, remoteIp, rxQ, txQ;
	private int localPort, remotePort, pid;
	
	/**
	 * CONSTRUCTORS
	 */
	public TcpConnectionStatRecord(String[] parts, int pid){
		super(System.currentTimeMillis());
		this.localIp = Long.parseLong(parts[1].split(":")[0], 16);
		this.localPort = Integer.parseInt(parts[1].split(":")[1], 16);
		this.remoteIp = Long.parseLong(parts[2].split(":")[0], 16);
		this.remotePort = Integer.parseInt(parts[2].split(":")[1], 16);
		this.rxQ = Integer.parseInt(parts[4].split(":")[1], 16);
		this.txQ = Integer.parseInt(parts[4].split(":")[0], 16);
		this.pid = pid;
	}

	/**
	 * PRODUCE RECORD METHODS
	 */
	public static boolean produceRecords(RecordQueue tcp_connection_stats){
		try {
			RandomAccessFile proc_net_tcp = null;
			long startTime = System.currentTimeMillis();
			String line = null;
			String socketId;
			int processesChecked=0, fdChecked=0, recordsGenerated=0;
			HashMap<String,String[]> recordMap = new HashMap<String,String[]>(32000);
			try{
				proc_net_tcp = new RandomAccessFile("/proc/net/tcp", "r");
				if((line = proc_net_tcp.readLine()) != null ){
					while( (line = proc_net_tcp.readLine()) != null ) {
						String parts[] = line.trim().split("\\s+");
						if(parts[3].equals("01")){
							socketId = parts[9];
							recordMap.put(socketId, parts);
						}
					}
				}
				proc_net_tcp.close();
			} catch (Exception e){
				System.err.println("Failed to parse line from /proc/net/tcp: " + line);
				e.printStackTrace();
				return false;
			} finally {
				try {
					proc_net_tcp.close();
				} catch (Exception e) {}
			}
			
			FilenameFilter fnFilter = new FilenameFilter() {
				public boolean accept(File dir, String name) {
					if(name.charAt(0) >= '1' && name.charAt(0) <= '9'){
						return true;
					}
					return false;
				}
			};
			File ppFile = new File("/proc");
			File[] pPaths = ppFile.listFiles(fnFilter);
			if(pPaths == null) return false;
			processesChecked = pPaths.length;
			long timestamp = System.currentTimeMillis();
			
			//For each process in /proc
			for (int pn = 0; pn<pPaths.length; pn++){
				int pid = Integer.parseInt(pPaths[pn].getName());
				String fdPathStr = pPaths[pn].toString() + "/fd";
				File fdFile = new File(fdPathStr);
				File[] fdPaths = fdFile.listFiles(fnFilter);
				if(fdPaths != null) {
					//For each file descriptor in /proc/[pid]/fd
					fdChecked += fdPaths.length;
					for (int x=0; x<fdPaths.length; x++){
						try{
							String linkTarget = Files.readSymbolicLink(Paths.get(fdPaths[x].toString())).toString();
							if(linkTarget.startsWith("socket:[")){
								socketId = linkTarget.split("\\[")[1].split("\\]")[0];
								String[] parts = null;
								if( (parts = recordMap.get(socketId)) != null )
									if(tcp_connection_stats.put(new TcpConnectionStatRecord(parts, pid)))
										recordsGenerated++;
								//System.err.println("pid: " + pid + " socketId: " + socketId + " path: " + fdPaths[x].toString());
							}
						} catch (Exception e){}
					}
				}
			}
			//long duration = System.currentTimeMillis() - startTime;
			//System.err.println("Generated " + recordsGenerated + " output records from " + processesChecked + " processes and " + 
			//					fdChecked + " file descriptors in " + duration + " ms");
		} catch (Exception e) {
			System.err.println("Failed to generate TcpConnectionStatRecord");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * OTHER METHODS
	 */
	public String toString(){
		return super.toString() + " tcp.connection.stats " + longIpToString(localIp) + ":" + localPort + " " + longIpToString(remoteIp) + ":" + remotePort + 
				" txQ:" + txQ + " rxQ:" + rxQ + " pid:" + pid;
	}
	public static String longIpToString(long ip){
		String ipStr = 	String.valueOf(ip % 256) + "." + 
						String.valueOf(ip / 256 % 256) + "." + 
						String.valueOf(ip / 65536 % 256) + "." + 
						String.valueOf(ip / 16777216);
		return ipStr;
	}
}