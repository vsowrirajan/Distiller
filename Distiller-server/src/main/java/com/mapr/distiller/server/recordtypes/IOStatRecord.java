package com.mapr.distiller.server.recordtypes;

public class IOStatRecord extends Record {
	private CpuUtilizationSample cpu = null;
	private DeviceUtilizationSample device = null;
	private String type;
	
	public IOStatRecord(String type){
		if(type.equals("cpu")) {
			cpu = new CpuUtilizationSample();
			this.type = type;
		} else if (type.equals("device")) {
			device = new DeviceUtilizationSample();
			this.type = type;
		} else {
			System.err.println("IostatRecord: Unknown record type requested: " + type);
			Exception e = new Exception();
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString(){
		if(type.equals("cpu")){
			return super.toString() + " SystemCpu idle:" + cpu.getIdle() + " user:" + cpu.getUser() + " system: " + cpu.getSystem();
		} else if (type.equals("device")) {
			return super.toString() + " DeviceUsage dev:" + device.getName() + " nrr:" + device.getNumReadReqs() + " nwr:" + device.getNumWriteReqs() +
					" avgrs:" + device.getAvgReadSizeBytes() + "b avgws:" + device.getAvgWriteSizeBytes() + "b rMB:" +
					device.getReadMB() + " wMB" + device.getWriteMB() + " await:" + device.getAvgWaitms() + "ms util:" + device.getUtilizationPct() + "%";
		}
		return super.toString() + " type:" + type;
	}
}

