package com.mapr.distiller.server.recordtypes;

public class IoStatRecord extends Record {
	private CpuUtilizationSample cpu = null;
	private DeviceUtilizationSample device = null;
	private String type;
	
	public IoStatRecord(String type){
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
	
	public CpuUtilizationSample getCpu() {
		return cpu;
	}

	public void setCpu(CpuUtilizationSample cpu) {
		this.cpu = cpu;
	}

	public DeviceUtilizationSample getDevice() {
		return device;
	}

	public void setDevice(DeviceUtilizationSample device) {
		this.device = device;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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

