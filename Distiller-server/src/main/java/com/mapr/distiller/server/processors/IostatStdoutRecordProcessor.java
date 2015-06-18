package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.IoStatRecord;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.WholeLineRecord;

public class IostatStdoutRecordProcessor implements RecordProcessor {
	// command[] defines the command whose output this RecordProcessor can
	// succesfully process
	public static final String command[] = new String[] { "iostat", "-cdmx",
			"1" };
	long timestamp = 0l, previousTimestamp = 0l;
	boolean processCpuLine = false;
	boolean processDeviceLines = false;
	boolean firstIteration = true;
	boolean foundFirstCpuHeader = false;

	public IostatStdoutRecordProcessor() {
	}

	public Record process(Record record) throws Exception {
		return process((WholeLineRecord) record);
	}

	public IoStatRecord process(WholeLineRecord wholeLineRecord) {
		String currentRecord = wholeLineRecord.getLine();
		if (firstIteration) {
			if (lineIsCpuHeader(currentRecord) && !foundFirstCpuHeader) {
				foundFirstCpuHeader = true;
				timestamp = System.currentTimeMillis();
			} else if (lineIsCpuHeader(currentRecord)) {
				firstIteration = false;
				processCpuLine = true;
				previousTimestamp = timestamp;
				timestamp = System.currentTimeMillis();
			}
		} else {
			if (lineIsCpuHeader(currentRecord)) {
				processCpuLine = true;
				previousTimestamp = timestamp;
				timestamp = System.currentTimeMillis();
			} else if (processCpuLine) {
				processCpuLine = false;
				String[] substrings = currentRecord.split("\\s+");
				IoStatRecord outputRecord = new IoStatRecord("cpu");
				outputRecord.setTimestamp(timestamp);
				outputRecord.setPreviousTimestamp(previousTimestamp);
				outputRecord.setDurationms(timestamp - previousTimestamp);
				outputRecord.getCpu()
						.setUser(Double.parseDouble(substrings[1]));
				outputRecord.getCpu().setSystem(
						Double.parseDouble(substrings[3]));
				outputRecord.getCpu()
						.setIdle(Double.parseDouble(substrings[6]));
				return outputRecord;
			} else if (lineIsDeviceHeader(currentRecord)) {
				processDeviceLines = true;
			} else if (processDeviceLines) {
				if (isEmptyLine(currentRecord)) {
					processDeviceLines = false;
				} else {
					String[] substrings = currentRecord.split("\\s+");
					IoStatRecord outputRecord = new IoStatRecord("device");
					outputRecord.setTimestamp(timestamp);
					outputRecord.setPreviousTimestamp(previousTimestamp);
					outputRecord.setDurationms(timestamp - previousTimestamp);
					outputRecord.getDevice().setName(substrings[0]);
					outputRecord.getDevice().setNumReadReqs(
							Integer.parseInt(substrings[3].split("\\.")[0]));
					outputRecord.getDevice().setNumWriteReqs(
							Integer.parseInt(substrings[4].split("\\.")[0]));
					outputRecord.getDevice().setReadMB(
							Double.parseDouble(substrings[5]));
					outputRecord.getDevice().setWriteMB(
							Double.parseDouble(substrings[6]));
					outputRecord.getDevice().setAvgWaitms(
							Double.parseDouble(substrings[9]));
					outputRecord.getDevice().setUtilizationPct(
							Double.parseDouble(substrings[11]));
					outputRecord.getDevice().setAvgReadSizeBytes(
							(int) (1048756d * outputRecord.getDevice()
									.getReadMB() / (double) outputRecord
									.getDevice().getNumReadReqs()));
					outputRecord.getDevice().setAvgWriteSizeBytes(
							(int) (1048576d * outputRecord.getDevice()
									.getWriteMB() / (double) outputRecord
									.getDevice().getNumWriteReqs()));
					return outputRecord;
				}
			}

		}
		return null;
	}

	private boolean lineIsCpuHeader(String line) {
		if (line.compareTo("avg-cpu:  %user   %nice %system %iowait  %steal   %idle") == 0)
			return true;
		return false;
	}

	private boolean lineIsDeviceHeader(String line) {
		if (line.compareTo("Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await  svctm  %util") == 0)
			return true;
		return false;
	}

	private boolean isEmptyLine(String line) {
		if (line.compareTo("") == 0)
			return true;
		return false;
	}

}
