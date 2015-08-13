package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.TcpConnectionStatRecord;

public class TcpConnectionStatRecordProcessor implements
		RecordProcessor<Record> {

	public String getName(){
		return "TcpConnectionStatRecordProcessor";
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		TcpConnectionStatRecord currentRecord = (TcpConnectionStatRecord) record;

		switch (metric) {
		case "localIp":
			return currentRecord.getLocalIp() == Long.parseLong(thresholdValue);

		case "remoteIp":
			return currentRecord.getRemoteIp() == Long
					.parseLong(thresholdValue);

		case "rxQ":
			return currentRecord.get_rxQ() == Long.parseLong(thresholdValue);

		case "txQ":
			return currentRecord.get_txQ() == Long.parseLong(thresholdValue);

		case "localPort":
			return currentRecord.getLocalPort() == Integer
					.parseInt(thresholdValue);

		case "remotePort":
			return currentRecord.getRemotePort() == Integer
					.parseInt(thresholdValue);

		case "pid":
			return currentRecord.get_pid() == Integer.parseInt(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in TcpConnectionStatRecord");
		}
	}

	@Override
	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		TcpConnectionStatRecord currentRecord = (TcpConnectionStatRecord) record;

		switch (metric) {
		case "rxQ":
			return currentRecord.get_rxQ() > Long.parseLong(thresholdValue);

		case "txQ":
			return currentRecord.get_txQ() > Long.parseLong(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in TcpConnectionStatRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		TcpConnectionStatRecord currentRecord = (TcpConnectionStatRecord) record;

		switch (metric) {
		case "rxQ":
			return currentRecord.get_rxQ() < Long.parseLong(thresholdValue);

		case "txQ":
			return currentRecord.get_txQ() < Long.parseLong(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in TcpConnectionStatRecord");
		}
	}

	@Override
	public TcpConnectionStatRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new TcpConnectionStatRecord((TcpConnectionStatRecord) rec1,
				(TcpConnectionStatRecord) rec2);
	}
}
