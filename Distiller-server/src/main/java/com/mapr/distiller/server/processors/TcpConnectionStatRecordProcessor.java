package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.TcpConnectionStatRecord;

public class TcpConnectionStatRecordProcessor implements
		Thresholdable<TcpConnectionStatRecord>, MovingAverageable<TcpConnectionStatRecord> {
	
	public boolean isNotEqual(TcpConnectionStatRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	@Override
	public boolean isEqual(TcpConnectionStatRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "localIp":
			return record.getLocalIp() == Long.parseLong(thresholdValue);

		case "remoteIp":
			return record.getRemoteIp() == Long.parseLong(thresholdValue);
		
		case "rxQ":
			return record.get_rxQ() == Long.parseLong(thresholdValue);

		case "txQ":
			return record.get_txQ() == Long.parseLong(thresholdValue);

		case "localPort":
			return record.getLocalPort() == Integer.parseInt(thresholdValue);
		
		case "remotePort":
			return record.getRemotePort() == Integer.parseInt(thresholdValue);
			
		case "pid":
			return record.get_pid() == Integer.parseInt(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in TcpConnectionStatRecord");
		}
	}
	
	@Override
	public boolean isAboveThreshold(TcpConnectionStatRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "rxQ":
			return record.get_rxQ() > Long.parseLong(thresholdValue);

		case "txQ":
			return record.get_txQ() > Long.parseLong(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in TcpConnectionStatRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(TcpConnectionStatRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "rxQ":
			return record.get_rxQ() < Long.parseLong(thresholdValue);

		case "txQ":
			return record.get_txQ() < Long.parseLong(thresholdValue);

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in TcpConnectionStatRecord");
		}
	}

	@Override
	public TcpConnectionStatRecord movingAverage(TcpConnectionStatRecord rec1,
			TcpConnectionStatRecord rec2) throws Exception{
		return new TcpConnectionStatRecord(rec1, rec2);
	}
}
