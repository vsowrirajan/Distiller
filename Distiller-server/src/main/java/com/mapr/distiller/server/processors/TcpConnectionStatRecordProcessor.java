package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.recordtypes.TcpConnectionStatRecord;

public class TcpConnectionStatRecordProcessor implements
		RecordProcessor<Record> {

	private static final Logger LOG = LoggerFactory
			.getLogger(TcpConnectionStatRecordProcessor.class);

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric)
			throws Exception {
		if (rec1.getPreviousTimestamp() == -1l
				|| rec2.getPreviousTimestamp() == -1l)
			throw new Exception(
					"TcpConnectionStatRecords can only be diff'd from non-raw TcpConnectionStatRecords");

		TcpConnectionStatRecord oldRecord, newRecord;
		if (rec1.getTimestamp() < rec2.getTimestamp()) {
			oldRecord = (TcpConnectionStatRecord) rec1;
			newRecord = (TcpConnectionStatRecord) rec2;
		} else {
			oldRecord = (TcpConnectionStatRecord) rec2;
			newRecord = (TcpConnectionStatRecord) rec1;
		}

		if (oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception(
					"Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");

		if (oldRecord.getLocalIp() != newRecord.getLocalIp()
				|| oldRecord.getRemoteIp() != newRecord.getRemoteIp()
				|| oldRecord.getLocalPort() != newRecord.getLocalPort()
				|| oldRecord.getRemotePort() != newRecord.getRemotePort()
				|| oldRecord.get_pid() != newRecord.get_pid())
			throw new Exception(
					"Can not diff input records for different connections");

		switch (metric) {
		case "rxQ":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "BigInteger", newRecord.get_rxQ()
							.subtract(oldRecord.get_rxQ()));

		case "txQ":
			return new DifferentialValueRecord(
					oldRecord.getPreviousTimestamp(), oldRecord.getTimestamp(),
					newRecord.getPreviousTimestamp(), newRecord.getTimestamp(),
					getName(), metric, "BigInteger", newRecord.get_txQ()
							.subtract(oldRecord.get_txQ()));

		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable in TcpConnectionStatRecordProcessor");
		}
	}

	public String getName() {
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
			return currentRecord.get_rxQ().equals(
					new BigInteger(thresholdValue));

		case "txQ":
			return currentRecord.get_txQ().equals(
					new BigInteger(thresholdValue));

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
			return currentRecord.get_rxQ().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "txQ":
			return currentRecord.get_txQ().compareTo(
					new BigInteger(thresholdValue)) == 1;

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
			return currentRecord.get_rxQ().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "txQ":
			return currentRecord.get_txQ().compareTo(
					new BigInteger(thresholdValue)) == -1;

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
