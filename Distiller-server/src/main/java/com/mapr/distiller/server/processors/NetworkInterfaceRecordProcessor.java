package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.NetworkInterfaceRecord;
import com.mapr.distiller.server.recordtypes.Record;

import java.math.BigInteger;

public class NetworkInterfaceRecordProcessor implements RecordProcessor<Record> {

	public String getName(){
		return "NetworkInterfaceRecordProcessor";
	}
	
	public boolean isNotEqual(Record record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}

	public boolean isEqual(Record record, String metric, String thresholdValue)
			throws Exception {

		NetworkInterfaceRecord currentRecord = (NetworkInterfaceRecord) record;

		switch (metric) {
		case "name":
			return currentRecord.getName().equals(thresholdValue);

		case "duplex":
			return currentRecord.getDuplex().equals(thresholdValue);

		case "fullDuplex":
			return currentRecord.getFullDuplex() == Boolean
					.parseBoolean(thresholdValue);

		case "carrier":
			return currentRecord.getCarrier() == Integer
					.parseInt(thresholdValue);

		case "speed":
			return currentRecord.getSpeed() == Integer.parseInt(thresholdValue);

		case "tx_queue_len":
			return currentRecord.get_tx_queue_len() == Integer
					.parseInt(thresholdValue);

		case "hasProblems":
			return (!currentRecord.getFullDuplex()
					|| currentRecord.getCarrier() != 1
					|| currentRecord.getSpeed() < 1000
					|| currentRecord.get_tx_queue_len() < 1000
					|| !currentRecord.get_collisions().equals(
							new BigInteger("0"))
					|| !currentRecord.get_rx_dropped().equals(
							new BigInteger("0"))
					|| !currentRecord.get_rx_errors().equals(
							new BigInteger("0"))
					|| !currentRecord.get_tx_dropped().equals(
							new BigInteger("0")) || !currentRecord
					.get_tx_errors().equals(new BigInteger("0")));

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in NetworkInterfaceRecord");
		}
	}

	public boolean isAboveThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		NetworkInterfaceRecord currentRecord = (NetworkInterfaceRecord) record;

		switch (metric) {
		case "speed":
			return currentRecord.getSpeed() > Integer.parseInt(thresholdValue);

		case "rxPacketsPerSecond":
			if (currentRecord.getRxPacketsPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getRxPacketsPerSecond() > Double
						.parseDouble(thresholdValue);

		case "rxBytesPerSecond":
			if (currentRecord.getRxBytesPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getRxBytesPerSecond() > Double
						.parseDouble(thresholdValue);

		case "rxUtilizationPct":
			if (currentRecord.getRxUtilizationPct() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getRxUtilizationPct() > Double
						.parseDouble(thresholdValue);

		case "txPacketsPerSecond":
			if (currentRecord.getTxPacketsPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getTxPacketsPerSecond() > Double
						.parseDouble(thresholdValue);

		case "txBytesPerSecond":
			if (currentRecord.getTxBytesPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getTxBytesPerSecond() > Double
						.parseDouble(thresholdValue);

		case "txUtilizationPct":
			if (currentRecord.getTxUtilizationPct() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getTxUtilizationPct() > Double
						.parseDouble(thresholdValue);

		case "collisions":
			return currentRecord.get_collisions().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "rx_dropped":
			return currentRecord.get_rx_dropped().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "rx_errors":
			return currentRecord.get_rx_errors().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "tx_dropped":
			return currentRecord.get_tx_dropped().compareTo(
					new BigInteger(thresholdValue)) == 1;

		case "tx_errors":
			return currentRecord.get_tx_errors().compareTo(
					new BigInteger(thresholdValue)) == 1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in NetworkInterfaceRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(Record record, String metric,
			String thresholdValue) throws Exception {

		NetworkInterfaceRecord currentRecord = (NetworkInterfaceRecord) record;

		switch (metric) {
		case "speed":
			return currentRecord.getSpeed() < Integer.parseInt(thresholdValue);

		case "rxPacketsPerSecond":
			if (currentRecord.getRxPacketsPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getRxPacketsPerSecond() < Double
						.parseDouble(thresholdValue);

		case "rxBytesPerSecond":
			if (currentRecord.getRxBytesPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getRxBytesPerSecond() < Double
						.parseDouble(thresholdValue);

		case "rxUtilizationPct":
			if (currentRecord.getRxUtilizationPct() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getRxUtilizationPct() < Double
						.parseDouble(thresholdValue);

		case "txPacketsPerSecond":
			if (currentRecord.getTxPacketsPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getTxPacketsPerSecond() < Double
						.parseDouble(thresholdValue);

		case "txBytesPerSecond":
			if (currentRecord.getTxBytesPerSecond() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getTxBytesPerSecond() < Double
						.parseDouble(thresholdValue);

		case "txUtilizationPct":
			if (currentRecord.getTxUtilizationPct() == -1d)
				throw new Exception(
						"Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return currentRecord.getTxUtilizationPct() < Double
						.parseDouble(thresholdValue);

		case "collisions":
			return currentRecord.get_collisions().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "rx_dropped":
			return currentRecord.get_rx_dropped().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "rx_errors":
			return currentRecord.get_rx_errors().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "tx_dropped":
			return currentRecord.get_tx_dropped().compareTo(
					new BigInteger(thresholdValue)) == -1;

		case "tx_errors":
			return currentRecord.get_tx_errors().compareTo(
					new BigInteger(thresholdValue)) == -1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in NetworkInterfaceRecord");
		}
	}

	@Override
	public NetworkInterfaceRecord merge(Record rec1, Record rec2)
			throws Exception {
		return new NetworkInterfaceRecord((NetworkInterfaceRecord) rec1,
				(NetworkInterfaceRecord) rec2);
	}

}
