package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.NetworkInterfaceRecord;

import java.math.BigInteger;

public class NetworkInterfaceRecordProcessor implements 
		Thresholdable<NetworkInterfaceRecord>, MovingAverageable<NetworkInterfaceRecord> {
	
	public boolean isNotEqual(NetworkInterfaceRecord record, String metric,
			String thresholdValue) throws Exception {
		return !isEqual(record, metric, thresholdValue);
	}
	
	public boolean isEqual(NetworkInterfaceRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "name":
			return record.getName().equals(thresholdValue);

		case "duplex":
			return record.getDuplex().equals(thresholdValue);
			
		case "fullDuplex":
			return record.getFullDuplex() == Boolean.parseBoolean(thresholdValue);
			
		case "carrier":
			return record.getCarrier() == Integer.parseInt(thresholdValue);
			
		case "speed":
			return record.getSpeed() == Integer.parseInt(thresholdValue);
			
		case "tx_queue_len":
			return record.get_tx_queue_len() == Integer.parseInt(thresholdValue);
			
		case "hasProblems":
			return (!record.getFullDuplex() ||
					record.getCarrier() != 1 ||
					record.getSpeed() < 1000 ||
					record.get_tx_queue_len() < 1000 ||
					!record.get_collisions().equals(new BigInteger("0")) ||
					!record.get_rx_dropped().equals(new BigInteger("0")) ||
					!record.get_rx_errors().equals(new BigInteger("0")) ||
					!record.get_tx_dropped().equals(new BigInteger("0")) ||
					!record.get_tx_errors().equals(new BigInteger("0")) );
			
		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in NetworkInterfaceRecord");
		}
	}

	public boolean isAboveThreshold(NetworkInterfaceRecord record, String metric,
			String thresholdValue) throws Exception {

		switch (metric) {
		case "speed":
			return record.getSpeed() > Integer.parseInt(thresholdValue);
		
		case "rxPacketsPerSecond":
			if(record.getRxPacketsPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getRxPacketsPerSecond() > Double.parseDouble(thresholdValue);
		
		case "rxBytesPerSecond":
			if(record.getRxBytesPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getRxBytesPerSecond() > Double.parseDouble(thresholdValue);
		
		case "rxUtilizationPct":
			if(record.getRxUtilizationPct() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getRxUtilizationPct() > Double.parseDouble(thresholdValue);
		
		case "txPacketsPerSecond":
			if(record.getTxPacketsPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getTxPacketsPerSecond() > Double.parseDouble(thresholdValue);
		
		case "txBytesPerSecond":
			if(record.getTxBytesPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getTxBytesPerSecond() > Double.parseDouble(thresholdValue);
		
		case "txUtilizationPct":
			if(record.getTxUtilizationPct() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getTxUtilizationPct() > Double.parseDouble(thresholdValue);
			
		case "collisions":
			return record.get_collisions().compareTo(new BigInteger(thresholdValue)) == 1;
		
		case "rx_dropped":
			return record.get_rx_dropped().compareTo(new BigInteger(thresholdValue)) == 1;
		
		case "rx_errors":
			return record.get_rx_errors().compareTo(new BigInteger(thresholdValue)) == 1;
		
		case "tx_dropped":
			return record.get_tx_dropped().compareTo(new BigInteger(thresholdValue)) == 1;
		
		case "tx_errors":
			return record.get_tx_errors().compareTo(new BigInteger(thresholdValue)) == 1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in NetworkInterfaceRecord");
		}
	}

	@Override
	public boolean isBelowThreshold(NetworkInterfaceRecord record, String metric,
			String thresholdValue) throws Exception {
		
		switch (metric) {
		case "speed":
			return record.getSpeed() < Integer.parseInt(thresholdValue);
		
		case "rxPacketsPerSecond":
			if(record.getRxPacketsPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getRxPacketsPerSecond() < Double.parseDouble(thresholdValue);
		
		case "rxBytesPerSecond":
			if(record.getRxBytesPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getRxBytesPerSecond() < Double.parseDouble(thresholdValue);
		
		case "rxUtilizationPct":
			if(record.getRxUtilizationPct() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getRxUtilizationPct() < Double.parseDouble(thresholdValue);
		
		case "txPacketsPerSecond":
			if(record.getTxPacketsPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getTxPacketsPerSecond() < Double.parseDouble(thresholdValue);
		
		case "txBytesPerSecond":
			if(record.getTxBytesPerSecond() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getTxBytesPerSecond() < Double.parseDouble(thresholdValue);
		
		case "txUtilizationPct":
			if(record.getTxUtilizationPct() == -1d)
				throw new Exception("Can not compare raw NetworkInterfaceRecord to threshold");
			else
				return record.getTxUtilizationPct() < Double.parseDouble(thresholdValue);
			
		case "collisions":
			return record.get_collisions().compareTo(new BigInteger(thresholdValue)) == -1;
		
		case "rx_dropped":
			return record.get_rx_dropped().compareTo(new BigInteger(thresholdValue)) == -1;
		
		case "rx_errors":
			return record.get_rx_errors().compareTo(new BigInteger(thresholdValue)) == -1;
		
		case "tx_dropped":
			return record.get_tx_dropped().compareTo(new BigInteger(thresholdValue)) == -1;
		
		case "tx_errors":
			return record.get_tx_errors().compareTo(new BigInteger(thresholdValue)) == -1;

		default:
			throw new Exception("Metric " + metric
					+ " is not Thresholdable in NetworkInterfaceRecord");
		}
	}

	@Override
	public NetworkInterfaceRecord movingAverage(NetworkInterfaceRecord rec1,
			NetworkInterfaceRecord rec2) throws Exception{
		return new NetworkInterfaceRecord(rec1, rec2);
	}

}
