package com.mapr.distiller.server.processors;

import com.mapr.distiller.server.recordtypes.DifferentialValueRecord;
import com.mapr.distiller.server.recordtypes.NetworkInterfaceRecord;
import com.mapr.distiller.server.recordtypes.Record;
import com.mapr.distiller.server.utils.Constants;

import java.math.BigInteger;

public class NetworkInterfaceRecordProcessor implements RecordProcessor<Record> {

	@Override
	public DifferentialValueRecord diff(Record rec1, Record rec2, String metric) throws Exception {
		if( rec1.getPreviousTimestamp()==-1l ||
			rec2.getPreviousTimestamp()==-1l )
			throw new Exception("NetworkInterfaceRecords can only be diff'd from non-raw NetworkInterfaceRecords");
			
		NetworkInterfaceRecord oldRecord, newRecord;
		if(rec1.getTimestamp() < rec2.getTimestamp()){
			oldRecord = (NetworkInterfaceRecord)rec1;
			newRecord = (NetworkInterfaceRecord)rec2;
		} else {
			oldRecord = (NetworkInterfaceRecord)rec2;
			newRecord = (NetworkInterfaceRecord)rec1;
		}
		if(!oldRecord.getName().equals(newRecord.getName()))
			throw new Exception("Can not diff NetworkInterfaceRecords where interface name does not match");
		
		if(oldRecord.getPreviousTimestamp() > newRecord.getPreviousTimestamp())
			throw new Exception("Can not calculate diff for input records where the timestamps of one record are within the timestamps of the other");

		switch (metric) {
		case "name":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												 !newRecord.getName().equals(oldRecord.getName()) );


		case "duplex":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												 !newRecord.getDuplex().equals(oldRecord.getDuplex()) );


		case "fullDuplex":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												 newRecord.getFullDuplex() != (oldRecord.getFullDuplex()) );


		case "carrier":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												 newRecord.getCarrier() != (oldRecord.getCarrier()) );


		case "speed":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												 newRecord.getSpeed() != (oldRecord.getSpeed()) );


		case "tx_queue_len":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												 newRecord.get_tx_queue_len() != (oldRecord.get_tx_queue_len()) );


		case "hasProblems":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"boolean",
												( !oldRecord.getFullDuplex() ||
												  oldRecord.getCarrier() != 1 ||
												  oldRecord.getSpeed() < 1000 ||
												  oldRecord.get_tx_queue_len() < 1000 ||
												  !oldRecord.get_collisions().equals(new BigInteger("0")) ||
												  !oldRecord.get_rx_dropped().equals(new BigInteger("0")) ||
												  !oldRecord.get_rx_errors().equals(new BigInteger("0")) ||
												  !oldRecord.get_tx_dropped().equals(new BigInteger("0")) || 
												  !oldRecord.get_tx_errors().equals(new BigInteger("0"))
												)
												!=
												( !newRecord.getFullDuplex() ||
												  newRecord.getCarrier() != 1 ||
												  newRecord.getSpeed() < 1000 ||
												  newRecord.get_tx_queue_len() < 1000 ||
												  !newRecord.get_collisions().equals(new BigInteger("0")) ||
												  !newRecord.get_rx_dropped().equals(new BigInteger("0")) ||
												  !newRecord.get_rx_errors().equals(new BigInteger("0")) ||
												  !newRecord.get_tx_dropped().equals(new BigInteger("0")) || 
												  !newRecord.get_tx_errors().equals(new BigInteger("0"))	
												)
					);

		case "rxPacketsPerSecond":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getRxPacketsPerSecond() - oldRecord.getRxPacketsPerSecond() );


		case "rxBytesPerSecond":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getRxBytesPerSecond() - oldRecord.getRxBytesPerSecond() );


		case "rxUtilizationPct":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getRxUtilizationPct() - oldRecord.getRxUtilizationPct() );


		case "txPacketsPerSecond":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getTxPacketsPerSecond() - oldRecord.getTxPacketsPerSecond() );


		case "txBytesPerSecond":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getTxBytesPerSecond() - oldRecord.getTxBytesPerSecond() );


		case "txUtilizationPct":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"double",
												 newRecord.getTxUtilizationPct() - oldRecord.getTxUtilizationPct() );

		case "collisions":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_collisions().subtract(oldRecord.get_collisions()) );


		case "rx_dropped":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_rx_dropped().subtract(oldRecord.get_rx_dropped()) );


		case "rx_errors":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_rx_errors().subtract(oldRecord.get_rx_errors()) );


		case "tx_dropped":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_tx_dropped().subtract(oldRecord.get_tx_dropped()) );


		case "tx_errors":
			return new DifferentialValueRecord( oldRecord.getPreviousTimestamp(),
												 oldRecord.getTimestamp(),
												 newRecord.getPreviousTimestamp(),
												 newRecord.getTimestamp(),
												 getName(),
												 metric,
												"BigInteger",
												 newRecord.get_tx_errors().subtract(oldRecord.get_tx_errors()) );



		
		default:
			throw new Exception("Metric " + metric
					+ " is not Diffable in NetworkInterfaceRecordProcessor");
		}
	}	
	public String getName(){
		return Constants.NETWORK_INTERFACE_RECORD_PROCESSOR;
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
