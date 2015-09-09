package com.mapr.distiller.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.EOFException;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.server.recordtypes.Record;

public class PersistanceClient {
	private static final Logger LOG = LoggerFactory.getLogger(PersistanceClient.class);

	public static void main(String[] args){
		int pos=0;
		String inputLocation = null;
		String inputType = null;
		String inputOrder = null;
		String metricName = null;
		String outputDir = null;
		String processingMethod = null;
		String processingKey = null;
		String processingValue = null;
		while(pos<args.length){
			if(args[pos].startsWith("-")){
				if(args.length == pos + 1){
					LOG.error("Failed to parse final argument: " + args[pos]);
					System.exit(1);
				}
				switch (args[pos]){
				
				case "-inputLocation":
					if(inputLocation != null){
						LOG.info("Argument \"-inputLocation\" is specified multiple times");
					}
					inputLocation = args[++pos];
					break;
				
				case "-inputType":
					if(inputType != null){
						LOG.info("Argument \"-inputType\" is specified multiple times");
					}
					inputType = args[++pos].toLowerCase();
					break;
				
				case "-inputOrder":
					if(inputOrder != null){
						LOG.info("Argument \"-inputOrder\" is specified multiple times");
					}
					inputOrder = args[++pos].toLowerCase();
					break;
				
				case "-metricName":
					if(metricName != null){
						LOG.info("Argument \"-metricName\" is specified multiple times");
					}
					metricName = args[++pos];
					break;
					
				case "-outputDir":
					if(outputDir != null){
						LOG.info("Argument \"-outputDir\" is specified multiple times");
					}
					outputDir = args[++pos];
					break;
					
				case "-processingMethod":
					if(outputDir != null){
						LOG.info("Argument \"-processingMethod\" is specified multiple times");
					}
					processingMethod = args[++pos].toLowerCase();
					break;
					
				case "-processingKey":
					if(outputDir != null){
						LOG.info("Argument \"-processingKey\" is specified multiple times");
					}
					processingKey = args[++pos];
					break;
					
				case "-processingValue":
					if(outputDir != null){
						LOG.info("Argument \"-processingValue\" is specified multiple times");
					}
					processingValue = args[++pos];
					break;
					
				default:
					LOG.error("Unknown argument: " + args[pos]);
					System.exit(1);
				}
			} else {
				LOG.error("Failed to parse argument: " + args[pos]);
				System.exit(1);
			}
			pos++;
		}
		
		if(inputLocation == null){
			LOG.error("No value provided for inputLocation");
			System.exit(1);
		}
		if(inputType == null){
			LOG.error("No value provided for inputType");
			System.exit(1);
		}
		if(inputOrder == null){
			inputOrder = "timestamp";
		}
		if(metricName == null){
			LOG.error("No value provided for metricName");
			System.exit(1);
		}
		if(processingMethod == null){
			LOG.error("No value provided for processingMethod");
			System.exit(1);
		}

		switch (inputOrder){
		case "timestamp":
			break;
		
		case "previoustimestamp":
			break;
		
		default:
			LOG.error("Unknown inputOrder: " + inputOrder);
			System.exit(1);
		}	
		
		switch (processingMethod) {
		
		case "printlocalfiles":
			printRecords(inputType, inputLocation, inputOrder, metricName, null);
			break;
		
		case "printlocalfileswithqualifier":
			if(processingKey==null){
				LOG.error("Use of processingMethod printlocalfileswithqualifier require a value for processingKey");
				System.exit(1);
			}
			printRecords(inputType, inputLocation, inputOrder, metricName, processingKey);
			break;
		
		case "sort":
			if(processingKey == null || outputDir == null){
				LOG.error("Request to perform sort processing requires processingKey and outputDir to use for sorting.");
				System.exit(1);
			}
			sortRecords(inputType, inputLocation, inputOrder, metricName, processingKey, outputDir);
			break;
			
		case "select":
			if(processingKey == null || processingValue == null || outputDir == null){
				LOG.error("Request to perform select processing requires processingKey, processingValue and outputDir to use for selection");
				System.exit(1);
			}
			selectRecords(inputType, inputLocation, inputOrder, metricName, processingKey, processingValue, outputDir);
			break;
			
		default:
			LOG.error("Unknown processingMethod: " + processingMethod);
			System.exit(1);
		}       
	}
	
	private static void printRecords(String inputType, String inputLocation, String inputOrder, String metricName, String qualifierKey){
		switch (inputType.toLowerCase()) {
		case "maprdb":
			printRecordsFromMapRDB(inputLocation, inputOrder, metricName, qualifierKey);
			break;
		
		case "localfilesystem" :
			printRecordsFromLocalFileSystem(inputLocation, inputOrder, metricName, qualifierKey);
			break;
		
		default:
			LOG.error("Unknown inputType: " + inputType);
			System.exit(1);
		}

	}
	
	private static void printRecordsFromMapRDB(String inputLocation, String inputOrder, String metricName, String qualifierKey){
		
		LOG.error("Not implemented");
		System.exit(1);
	}
	
	private static void printRecordsFromLocalFileSystem(String inputLocation, String inputOrder, String metricName, String qualifierKey){
		File inputDir = new File(inputLocation);
		String[] dirEntries = null;
		String[] inputFiles = null;
		TreeMap<Long, String> inputFileMap = new TreeMap<Long, String>();
		TreeMap<String, Long> lastReturnedRecordTimestampForQualifier = new TreeMap<String, Long>();
		long startTimestamp, endTimestamp;
		
		if(!inputDir.isDirectory()){
			LOG.error("inputLocation is not a directory: " + inputLocation);
			System.exit(1);
		}
		
		try {
			dirEntries = inputDir.list();
		} catch (Exception e){
			LOG.error("Failed to retrieve directory list for " + inputLocation, e);
		}
		
		for (String entry : dirEntries){
			String[] subStr = entry.split("_");
			if((subStr.length != 8  && subStr.length != 4) || !subStr[1].equals(metricName)){
				LOG.debug("Ignoring file: " + entry);
			} else {
				ObjectInputStream s = null;
				try {
					s = new ObjectInputStream(new GZIPInputStream(new FileInputStream(inputLocation + "/" + entry)));
					Record r = ((Record)(s.readObject()));
					inputFileMap.put(new Long(r.getTimestamp()), entry);
					LOG.info("Adding input file " + entry + " with timestamp " + r.getTimestamp());
				} catch (Exception e){
					LOG.warn("Failed to read file " + entry + ", omitting it from further processing.", e);
				}
			}
		}
		if(inputFileMap.size()==0){
			LOG.error("Did not find any input files matching metric " + 
					metricName + " in input dir " + inputLocation);
			System.exit(1);
		}
		inputFiles = inputFileMap.values().toArray(new String[0]);

		long lastReturnedRecordTimestamp=-1;
		for (int x=0; x<inputFiles.length; x++){
			ObjectInputStream inputStream = null;
			Record recordToReturn = null;
			try {
				inputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream(inputLocation + "/" + inputFiles[x])));
				LOG.info("Reading from " + inputLocation + "/" + inputFiles[x]);
				try {
					while((recordToReturn = (Record)(inputStream.readObject())) != null){
						if(qualifierKey==null){
							if(recordToReturn.getTimestamp() >= lastReturnedRecordTimestamp){
								System.out.println("tsp:" + ((lastReturnedRecordTimestamp==-1) ? 0 : (recordToReturn.getTimestamp() - lastReturnedRecordTimestamp)) + 
										" " + recordToReturn.toString());
								lastReturnedRecordTimestamp = recordToReturn.getTimestamp();
							}
						} else {
							if(lastReturnedRecordTimestampForQualifier.containsKey(recordToReturn.getValueForQualifier(qualifierKey))){
								if(recordToReturn.getTimestamp() >= lastReturnedRecordTimestampForQualifier.get(recordToReturn.getValueForQualifier(qualifierKey)).longValue()) {
									System.out.println("tsp:" + ((lastReturnedRecordTimestamp==-1) ? 0 : (recordToReturn.getTimestamp() - lastReturnedRecordTimestamp)) + 
											" " + recordToReturn.toString());
									lastReturnedRecordTimestampForQualifier.put(recordToReturn.getValueForQualifier(qualifierKey), new Long(recordToReturn.getTimestamp()));
								}
							} else {
								System.out.println("tsp:" + ((lastReturnedRecordTimestamp==-1) ? 0 : (recordToReturn.getTimestamp() - lastReturnedRecordTimestamp)) + 
										" " + recordToReturn.toString());
								lastReturnedRecordTimestampForQualifier.put(recordToReturn.getValueForQualifier(qualifierKey), new Long(recordToReturn.getTimestamp()));
							}
						}
					}
				} catch (EOFException eof){
					//Do nothing, its the end of the file.
				} catch (Exception e){
					LOG.info("Exception while reading:", e);
				}
				try {
					inputStream.close();
				} catch (Exception e){}
			} catch (Exception e){
				LOG.error("Failed to create an ObjectInputStream for " + inputLocation + "/" + inputFiles[x], e);
				System.exit(1);
			}
		}
	}
	
	private static void sortRecords(String inputType, String inputLocation, String inputOrder, String metricName, String processingKey, String outputDir){
		LOG.error("sortRecords not implemented.");
	}
	
	private static void selectRecords(String inputType, String inputLocation, String inputOrder, String metricName, String processingKey, String processingValue, String outputDir){
		LOG.error("selectRecords not implemented.");
	}
	
}
