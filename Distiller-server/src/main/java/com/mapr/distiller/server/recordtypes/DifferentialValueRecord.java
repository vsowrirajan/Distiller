package com.mapr.distiller.server.recordtypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mapr.distiller.server.utils.Constants;

public class DifferentialValueRecord extends Record{
	private static final Logger LOG = LoggerFactory
			.getLogger(DifferentialValueRecord.class);
	private static final long serialVersionUID = Constants.SVUID_DIFFERENTIAL_VALUE_RECORD;
	private String inputRecordType;
	private String valueName;
	private String valueType;
	private Object value;
	private long oldRecordTimestamp;
	private long newRecordPreviousTimestamp;
	
	@Override
	public String getRecordType(){
		return Constants.DIFFERENTIAL_VALUE_RECORD;
	}
	

	
	public DifferentialValueRecord( long previousTimestamp,
									long oldRecordTimestamp,
									long newRecordPreviousTimestamp,
									long timestamp,
									String inputRecordType,
									String valueName,
									String valueType,
									Object value ) throws Exception
	{
		super(timestamp, previousTimestamp);
		this.oldRecordTimestamp = oldRecordTimestamp;
		this.newRecordPreviousTimestamp = newRecordPreviousTimestamp;
		this.inputRecordType = inputRecordType;
		this.valueName = valueName;
		this.valueType = valueType;
		this.value = value;
	}
	
	public String getInputRecordType(){
		return inputRecordType;
	}
	
	public String getValueName(){
		return valueName;
	}
	
	public Object getValue(){
		return value;
	}
	
	public String getValueType(){
		return valueType;
	}
	
	public long getOldRecordTimestamp(){
		return oldRecordTimestamp;
	}
	
	public long getNewRecordPreviousTimestamp() {
		return newRecordPreviousTimestamp;
	}
}
