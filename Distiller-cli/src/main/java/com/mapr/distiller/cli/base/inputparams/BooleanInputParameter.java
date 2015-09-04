package com.mapr.distiller.cli.base.inputparams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Boolean input parameter
 * @author yufeldman
 *
 */
public class BooleanInputParameter extends BaseInputParameter {
	
	private static final List<String> trueValues = Arrays.asList(new String[] {"t", "true", "yes", "y", "1"});
	private static final List<String> falseValues = Arrays.asList(new String[]{"f", "false", "no", "n", "0"});
	
	private static Map<Boolean, List<String>> booleanMap = new HashMap<Boolean, List<String>>();
	
	static {
		booleanMap.put(Boolean.TRUE, trueValues);
		booleanMap.put(Boolean.FALSE, falseValues);
	}
		
	public BooleanInputParameter(String name, String description,
			boolean isRequired, Boolean defaultValue ) {
		super(name, description, isRequired, defaultValue);
	}

	public BooleanInputParameter(String name, String description,
			boolean isRequired, Object defaultValue ) {
		super(name, description, isRequired, defaultValue);
	}

	@Override
	public String getBasicDataType() {
		return "Boolean";
	}

	@Override
	public Object valueOf(String value) {
		if ( value == null ) {
			return null;
		}
		if (trueValues.contains(value.toLowerCase())) {
			return Boolean.TRUE;
		} else if (falseValues.contains(value.toLowerCase())) {
			return Boolean.FALSE;
		}
		
		return null;
	}

	@Override
	public String getParameterDefaultValueAsString() {
		if ( m_defaultValue != null ) {
			if ( booleanMap.get(Boolean.TRUE).contains(m_defaultValue.toString())) { 
				return Boolean.TRUE.toString();
			} else if (booleanMap.get(Boolean.FALSE).contains(m_defaultValue.toString())) { 
				return Boolean.FALSE.toString();
			}
		}
		return null;
	}

}
