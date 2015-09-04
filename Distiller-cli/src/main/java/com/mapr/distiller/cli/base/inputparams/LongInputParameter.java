package com.mapr.distiller.cli.base.inputparams;

import org.apache.log4j.Logger;

/**
 * Long input parameter
 * @author yufeldman
 *
 */
public class LongInputParameter extends BaseInputParameter {

	private Long m_minValue;
	private Long m_maxValue;

	private static final Logger LOG = Logger.getLogger(LongInputParameter.class);
	
	public LongInputParameter(String name, String description,
			boolean isRequired, Object defaultValue) {
		super(name, description, isRequired, defaultValue);
	}

	public LongInputParameter(String name, String description,
			boolean isRequired, Object defaultValue, Long min, Long max) {
		super(name, description, isRequired, defaultValue);
		
		m_minValue = min;
		m_maxValue = max;
	}

	
	public Long getMinValue() {
		return m_minValue;
	}

	public void setMinValue(Long mMinValue) {
		m_minValue = mMinValue;
	}

	public Long getMaxValue() {
		return m_maxValue;
	}

	public void setMaxValue(Long mMaxValue) {
		m_maxValue = mMaxValue;
	}

	@Override
	public String getBasicDataType() {
		return "Long";
	}

	@Override
	public Object valueOf(String value) {
		if ( value == null ) {
			return null;
		}
		try {
			return Long.valueOf(value);
		} catch(NumberFormatException nfe) {
			/**
      /**
       * <MAPR_ERROR>
       * Message:Value: <value> is not Long
       * Function:LongInputParameter.valueOf()
       * Meaning:A non-long value was passed to a parameter that requires type Long.
       * Resolution:Check the command syntax, and make sure to pass the correct type and a valid value to each parameter.
       * </MAPR_ERROR>
       */
			LOG.error("Value: " + value + " is not Long");
		}
		return null;
	}

	@Override
	public boolean minAndMaxApplicable() {
		return true;
	}
	
	@Override
	public String getMinValueAsString() {
		return ((m_minValue != null) ? m_minValue.toString() : null); 
	}
	
	@Override
	public String getMaxValueAsString() {
		return ((m_maxValue != null) ? m_maxValue.toString() : null);
	}

	@Override
	public boolean isWithinBoundaries(String value) {
		Long longValue = new Long(value);

		if ( longValue == null ) {
			return false;
		}
		if ( m_minValue != null ) {
			if ( longValue.longValue() < m_minValue.longValue() ) {
				return false;
			}
		}
		if ( m_maxValue != null ) {
			if ( longValue.longValue() > m_maxValue.longValue() ) {
				return false;
			}
		}
		if ( m_minValue != null && m_maxValue != null ) {
			if ( m_minValue.longValue() <= longValue.longValue() && longValue.longValue() <= m_maxValue.longValue() ) {
				return true;
			}
		}
		return true;
	}

	@Override
	public String getParameterDefaultValueAsString() {
		if ( m_defaultValue != null ) {
			return Long.toString(((Long) m_defaultValue).longValue());
		}
		return null;
	}


}
