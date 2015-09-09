package com.mapr.distiller.cli.base.inputparams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integer Input Parameter
 *
 */
public class IntegerInputParameter extends BaseInputParameter {

  private Integer m_minValue; // = Integer.MIN_VALUE;
  private Integer m_maxValue; // = Integer.MAX_VALUE;

  private static final Logger LOG = LoggerFactory
      .getLogger(IntegerInputParameter.class);

  public IntegerInputParameter(String name, String description,
      boolean isRequired, Integer defaultValue) {
    super(name, description, isRequired, defaultValue);
  }

  public IntegerInputParameter(String name, String description,
      boolean isRequired, Integer defaultValue, Integer min, Integer max) {
    super(name, description, isRequired, defaultValue);

    m_minValue = min;
    m_maxValue = max;
  }

  public Integer getMinValue() {
    return m_minValue;
  }

  public void setMinValue(Integer mMinValue) {
    m_minValue = mMinValue;
  }

  public Integer getMaxValue() {
    return m_maxValue;
  }

  public void setMaxValue(Integer mMaxValue) {
    m_maxValue = mMaxValue;
  }

  @Override
  public String getBasicDataType() {
    return "Integer";
  }

  @Override
  public Object valueOf(String value) {
    if (value == null) {
      return null;
    }
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException nfe) {
      /**
       * <MAPR_ERROR> Message:Value: <value> is not an Integer
       * Function:IntegerInputParameter.valueOf() Meaning:A non-integer value
       * was passed to a parameter that requires a value of type Integer.
       * Resolution:Check the command syntax, and make sure to pass the correct
       * type and a valid value to each parameter. </MAPR_ERROR>
       */
      LOG.error("Value: " + value + " is not an Integer");
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
    Integer intValue = new Integer(value);

    if (intValue == null) {
      return false;
    }
    if (m_minValue != null) {
      if (intValue.intValue() < m_minValue.intValue()) {
        return false;
      }
    }
    if (m_maxValue != null) {
      if (intValue.intValue() > m_maxValue.intValue()) {
        return false;
      }
    }
    if (m_minValue != null && m_maxValue != null) {
      if (m_minValue.intValue() <= intValue.intValue()
          && intValue.intValue() <= m_maxValue.intValue()) {
        return true;
      }
    }
    return true;
  }

  @Override
  public String getParameterDefaultValueAsString() {
    if (m_defaultValue != null) {
      return Integer.toString(((Integer) m_defaultValue).intValue());
    }
    return null;
  }

}
