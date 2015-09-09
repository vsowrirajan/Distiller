package com.mapr.distiller.cli.base.inputparams;

/**
 * Base class to define input parameter
 */
public abstract class BaseInputParameter {

  protected String m_name;
  protected String m_description;
  protected boolean m_required;
  protected Object m_defaultValue;
  protected boolean m_isValueRequired = true;
  protected boolean m_isInvisible;

  protected BaseInputParameter(String name, String description,
      boolean isRequired, Object defaultValue) {
    m_name = name;
    m_description = description;
    m_required = isRequired;
    m_defaultValue = defaultValue;
  }

  protected BaseInputParameter(String name, String description,
      boolean isRequired, boolean isValueRequired) {
    m_name = name;
    m_description = description;
    m_required = isRequired;
    m_defaultValue = null;
    m_isValueRequired = isValueRequired;
  }

  public String getName() {
    return m_name;
  }

  public void setName(String mName) {
    m_name = mName;
  }

  public String getDescription() {
    return m_description;
  }

  public void setDescription(String mDescription) {
    m_description = mDescription;
  }

  public boolean isRequired() {
    return m_required;
  }

  public void setRequired(boolean mRequired) {
    m_required = mRequired;
  }

  public Object getParameterDefaultValue() {
    return m_defaultValue;
  }

  public void setParameterDefaultValue(Object mParameterDefaultValue) {
    m_defaultValue = mParameterDefaultValue;
  }

  public boolean isValueRequired() {
    return m_isValueRequired;
  }

  public void setIsValueRequired(boolean mIsValueRequired) {
    m_isValueRequired = mIsValueRequired;
  }

  @Override
  public String toString() {
    return m_name;
  }

  public boolean minAndMaxApplicable() {
    return false;
  }

  /**
   * Only applicable for numeric types
   * 
   * @return
   */
  public String getMinValueAsString() {
    return null;
  }

  /**
   * Only applicable for numeric types
   * 
   * @return
   */
  public String getMaxValueAsString() {
    return null;
  }

  public boolean isWithinBoundaries(String value) {
    return true;
  }

  public boolean isInvisible() {
    return m_isInvisible;
  }

  public BaseInputParameter setInvisible(boolean mIsInvisible) {
    m_isInvisible = mIsInvisible;
    return this;
  }

  abstract public Object valueOf(String value);

  abstract public String getBasicDataType();

  abstract public String getParameterDefaultValueAsString();
}
