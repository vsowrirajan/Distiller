package com.mapr.distiller.cli.base.inputparams;

/**
 * Text input parameter
 */
public class TextInputParameter extends BaseInputParameter {

  public TextInputParameter(String name, String description,
      boolean isRequired, String defaultValue) {
    super(name, description, isRequired, defaultValue);
  }

  @Override
  public String getBasicDataType() {
    return "String";
  }

  @Override
  public Object valueOf(String value) {
    return value;
  }

  @Override
  public String getParameterDefaultValueAsString() {
    if (m_defaultValue != null) {
      return (String) m_defaultValue;
    }
    return null;
  }

}
