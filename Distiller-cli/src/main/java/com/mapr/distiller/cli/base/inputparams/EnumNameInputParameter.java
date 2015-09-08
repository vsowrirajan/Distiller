package com.mapr.distiller.cli.base.inputparams;

public class EnumNameInputParameter extends BaseInputParameter {

  private Class m_enumClass;

  protected EnumNameInputParameter(Enum name, String description,
      boolean isRequired, Class enumClass, String defaultValue) {
    super(name.name(), description, isRequired, defaultValue);
  }

  @Override
  public String getBasicDataType() {
    return "String";
  }

  @Override
  public String getParameterDefaultValueAsString() {
    if (m_defaultValue != null) {
      return (String) m_defaultValue;
    }
    return null;
  }

  @Override
  public Object valueOf(String value) {
    return value;
  }

}
