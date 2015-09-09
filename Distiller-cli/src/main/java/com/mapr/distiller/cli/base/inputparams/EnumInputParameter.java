package com.mapr.distiller.cli.base.inputparams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnumInputParameter extends BaseInputParameter {

  private static final Logger LOG = LoggerFactory
      .getLogger(EnumInputParameter.class);

  private Class m_enumClass;

  public EnumInputParameter(String name, String description,
      boolean isRequired, Class enumClass, Object defaultValue) {
    super(name, description, isRequired, defaultValue);
    m_enumClass = enumClass;
  }

  @Override
  public String getBasicDataType() {
    return "Enum";
  }

  @Override
  public String getParameterDefaultValueAsString() {
    if (m_defaultValue != null) {
      try {
        Enum defaultEnum = (Enum) m_defaultValue;
        return defaultEnum.name();
      } catch (IllegalArgumentException ex) {
        LOG.error("Invalid enum value");
      }
    }
    return null;
  }

  @Override
  public Object valueOf(String value) {
    try {
      return Enum.valueOf(m_enumClass, value);
    } catch (IllegalArgumentException ex) {
      LOG.error("Invalid enum value");
    }
    return null;
  }
}
