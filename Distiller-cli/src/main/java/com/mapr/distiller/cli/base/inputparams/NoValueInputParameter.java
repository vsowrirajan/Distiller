package com.mapr.distiller.cli.base.inputparams;

/**
 * Input parameter that does not require value - just presence of the parameter
 * itself
 */
public class NoValueInputParameter extends BaseInputParameter {

  public NoValueInputParameter(String name, String description,
      boolean isRequired, boolean isValueRequired) {
    super(name, description, isRequired, false);

  }

  @Override
  public String getBasicDataType() {
    return null;
  }

  @Override
  public Object valueOf(String value) {
    return null;
  }

  @Override
  public String getParameterDefaultValueAsString() {
    return null;
  }

  @Override
  public boolean isValueRequired() {
    return false;
  }
}
