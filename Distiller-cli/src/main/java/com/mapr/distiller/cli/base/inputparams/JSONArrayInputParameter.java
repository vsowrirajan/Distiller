package com.mapr.distiller.cli.base.inputparams;

import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONArrayInputParameter extends BaseInputParameter {

  private static final Logger LOG = LoggerFactory
      .getLogger(JSONArrayInputParameter.class);

  public JSONArrayInputParameter(String name, String description,
      boolean isRequired) {
    super(name, description, isRequired, null);
  }

  @Override
  public String getBasicDataType() {
    return "JSONArray";
  }

  @Override
  public String getParameterDefaultValueAsString() {
    return null;
  }

  @Override
  public JSONArray valueOf(String value) {
    if (value == null) {
      return null;
    }
    JSONArray jA;
    try {
      jA = new JSONArray(value);
      return jA;
    } catch (JSONException e) {
      /**
       * <MAPR_ERROR> Message:Provided input can not be converted to JSONArray:
       * <value> Function:JSONArrayInputParameter.valueOf() Meaning:An internal
       * error occurred while processing the input value. Resolution:Make sure
       * the input data is valid. </MAPR_ERROR>
       */
      LOG.error("Provided input can not be converted to JSONArray: " + value);
    }
    return null;
  }

}
