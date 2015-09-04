package com.mapr.distiller.cli.base.inputparams;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONObjectInputParameter extends BaseInputParameter {

	private static final Logger LOG = Logger.getLogger(JSONObjectInputParameter.class);
	
	public JSONObjectInputParameter(String name, String description,
			boolean isRequired) {
		super(name, description, isRequired, null);
	}

	@Override
	public String getBasicDataType() {
		return "JSONObject";
	}

	@Override
	public String getParameterDefaultValueAsString() {
		return null;
	}

	@Override
	public JSONObject valueOf(String value) {
		if ( value == null ) {
			return null;
		}
		
		try {
			JSONObject jO = new JSONObject(value);
			return jO;
		} catch (JSONException e) {
      /**
       * <MAPR_ERROR>
       * Message:Provided input can not be converted to JSONObject: <value>
       * Function:JSONObjectInputParameter.valueOf()
       * Meaning:An internal error occurred while processing the input value.
       * Resolution:Make sure the input data is valid.
       * </MAPR_ERROR>
       */
			LOG.error("Provided input can not be converted to JSONObject: " + value);
		}
		return null;
	}

}
