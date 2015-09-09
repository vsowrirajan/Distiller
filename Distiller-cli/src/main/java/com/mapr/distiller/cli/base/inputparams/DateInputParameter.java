package com.mapr.distiller.cli.base.inputparams;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateInputParameter extends BaseInputParameter {

  private static final Logger LOG = LoggerFactory
      .getLogger(DateInputParameter.class);

  public DateInputParameter(String name, String description,
      boolean isRequired, Object defaultValue) {
    super(name, description, isRequired, defaultValue);
  }

  @Override
  public Object valueOf(String value) {
    try {
      return new Date(Long.parseLong(value)); // try if value is a timestamp
    } catch (NumberFormatException nfe) {
      try {
        DateFormat dateInstance = DateFormat.getDateInstance(DateFormat.SHORT);
        dateInstance.setLenient(false);
        dateInstance.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateInstance.parse(value); // try if value is a date string
                                          // (mm/dd/yyyy)
      } catch (ParseException pe) {
        LOG.error("Exception while parsing the value into date. Value: "
            + value); // log an error and give up!
      }
    }

    return null;
  }

  @Override
  public String getBasicDataType() {
    return "Date";
  }

  @Override
  public String getParameterDefaultValueAsString() {
    return m_defaultValue != null ? DateFormat
        .getDateInstance(DateFormat.SHORT).format(m_defaultValue) : null;
  }
}
