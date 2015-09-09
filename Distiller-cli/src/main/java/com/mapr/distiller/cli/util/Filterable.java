package com.mapr.distiller.cli.util;

public interface Filterable {
  public Object getValueInData();

  public Object getValueInFilter();
}
