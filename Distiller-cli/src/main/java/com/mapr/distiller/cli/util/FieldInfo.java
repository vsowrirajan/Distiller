package com.mapr.distiller.cli.util;

import java.lang.Class;

/* class maintains a table for
 * fieldId, short name, long name and field type (used for filtering)
 * for example
 * 2, ip, ip-addr, String
 * 3, msu, mem-used, long
 */

public class FieldInfo { 
	String shortName, longName;
	Class fieldType;
	int id;

	public FieldInfo(int id, String sName, String lName, Class type) {
	  this.id = id;
	  this.shortName = sName;
	  this.longName = lName;
	  this.fieldType = type;
	}

	public int getId() { return id; }
	public String getShortName() { return shortName; }
	public String getLongName() { return longName; }
	public Class getFieldType() { return fieldType; }
	public String getName(boolean terse) {
	  if (terse == true) {
	    return getShortName();
	  } else {
	    return getLongName();
	  }
	}
}
