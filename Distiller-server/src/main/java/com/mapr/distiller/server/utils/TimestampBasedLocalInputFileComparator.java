package com.mapr.distiller.server.utils;

import java.util.Comparator;

public class TimestampBasedLocalInputFileComparator implements Comparator<String>{
	public int compare(String s1, String s2) {
		long s1T = Long.parseLong(s1.split("_")[7]);
		long s2T = Long.parseLong(s2.split("_")[7]);
		long s1PT = Long.parseLong(s1.split("_")[5]);
		long s2PT = Long.parseLong(s2.split("_")[5]);
		if(s1T < s2T){
			return -1;
		} else if (s2T < s1T){
			return 1;
		} else if (s1PT < s2PT){
			return -1;
		} else if (s2PT < s1PT){
			return 1;
		} else {
			if(s1.compareTo(s2) < 0)
				return -1;
			else if(s1.compareTo(s2) > 0)
				return 1;
			else 
				return 0;
		}
	}
}
