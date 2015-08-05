package com.mapr.distiller.server.datatypes;

import java.util.LinkedList;
import java.util.ListIterator;

import com.mapr.distiller.server.datatypes.ProcMetricDescriptor;

public class ProcMetricDescriptorManager {
	private LinkedList<ProcMetricDescriptor> metricList;
	
	public ProcMetricDescriptorManager(){
		this.metricList = new LinkedList<ProcMetricDescriptor>();
	}
	
	public boolean containsMetricName(String metricName){
		ListIterator<ProcMetricDescriptor> i = metricList.listIterator();
		while(i.hasNext()){
			if(i.next().metricName.equals(metricName))
				return true;
		}
		return false;
	}
	
	public boolean containsDescriptor(ProcMetricDescriptor d){
		ListIterator<ProcMetricDescriptor> i = metricList.listIterator();
		while(i.hasNext()){
			if(i.next().equals(d))
				return true;
		}
		return false;
	}

	public boolean containsDescriptor(String metricName, int periodicity){
		ListIterator<ProcMetricDescriptor> i = metricList.listIterator();
		while(i.hasNext()){
			if(i.next().equals(metricName, periodicity))
				return true;
		}
		return false;
	}
	
	public boolean addDescriptor(String metricName, int periodicity){
		return metricList.add(new ProcMetricDescriptor(metricName, periodicity));
	}

	public boolean removeDescriptor(String metricName, int periodicity){
		return metricList.remove(new ProcMetricDescriptor(metricName, periodicity));
	}
}
