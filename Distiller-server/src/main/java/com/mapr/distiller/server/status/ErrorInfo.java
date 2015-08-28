package com.mapr.distiller.server.status;

public class ErrorInfo {
	public final String URL;
	
	public final Class exception;
	public final String shortMessage;

	public ErrorInfo(String url, Exception ex) {
		this.URL = url;
		this.exception = ex.getClass();
		this.shortMessage = ex.getMessage();
	}
}
