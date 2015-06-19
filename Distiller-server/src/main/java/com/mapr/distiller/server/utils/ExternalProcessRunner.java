package com.mapr.distiller.server.utils;

import java.io.IOException;
import java.lang.Object;
import java.io.InputStream;

public class ExternalProcessRunner {
	private ProcessBuilder processBuilder = null;
	private Process process = null;
	private boolean exitByRequest = false;
	private boolean launchedChildProcess = false;
	private String id;
	private int retCode = 0;

	public ExternalProcessRunner(String id, String[] commandToRun) {
		this.id = id + ":EPR";
		this.processBuilder = new ProcessBuilder(commandToRun);
	}

	public InputStream getStdoutStream() {
		if (process == null)
			return null;
		try {
			process.exitValue();
			System.err
					.println("DEBUG: "
							+ id
							+ ": Failed to return stdout stream as child process is not running.");
		} catch (Exception e) {
			return process.getInputStream();
		}
		return null;
	}

	public InputStream getStderrStream() {
		if (process == null) {
			return null;
		}
		
		try {
			process.exitValue();
			System.err
					.println("DEBUG: "
							+ id
							+ ": Failed to return stderr stream as child process is not running");
		} catch (Exception e) {
			return process.getErrorStream();
		}
		return null;
	}

	public boolean isChildProcessAlive() {
		if (process == null) {
			return false;
		}

		try {
			process.exitValue();
			return false;
		} catch (Exception e) {

		}

		return true;
	}

	public boolean isChildProcessFinished() {
		if (launchedChildProcess && !isChildProcessAlive()) {
			return true;
		}
		return false;
	}

	public void stopChildProcess(Object o) {
		System.err.println("INFO: " + id
				+ ": Reveived request to terminate child process from " + o);
		exitByRequest = true;
		process.destroy();

	}

	public boolean startProcess() {
		try {
			process = processBuilder.start();
		} catch (IOException e) {
			System.err.println("INFO: " + id
					+ ": Failed to launch child process due to IOException");
			e.printStackTrace();
			return false;
		}
		launchedChildProcess = true;
		return true;
	}

}
