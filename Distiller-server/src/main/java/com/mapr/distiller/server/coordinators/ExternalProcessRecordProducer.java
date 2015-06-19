package com.mapr.distiller.server.coordinators;

import com.mapr.distiller.server.processors.RecordProcessor;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.readers.InputStreamRecordReader;
import com.mapr.distiller.server.utils.ExternalProcessRunner;

public class ExternalProcessRecordProducer {

	private ExternalProcessRunner processMonitor = null;
	private InputStreamRecordReader stdoutReader = null, stderrReader = null;
	private RecordProducer stdoutProducer = null, stderrProducer = null;
	private RecordProcessor stdoutProcessor = null, stderrProcessor = null;
	private RecordQueue stdoutQueue = null, stderrQueue = null;
	private String id = "";
	private String[] commandToRun;
	private boolean startupComplete = false;

	public ExternalProcessRecordProducer(String id, String[] commandToRun,
			InputStreamRecordReader stdoutReader,
			InputStreamRecordReader stderrReader,
			RecordProcessor stdoutProcessor, RecordProcessor stderrProcessor,
			RecordQueue stdoutQueue, RecordQueue stderrQueue) {

		this.id = id + ":EPRP";

		this.commandToRun = commandToRun;
		this.processMonitor = new ExternalProcessRunner(this.id,
				this.commandToRun);

		this.stdoutReader = stdoutReader;
		this.stderrReader = stderrReader;

		this.stdoutProcessor = stdoutProcessor;
		this.stderrProcessor = stderrProcessor;

		this.stdoutQueue = stdoutQueue;
		this.stderrQueue = stderrQueue;

		this.stdoutProducer = new RecordProducer(stdoutReader, stdoutProcessor,
				stdoutQueue, id + ":RPout");
		this.stderrProducer = new RecordProducer(stderrReader, stderrProcessor,
				stderrQueue, id + ":RPerr");
	}

	public void startChildProcesses(Object o) {
		System.err
				.println("INFO: "
						+ id
						+ ": Starting all child processes/threads as requested by "
						+ o);
		// Start the external process
		this.processMonitor.startProcess();

		// Wait for the external process to be ready
		while (!this.processMonitor.isChildProcessAlive()) {
			try {
				Thread.sleep(100);
			} catch (Exception e) {
			}
			;
		}

		// Connect the RecordReaders to their respective InputStreams
		stdoutReader.setInputStream(processMonitor.getStdoutStream());
		stderrReader.setInputStream(processMonitor.getStderrStream());

		// Start the RecordProducer threads that will drain the stdout/stderr
		// streams from external process and populate Record's into ouptut
		// RecordQueue's
		this.stdoutProducer.start();
		this.stderrProducer.start();

		startupComplete = true;
	}

	public boolean isChildProcessFinished() {
		return processMonitor.isChildProcessFinished();
	}

	public boolean areAllChildThreadsRunning() {
		if ((!isChildProcessFinished() && stdoutProducer.isAlive() && stderrProducer
				.isAlive()) || !startupComplete)
			return true;
		return false;
	}

	public void stopChildProcess(Object o) {
		processMonitor.stopChildProcess(o);
	}
}
