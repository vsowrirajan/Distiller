import java.util.Map;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.xmlrpc.server.XmlRpcServer;
import org.apache.xmlrpc.server.XmlRpcServerConfigImpl;
import org.apache.xmlrpc.webserver.WebServer;
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.XmlRpcException;

import com.mapr.distiller.server.coordinators.IostatProcessRecordProducer;
import com.mapr.distiller.server.coordinators.IostatRecordToComponentRecordProducer;
import com.mapr.distiller.server.coordinators.SystemCpuRecordToSystemCpuHighRecordProducer;
import com.mapr.distiller.server.coordinators.SystemCpuRecordToSystemCpuMovingAverageRecordProducer;
import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.utils.DefaultConfiguration;

public class SystemMonitor {

	public static class ViewerClient {
		public String listRecordQueues() {
			String ret = "";
			Iterator<Map.Entry<String, RecordQueue>> i = nameToRecordQueueMap
					.entrySet().iterator();
			while (i.hasNext()) {
				Map.Entry<String, RecordQueue> p = (Map.Entry<String, RecordQueue>) i
						.next();
				ret = ret + "Queue Name:" + p.getKey() + " qid:" + p.getValue()
						+ " size:" + p.getValue().queueSize() + " consumers";
				String[] consumers = p.getValue().listConsumers();
				String[] producers = p.getValue().listProducers();
				for (int x = 0; x < consumers.length; x++) {
					ret = ret + ":" + consumers[x];
				}
				ret = ret + " producers";
				for (int x = 0; x < producers.length; x++) {
					ret = ret + ":" + producers[x];
				}
				ret = ret + "\n";
			}
			return ret;
		}

		public String printRecords(String queueName) {
			if (!nameToRecordQueueMap.containsKey(queueName)) {
				return "No such RecordQueue: " + queueName;
			}
			RecordQueue q = nameToRecordQueueMap.get(queueName);
			if (q.queueSize() > 100) {
				System.err
						.println("Request received to print all records from queue "
								+ queueName
								+ " with more than 100 records, will limit results to 100");
				return printNewestRecords(queueName, 100);
			}
			return q.printRecords();
		}

		public String printNewestRecords(String queueName, int numRecords) {
			if (numRecords > 100) {
				System.err.println("Request received to print " + numRecords
						+ " records from queue " + queueName
						+ ", will limit results to 100");
				numRecords = 100;
			}
			if (!nameToRecordQueueMap.containsKey(queueName)) {
				return "No such RecordQueue: " + queueName;
			}
			RecordQueue q = nameToRecordQueueMap.get(queueName);
			return q.printNewestRecords(numRecords);
		}

	};

	private final WebServer webServer;
	private final XmlRpcServer xmlRpcServer;

	public SystemMonitor(int port) {
		webServer = new WebServer(port);
		xmlRpcServer = webServer.getXmlRpcServer();
		PropertyHandlerMapping phm = new PropertyHandlerMapping();

		try {
			phm.addHandler("ViewerClient", ViewerClient.class);
		} catch (XmlRpcException e) {
			System.err.println("Failed to setup RPC server");
			e.printStackTrace();
			System.exit(1);
		}

		xmlRpcServer.setHandlerMapping(phm);
		XmlRpcServerConfigImpl serverConfig = (XmlRpcServerConfigImpl) xmlRpcServer
				.getConfig();
		serverConfig.setEnabledForExceptions(true);
		serverConfig.setContentLengthOptional(false);
		try {
			webServer.start();
		} catch (Exception e) {
			System.err.println("Failed to start RPC server");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static ConcurrentHashMap<String, RecordQueue> nameToRecordQueueMap;

	public static void main(String[] args) {
		boolean shouldExit = false;
		nameToRecordQueueMap = new ConcurrentHashMap<String, RecordQueue>(1000);

		// Generate the configuration that decides how SystemMonitor will
		// run/what it will do
		Properties configuration = DefaultConfiguration.getConfiguration();

		for (int argc = 0; argc < args.length; argc++) {
			String[] substrings = args[argc].split(":", 2);
			if (substrings.length > 1 && substrings[0].equals("conf")
					&& substrings[1].length() > 0) {
				configuration = DefaultConfiguration.applyConfigurationFile(
						configuration, substrings[1]);

			} else {
				System.err
						.println("Unknown command line option: " + args[argc]);
				System.exit(1);
			}
		}

		// Configuration done, implement monitoring defined by configuration.

		// Start the RPC server for client requests
		SystemMonitor myRpcServer = new SystemMonitor(
				Integer.parseInt(configuration.getProperty("rpc.server.port",
						"23721")));

		// Eventually, this next step should be to dynamically create Monitor's
		// based on a configuration file. For now, there is a set of diagnostics
		// that are statically defined and must be referenced by their given
		// configuration properties.
		// Start iostatProcessRecordProducer if enabled
		IostatProcessRecordProducer iostatProcessRecordProducer = null;
		IostatRecordToComponentRecordProducer iostatRecordToComponentRecordProducer = null;
		SystemCpuRecordToSystemCpuHighRecordProducer systemCpuRecordToSystemCpuHighRecordProducer = null;
		SystemCpuRecordToSystemCpuMovingAverageRecordProducer systemCpuRecordToSystemCpuMovingAverageRecordProducer = null;
		if (Boolean.valueOf(configuration
				.getProperty("monitor.iostat", "false"))) {
			iostatProcessRecordProducer = new IostatProcessRecordProducer(
					nameToRecordQueueMap);
			iostatProcessRecordProducer.start();
			iostatRecordToComponentRecordProducer = new IostatRecordToComponentRecordProducer(
					nameToRecordQueueMap, "Iostat");
			iostatRecordToComponentRecordProducer.start();
			systemCpuRecordToSystemCpuHighRecordProducer = new SystemCpuRecordToSystemCpuHighRecordProducer(
					nameToRecordQueueMap, "Iostat", 90d);
			systemCpuRecordToSystemCpuHighRecordProducer.start();
			systemCpuRecordToSystemCpuMovingAverageRecordProducer = new SystemCpuRecordToSystemCpuMovingAverageRecordProducer(
					nameToRecordQueueMap, "Iostat", 10);
			systemCpuRecordToSystemCpuMovingAverageRecordProducer.start();
		}

		// Monitors are started, now wait until its time to shut down.
		long lastStatus = 0l, statusInterval = 10000l;
		while (!shouldExit) {
			if (System.currentTimeMillis() - statusInterval > lastStatus) {
				lastStatus = System.currentTimeMillis();
				Iterator<Map.Entry<String, RecordQueue>> it = nameToRecordQueueMap
						.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String, RecordQueue> pair = (Map.Entry<String, RecordQueue>) it
							.next();
					RecordQueue q = (RecordQueue) pair.getValue();
					System.err.println(System.currentTimeMillis()
							+ " RecordQueue " + pair.getKey() + " contains "
							+ q.queueSize() + " records");
				}
				System.err.println();
			}
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				if (!shouldExit) {
					System.err
							.println("SystemMonitor shutting down due to exception");
					e.printStackTrace();
					shouldExit = true;
				}
				System.err.println("SystemMonitor shutting down by request");
			}
		}

		// Its time to shut down, inform all Monitors.
		if (iostatProcessRecordProducer != null
				&& iostatProcessRecordProducer.isAlive()) {
			iostatProcessRecordProducer.exitRequest();
		}

		// Wait for all Monitors to shut down, interrupt them if this shutdown
		// thread is interrupted.
		while (iostatProcessRecordProducer.isAlive()) {
			try {
				Thread.sleep(1000);
			} catch (Exception E) {
				System.err
						.println("SystemMonitor interrupted during shutdown, interrupting child thread iostatProcessRecordProducer");
				iostatProcessRecordProducer.interrupt();
			}
		}

		// All Monitors spawned by this SystemMonitor are stopped.
		return;
	}

}