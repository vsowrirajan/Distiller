package com.mapr.distiller.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.client.utils.PrettyPrint;

// This class is here temporarily for testing client interactions. Once we are done, we can remove this class.
public class App {

  private static final Logger LOG = LoggerFactory.getLogger(App.class);

  public static void main(String[] args) {
    CoordinatorClient client = new CoordinatorClient("http://localhost", "8080");

    // 1. Get status of the Record queues
    System.out.println(PrettyPrint.toPrettyString(client.getRecordQueues()));

    // 2. Get status of the Metric Actions
    System.out.println(PrettyPrint.toPrettyString(client.getMetricActions()));

    String queueName = "ThreadResourceEvents-60sI";
    int count = 100;

    // 3. Get Records from a particular queue with a count
    System.out.println(PrettyPrint.toPrettyString(client.getRecords(queueName,
        count)));

    String metricActionName = "CumulativeSystemMemory-1sI";

    // 4. Get status of a particular MetricAction
    System.out.println(PrettyPrint.toPrettyString(client
        .getMetricAction(metricActionName)));
    
    //5. Get status of a particular queue
    System.out.println(PrettyPrint.toPrettyString(client
        .getQueueStatus(queueName)));
    
    //6. Disable MetricAction
    System.out.println(client.metricDisable(metricActionName));
    
    //7. Enable MetricAction
    System.out.println(client.metricEnable(metricActionName));
    
    //8. Is Running MetricAction
    System.out.println(client.isRunningMetricAction(metricActionName));
    
    //9. Is Scheduled MetricAction
    System.out.println(client.isScheduledMetricAction(metricActionName));
    
  }
}
