package com.mapr.distiller.client;

import java.net.URI;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class CoordinatorClient {

  private static final Logger LOG = LoggerFactory
      .getLogger(CoordinatorClient.class);

  private String baseUrl;
  private final RestTemplate restTemplate = new RestTemplate();

  public CoordinatorClient() {

  }

  public CoordinatorClient(String serverUrl, String serverPort) {
    LOG.info("Coordinator Client initial setup");
    this.baseUrl = serverUrl + ":" + serverPort;
  }

  public String getRecordQueues() {
    try {
      URI url = new URI(baseUrl + "/distiller/queues");
      /*
       * ResponseEntity<RecordQueueStatus[]> responseEntity = restTemplate
       * .getForEntity(url, RecordQueueStatus[].class); List<RecordQueueStatus>
       * recordQueueStatus = Arrays.asList(responseEntity .getBody());
       */

      String response = restTemplate.getForObject(url, String.class);
      return response;
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  public String getMetricActions() {
    try {
      URI url = new URI(baseUrl + "/distiller/metricActions");
      String response = restTemplate.getForObject(url, String.class);
      return response;
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  public String getRecords(String queueName, int count) {
    queueName = queueName.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/queues/records/" + queueName
          + "/" + count);
      String response = restTemplate.getForObject(url, String.class);
      return response;
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  public String getMetricAction(String metricActionName) {
    metricActionName = metricActionName.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/metricActions/"
          + metricActionName);
      String response = restTemplate.getForObject(url, String.class);
      return response;
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  public String getQueueStatus(String queueName) {
    queueName = queueName.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/queues/" + queueName);
      String response = restTemplate.getForObject(url, String.class);
      return response;
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }

  public boolean metricDisable(String metricName) {
    metricName = metricName.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/metricActions/disable/"
          + metricName);
      HttpHeaders headers = new HttpHeaders();
      HttpEntity<String> requestEntity = new HttpEntity<String>("");
      ResponseEntity<Boolean> response = restTemplate.exchange(url,
          HttpMethod.PUT, requestEntity, Boolean.class);
      return response.getBody();
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  public boolean metricEnable(String metricName) {
    metricName = metricName.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/metricActions/enable/"
          + metricName);
      HttpHeaders headers = new HttpHeaders();
      HttpEntity<String> requestEntity = new HttpEntity<String>("");
      ResponseEntity<Boolean> response = restTemplate.exchange(url,
          HttpMethod.PUT, requestEntity, Boolean.class);
      return response.getBody();
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  public boolean metricDelete(String metricName) {
    metricName = metricName.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/metricActions/delete/"
          + metricName);
      HttpHeaders headers = new HttpHeaders();
      HttpEntity<String> requestEntity = new HttpEntity<String>("");
      ResponseEntity<Boolean> response = restTemplate.exchange(url,
          HttpMethod.PUT, requestEntity, Boolean.class);
      return response.getBody();
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  public boolean isScheduledMetricAction(String metricAction) {
    metricAction = metricAction.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/metricActions/isScheduled/"
          + metricAction);
      HttpHeaders headers = new HttpHeaders();
      HttpEntity<String> requestEntity = new HttpEntity<String>("");
      ResponseEntity<Boolean> response = restTemplate.exchange(url,
          HttpMethod.GET, requestEntity, Boolean.class);
      return response.getBody();
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  public boolean isRunningMetricAction(String metricAction) {
    metricAction = metricAction.trim();

    try {
      URI url = new URI(baseUrl + "/distiller/metricActions/isRunning/"
          + metricAction);
      HttpHeaders headers = new HttpHeaders();
      HttpEntity<String> requestEntity = new HttpEntity<String>("");
      ResponseEntity<Boolean> response = restTemplate.exchange(url,
          HttpMethod.GET, requestEntity, Boolean.class);
      return response.getBody();
    }

    catch (URISyntaxException e) {
      LOG.error(e.getMessage());
      return false;
    }
  }
}
