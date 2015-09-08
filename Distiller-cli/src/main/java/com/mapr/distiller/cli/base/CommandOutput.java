package com.mapr.distiller.cli.base;

import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Stack;

import org.json.JSONException;
import org.json.JSONString;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to keep structured command output- based on Hierarchy of list of lists
 * May need some refactoring for easier Hierarchy construction
 */
public class CommandOutput implements JSONString {

  private static final Logger LOG = LoggerFactory
      .getLogger(CommandOutput.class);

  protected String response;

  protected OutputHierarchy output;

  protected boolean isLongOutput;

  protected boolean isHeaderRequired = true;

  private String[] nodeOrderByName = null;

  public void setNodeOrder(String[] nodeOrder) {
    this.nodeOrderByName = nodeOrder;
  }

  public String[] getNodeOrder() {
    return this.nodeOrderByName;
  }

  public CommandOutput(String response) {
    this.response = response;
    output = new OutputHierarchy();
  }

  public CommandOutput(OutputHierarchy output) {
    this.output = output;
  }

  public CommandOutput() {
    output = new OutputHierarchy();
  }

  public OutputHierarchy getOutput() {
    return output;
  }

  public void setOutput(OutputHierarchy output) {
    this.output = output;
  }

  public boolean isLongOutput() {
    return isLongOutput;
  }

  public void setLongOutput(boolean isLongOutput) {
    this.isLongOutput = isLongOutput;
  }

  public boolean isHeaderRequired() {
    return isHeaderRequired;
  }

  public void setHeaderRequired(boolean isHeaderRequired) {
    this.isHeaderRequired = isHeaderRequired;
  }

  /**
   * Convert Hierarchy into JSONFormat
   * 
   * @return
   */
  @Override
  public String toJSONString() {
    if (output == null) {
      System.out.println("Output is null");
      if (response == null) {
        return null;
      } else {
        return response;
      }
    }
    return output.toJSONString();
  }

  /**
   * Method to pretty print CommandOutput based on JSON output
   * 
   * @return
   */
  public String toPrettyString() {
    String jsonString = toJSONString();
    if (jsonString == null) {
      return null;
    }
    return toPrettyStringStatic(jsonString);
  }

  /**
   * Helper method for unit testing
   * 
   * @param jsonString
   * @return
   */
  public static String toPrettyStringStatic(String jsonString) {
    if (jsonString == null) {
      return null;
    }
    StringWriter sw = new StringWriter();
    char[] chars = jsonString.toCharArray();
    // need a stack to keep track of open/close quotes, {} and []
    try {
      Stack<Character> charStack = new Stack<Character>();
      // to make stack not empty
      charStack.push(' ');
      int parenCount = 0;
      for (char ch : chars) {
        switch (ch) {
        case '"':
          if (charStack.peek() != ch) {
            // if this is new "
            charStack.push(ch);
          } else {
            // we have pair
            charStack.pop();
          }
          sw.append(ch);
          break;
        case '{':
        case '[':
          sw.append(ch);
          if (charStack.peek() != '"') {
            // this is not part of any name or value
            charStack.push(ch);
            sw.append("\n");
            parenCount++;
            for (int i = 0; i < parenCount; i++) {
              sw.append("\t");
            }
          } // otherwise don't put it
          break;
        case '}':
        case ']':
          if (charStack.peek() != '"') {
            charStack.pop();
            sw.append("\n");
            parenCount--;
            for (int i = 0; i < parenCount; i++) {
              sw.append("\t");
            }
          }
          sw.append(ch);
          break;
        case ',':
          if (charStack.peek() != '"') {
            sw.append(ch);
            sw.append("\n");
            for (int i = 0; i < parenCount; i++) {
              sw.append("\t");
            }
          } else {
            sw.append(ch);
          }
          break;

        default:
          sw.append(ch);
          break;
        }
      }
    } catch (Exception e) {
      /**
       * <MAPR_ERROR> Message:Exception during pretty printing of JSON Output:
       * <JSON> Function:CommandOutput.toPrettyStringStatic() Meaning:An
       * internal error occcurred while formatting the JSON output. Resolution:
       * </MAPR_ERROR>
       */
      LOG.error("Exception during pretty printing of JSON Output: "
          + jsonString, e);
    }

    return sw.toString();
  }

  public String toString() {
    return (response != null) ? response : output.toString();
  }

  public static class OutputHierarchy implements JSONString {
    private static final int NOT_SET = -1;
    private List<OutputNode> outputNodes = new ArrayList<OutputNode>();
    private List<OutputError> outputErrors = new ArrayList<OutputError>();
    private List<String> messages = new ArrayList<String>(); // to be displayed
                                                             // on UI
    private int total = NOT_SET;

    @Override
    public String toString() {
      return "nodes: " + outputNodes + " errors: " + outputErrors;
    }

    public OutputHierarchy() {
    }

    @Override
    public String toJSONString() {
      StringWriter sw = new StringWriter();
      JSONWriter jWriter = new JSONWriter(sw);
      try {
        jWriter.object();
        long currentTime = System.currentTimeMillis();
        DateFormat format = new SimpleDateFormat(
            "yyyy-MM-dd hh:mm:ss.SSS 'GMT'Z");

        jWriter.key("timestamp").value(currentTime);
        jWriter.key("timeofday").value(format.format(new Date(currentTime)));
        if (outputErrors.isEmpty()) {
          // total success
          jWriter.key("status").value("OK");
          jWriter.key("total").value(getTotal());
        } else if (outputNodes.isEmpty() && messages.isEmpty()) {
          // total failure
          jWriter.key("status").value("ERROR");
        } else {
          // partial case
          jWriter.key("status").value("PARTIAL");
        }

        if (outputErrors.isEmpty()) {
          jWriter.key("data").array();
          for (OutputNode node : outputNodes) {
            jWriter.value(node);
          }
          jWriter.endArray();
        }

        if (!messages.isEmpty()) {
          jWriter.key("messages").array();
          for (String message : messages) {
            jWriter.value(message);
          }
          jWriter.endArray();
        }

        // if ( outputErrors.isEmpty() && outputNodes.isEmpty()) { // totally OK
        // response
        // jWriter.key("data").array().endArray();
        // }

        if (!outputErrors.isEmpty()) {
          /*
           * Leave it for now if we decide to do fancier error handling later
           * jWriter.key("errors").object(); Map<Integer, List<OutputError>>
           * errorMap = new HashMap<Integer, List<OutputError>>(); for (
           * OutputError error: outputErrors ) { if (
           * !errorMap.containsKey(error.getErrorCode())) {
           * errorMap.put(error.getErrorCode(), new ArrayList<OutputError>()); }
           * errorMap.get(error.getErrorCode()).add(error); } for (
           * Map.Entry<Integer, List<OutputError>> entry : errorMap.entrySet())
           * { // TODO change to named const
           * jWriter.key(entry.getKey().toString()); jWriter.array(); for (
           * OutputError error : entry.getValue()) { error.toJSON(jWriter); }
           * jWriter.endArray(); } jWriter.endObject();
           */
          jWriter.key("errors").array();
          for (OutputError error : outputErrors) {
            error.toJSON(jWriter);
          }
          jWriter.endArray();
        }
        jWriter.endObject();
      } catch (JSONException e) {
        /**
         * <MAPR_ERROR> Message:JSON Parsing Exception: <exception> DATA:
         * <string> Function:CommandOutput.toJSONString() Meaning:An internal
         * error occurred while parsing the JSON. Resolution: </MAPR_ERROR>
         */
        LOG.error("JSON Parsing Exception: " + e + " DATA:" + sw.toString());
      }
      return sw.toString();
    }

    public List<OutputNode> getOutputNodes() {
      return outputNodes;
    }

    public void setOutputNodes(List<OutputNode> outputNodes) {
      this.outputNodes = outputNodes;
    }

    private int getTotal() {
      return total == NOT_SET ? outputNodes.size() : total;
    }

    public void setTotal(int total) {
      this.total = total;
    }

    private boolean canGroup(OutputNode newNode) {
      for (OutputNode node : outputNodes) {
        if (node.getName() != null && node.getName().equals(newNode.getName())) {
          node.addPeer(newNode);
          return true;
        }
      }
      return false;
    }

    public void addNode(OutputNode node) {
      if (!canGroup(node))
        outputNodes.add(node);
    }

    public List<OutputError> getOutputErrors() {
      return outputErrors;
    }

    public void setOutputErrors(List<OutputError> outputErrors) {
      this.outputErrors = outputErrors;
    }

    public void addError(OutputError error) {
      outputErrors.add(error);
    }

    public void addMessage(String message) {
      messages.add(message);
    }

    public List<String> getMessages() {
      return messages;
    }

    public static class OutputNode implements JSONString {
      private String name = null;
      /*
       * A node either has a value (a leaf node) or it has children (a parent
       * node)
       */
      private Object value = null;
      private List<OutputNode> children = new ArrayList<OutputNode>();
      /*
       * Peers are used for groupby operation. A parent node can have peers with
       * same name. A leaf node can have peers with same name. All peers will be
       * represented as an array
       */
      private List<OutputNode> peers = new ArrayList<OutputNode>();

      public OutputNode() {
        this.name = null;
        this.value = null;
      }

      public OutputNode(String name) {
        this.name = name;
        this.value = null;
      }

      public OutputNode(String name, Object value) {
        this.name = name;
        this.value = value;
      }

      public OutputNode(String name, int value) {
        this.name = name;
        this.value = Integer.valueOf(value);
      }

      public OutputNode(String name, long value) {
        this.name = name;
        this.value = Long.valueOf(value);
      }

      @Override
      public String toJSONString() {
        StringWriter sw = new StringWriter();
        JSONWriter jWriter = new JSONWriter(sw);
        try {
          if (this.isLeafNode() == true) {
            if (peers.isEmpty()) {
              /* It must have a name and a value. */
              jWriter.object();
              jWriter.key(getName()).value(getValue());
              jWriter.endObject();
              return sw.toString();
            } else {
              /* This is a leaf node all its peers should be leaf nodes */
              /* start an array */
              jWriter.object();
              jWriter.key(this.getName()).array();
              for (OutputNode peer : peers) {
                if (peer.isLeafNode()) {
                  jWriter.value(peer.getValue());
                } else {
                  /*
                   * We have a leaf node whose peer is non-leaf node FIXME:
                   * Should this be allowed?
                   */
                  jWriter.value(peer);
                }
              }
              jWriter.endArray();
              jWriter.endObject();
              return sw.toString();
            }
          } else {
            /* This is a non leaf node */
            if (peers.isEmpty()) {
              jWriter.object();
              for (OutputNode child : children) {
                /* If a child is a leaf node add k,v */
                if (child.isLeafNode() == true) {
                  if (child.getPeers().isEmpty()) {
                    jWriter.key(child.getName()).value(child.getValue());
                  } else {
                    /* Child itself is an array of (most likely) non-leaf nodes */
                    jWriter.key(child.getName()).array();
                    for (OutputNode peer : child.getPeers()) {
                      if (peer.isLeafNode() == true) {
                        /*
                         * We have a non leaf node whose peer is leaf node.
                         * FIXME: Should this be allowed?
                         */
                        jWriter.value(peer.getValue());
                      } else {
                        if (!peer.isEmpty()) {
                          jWriter.value(peer);
                        }
                      }
                    }
                    jWriter.endArray();
                  }
                } else {
                  /* child is a non-leaf node it must have a name */
                  if (child.getPeers().isEmpty()) {
                    jWriter.key(child.getName()).value(child);
                  } else {
                    /* Child itself is an array of (most likely) non-leaf nodes */
                    jWriter.key(child.getName()).array();
                    for (OutputNode peer : child.getPeers()) {
                      if (peer.isLeafNode() == true) {
                        /*
                         * We have a non leaf node whose peer is leaf node.
                         * FIXME: Should this be allowed?
                         */
                        jWriter.value(peer.getValue());
                      } else {
                        if (!peer.isEmpty()) {
                          jWriter.value(peer);
                        }
                      }
                    }
                    jWriter.endArray();
                  }
                }
              }
              jWriter.endObject();
              return sw.toString();
            } else {
              /* start an array */
              jWriter.object().key(this.getName()).array();
              /* add all values */
              for (OutputNode peer : peers) {
                if (peer.isLeafNode() == true) {
                  /*
                   * We have a non leaf node whose peer is leaf node. FIXME:
                   * Should this be allowed?
                   */
                  jWriter.value(peer.getValue());
                } else {
                  if (!peer.isEmpty()) {
                    jWriter.value(peer);
                  }
                }
              }
              jWriter.endArray();
              jWriter.endObject();
            }
          }
        } catch (JSONException e) {
          /**
           * <MAPR_ERROR> Message:JSON Parsing Exception: <exception> DATA:
           * <string> Function:CommandOutput.toJSONString() Meaning:An internal
           * error occurred while parsing the JSON. Resolution: </MAPR_ERROR>
           */
          LOG.error("JSON Parsing Exception" + e + "DATA: " + sw.toString());
        }
        return sw.toString();
      }

      private boolean isEmpty() {
        if ((getName() == null || getName().isEmpty()) && getValue() == null
            && this.getChildren().isEmpty() && this.getPeers().isEmpty()) {
          return true;
        }
        return false;
      }

      public Object getValue() {
        return value;
      }

      public void setValue(Object v) {
        this.value = v;
      }

      public void setName(String n) {
        this.name = n;
      }

      public List<OutputNode> getChildren() {
        return children;
      }

      public List<OutputNode> getPeers() {
        return peers;
      }

      public void setChildren(List<OutputNode> children) {
        this.children = children;
      }

      private boolean isLeafNode() {
        if (value != null) {
          return true;
        }
        /*
         * value is null so go down its peer list to see if it is an array of
         * values
         */
        if (!this.peers.isEmpty()) {
          OutputNode peer = this.peers.get(0);
          if (peer.getValue() != null)
            return true;
        }
        return false;
      }

      public void addPeer(OutputNode peerNode) {
        if ((this.isLeafNode() && peerNode.isLeafNode())
            || (!this.isLeafNode() && !peerNode.isLeafNode())) {
          /* Reset name to null */
          peerNode.setName(null);
          /*
           * If this is the first time we are adding make another o/p node and
           * add it to peer list Any array node will look like <dummy node
           * carrying name of array> -> peer -> peer peers carry values. Its
           * used to make recursion work.
           */
          if (peers.isEmpty()) {
            OutputNode node = new OutputNode();
            node.setChildren(this.getChildren());
            node.setValue(this.getValue());
            node.setName(null);
            this.children = new ArrayList<OutputNode>();
            this.value = null;
            this.peers.add(node);
          }
          /* now add the actual peer */
          this.peers.add(peerNode);
        } else {
          System.out.println("Serious error while grouping " + this.getName()
              + " source: " + this.isLeafNode() + " peer: "
              + peerNode.isLeafNode());
        }
      }

      private boolean canGroup(OutputNode newNode) {
        for (OutputNode node : children) {
          if (node.getName() != null
              && node.getName().equals(newNode.getName())) {
            node.addPeer(newNode);
            return true;
          }
        }
        return false;
      }

      public void addChild(OutputNode child) {
        if (!canGroup(child)) {
          children.add(child);
        }
      }

      public String getName() {
        return name;
      }

      public void addNode(OutputNode node) {
        if (!canGroup(node)) {
          children.add(node);
        }
      }

      @Override
      public String toString() {
        return "Name: " + name + ", Value: " + value + " children: " + children;
      }

      public String childrenString() {
        return "...";
      }

      public String peersString() {
        return "...";
      }
    }

    public static class OutputError {
      // final private ErrorCodeEnum ErrorCodeEnum;
      final private int errorCode;
      final private String errorDescription;
      private String field;
      private String fieldValue;
      private boolean propagateErrorSupport;

      public OutputError(int errorCode, String errorDescription) {
        this.errorCode = errorCode;
        this.errorDescription = errorDescription;
        propagateErrorSupport = false;
      }

      public void toJSON(JSONWriter jWriter) throws JSONException {
        jWriter.object();
        // jWriter.object().key("errorid").value(ErrorCodeEnumEnum);
        /*
         * Leave for later fancier error handling if ( field != null ) {
         * jWriter.key("field").value(field); if ( fieldValue != null ) {
         * jWriter.key("value").value("fieldValue"); } }
         */
        jWriter.key("id").value(errorCode);
        jWriter.key("desc").value(errorDescription).endObject();
      }

      public int getErrorCode() {
        return errorCode;
      }

      public String getErrorDescription() {
        return errorDescription;
      }

      public OutputError setField(String field) {
        this.field = field;
        return this;
      }

      public String getField() {
        return field;
      }

      public OutputError setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
        return this;
      }

      public String getFieldValue() {
        return fieldValue;
      }

      public boolean isPropagateErrorSupported() {
        return propagateErrorSupport;
      }

      public OutputError setPropagateErrorSupport(boolean val) {
        propagateErrorSupport = val;
        return this;
      }

      @Override
      public String toString() {
        return "errorCode: " + errorCode + ", ErrorDescription: "
            + errorDescription;
      }
    }
  }

}
