package com.mapr.distiller.cli.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputError;
import com.mapr.distiller.cli.base.CommandOutput.OutputHierarchy.OutputNode;

public class CommandLineOutput extends CommandOutput {

  private static final Logger LOG = LoggerFactory
      .getLogger(CommandLineOutput.class);

  public CommandLineOutput() {
    super();
  }

  public CommandLineOutput(String response) {
    super(response);
  }

  public CommandLineOutput(OutputHierarchy outHier) {
    super(outHier);
  }

  @Override
  public String toPrettyString() {
    StringBuilder sb = new StringBuilder();
    List<OutputNode> dataNodes = output.getOutputNodes();
    List<OutputError> errors = output.getOutputErrors();
    List<String> messages = output.getMessages();

    if (!messages.isEmpty()) {
      // some error conditions. list them
      sb.append("\n");
      for (String message : messages) {
        sb.append(message);
        sb.append("\n");
      }
    }

    if (!errors.isEmpty()) {
      // some error conditions. list them
      for (OutputError error : errors) {
        sb.append("ERROR (");
        sb.append(error.getErrorCode());
        sb.append(") -  ");
        sb.append(error.getErrorDescription());
        sb.append("\n");
      }
      return sb.toString();
    }
    // now only results left
    boolean isFirst = isHeaderRequired();
    Map<String, ColumnData> output = new HashMap<String, ColumnData>();
    int count = 0;

    for (OutputNode node : dataNodes) {
      // check if highest level has anything here and it is not: "status":"OK"
      if (node.getName() != null) {
        parseNode(null, isFirst, output, count);
      } else {
        parseNode(node, isFirst, output, count);
      }
      count++;
      isFirst = false;
    }

    if (output.size() > 0) {
      // append the messages to the output
      String outTxt = formatOutput(output, isHeaderRequired());
      sb.insert(0, outTxt);
      return sb.toString();
    }
    return "";
  }

  /**
   * Helper method to fill out List<ColumnData> structure to format output later
   * 
   * @param node
   * @param isHeaderNeeded
   * @param output
   * @param count
   */
  private void parseNode(OutputNode node, boolean isHeaderNeeded,
      Map<String, ColumnData> output, int count) {
    List<OutputNode> dataNodes = this.output.getOutputNodes();
    if (node != null) {
      dataNodes = node.getChildren();
    }

    for (OutputNode child : dataNodes) {
      // supposedly this is the leaf one
      if (child.getName() != null) {
        if (output.containsKey(child.getName())) {
          // we have seen this column header before
          // just need to continue adding values to it
        } else {
          output.put(child.getName(), new ColumnData(child.getName().length()));
        }
      }

      String value = "";
      if (child.getValue() != null) {
        value = child.getValue().toString();
        if (child.getValue() instanceof OutputNode) {
          value = child.toJSONString();
        }
      } else {
        // value is null - looks like it has children and/or peers
        if (!child.getChildren().isEmpty() || !child.getPeers().isEmpty()) {
          if (isLongOutput()) {
            value = child.toJSONString();
          } else if (child.getPeers().isEmpty()) {
            value = child.childrenString();
          } else if (child.getChildren().isEmpty()) {
            value = child.peersString();
          } else {
            value = "...";
          }
        }
      }

      output.get(child.getName()).setMaxColumnLength(value.length());
      output.get(child.getName()).add(new RowField(count, value));
    }
    return;
  }

  /**
   * Helper method to produce String output based on List<ColumnData> data
   * structure
   * 
   * @param output
   * @return
   */
  private String formatOutput(Map<String, ColumnData> output,
      boolean isHeaderNeeded) {
    StringBuilder sb = new StringBuilder();

    String[] nodeOrder = null;
    if (getNodeOrder() == null) {
      Set<String> nodeNameSet = output.keySet();
      nodeOrder = nodeNameSet.toArray(new String[nodeNameSet.size()]);
    } else {
      nodeOrder = getNodeOrder();
    }
    if (isHeaderNeeded) {
      for (String header : nodeOrder) {
        int maxColWidth = output.get(header).getMaxColumnLength();
        char[] asChars = Arrays.copyOf(header.toCharArray(), maxColWidth);
        Arrays.fill(asChars, header.length(), asChars.length, ' ');
        sb.append(asChars);
        sb.append("  ");
      }
      sb.append("\n");
    }

    int index = 0;
    int maxColLen = 0;

    do {
      for (String header : nodeOrder) {
        ColumnData cData = output.get(header);
        int maxColWidth = cData.getMaxColumnLength();
        if (maxColLen < cData.getFields().size()) {
          maxColLen = cData.getFields().size();
        }
        RowField rF = cData.findByRowNum(index);
        String field = "";
        if (rF != null) {
          field = rF.getField();
        } else {
          // skip the value, as it was not provided
        }
        char[] asChars = Arrays.copyOf(field.toCharArray(), maxColWidth);
        Arrays.fill(asChars, field.length(), asChars.length, ' ');
        sb.append(asChars);
        sb.append("  ");
      }
      index++;
      sb.append("\n");
    } while (index < maxColLen);

    return sb.toString();
  }

  /**
   * Helper class to keep list of fields (column) and max column width together
   * for nicely formatted output
   * 
   * @author yufeldman
   *
   */
  private static class ColumnData {

    int maxColumnLength;
    List<RowField> fields = new ArrayList<RowField>();

    @SuppressWarnings("unused")
    public ColumnData() {

    }

    public ColumnData(int columnLength) {
      maxColumnLength = columnLength;
    }

    public void setMaxColumnLength(int length) {
      if (maxColumnLength < length) {
        maxColumnLength = length;
      }
    }

    public void add(RowField field) {
      fields.add(field);
    }

    @SuppressWarnings("unused")
    public void setFields(List<RowField> fields) {
      this.fields = fields;
    }

    public int getMaxColumnLength() {
      return maxColumnLength;
    }

    public List<RowField> getFields() {
      return fields;
    }

    public RowField findByRowNum(int rowNum) {
      for (RowField rowField : fields) {
        if (rowField.getRowNum() == rowNum) {
          return rowField;
        }
      }
      return null;
    }
  }

  /**
   * Helper class to keep field and it's index in column
   * 
   * @author yufeldman
   *
   */
  private static class RowField {
    final private int rowNum;
    final private String field;

    public RowField(int rowNum, String field) {
      this.rowNum = rowNum;
      this.field = field;
    }

    public int getRowNum() {
      return rowNum;
    }

    public String getField() {
      return field;
    }
  }

}
