package com.mapr.distiller.cli.filters;

import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilteringHelper {

  public static Pattern pGlobal = Pattern
      .compile("(==)|(\\!=)|(>=)|(<=)|(>)|(<)");
  public static Pattern p = Pattern
      .compile("(and)|(or)|(\\[[\\w. <>=!\\*]++\\])");

  public static List<String> filter(List<String> nodeNames, String filter) {
    List<String> retList = new ArrayList<String>();

    List<String> groups = new ArrayList<String>();
    Matcher m = p.matcher(filter);
    while (m.find()) {
      groups.add(m.group().trim());
    }

    for (String node : nodeNames) {
      if (filtering(node, groups)) {
        retList.add(node);
      }
    }
    return retList;
  }

  private static boolean filtering(String node, List<String> groups) {

    Stack<Object> filtersStack = new Stack<Object>();
    for (String group : groups) {
      if (group.startsWith("[")) {
        Boolean prevOperand = null;
        try {
          String op = (String) filtersStack.peek();
          if (op.equalsIgnoreCase("or")) {
            // need to get previous value and reevaluate
            filtersStack.pop();
            prevOperand = (Boolean) filtersStack.pop();
          }
        } catch (EmptyStackException e) {
          // this is first element
        }
        boolean eval = evaluate("node", node, group.replaceFirst("^\\[", "")
            .replaceFirst("\\]$", ""));
        if (prevOperand != null) {
          eval |= prevOperand;
        }
        // evaluate group itself
        filtersStack.push(Boolean.valueOf(eval));
      } else if (group.equalsIgnoreCase("and")) {
        if (((Boolean) filtersStack.peek()) == Boolean.FALSE) {
          // it will be false anyway
          return false;
        }
        filtersStack.push(group);
      } else if (group.equalsIgnoreCase("or")) {
        // pop previous one from stack
        // evaluate and push back
        filtersStack.push(group);
      }
    }
    // deal with "and"(s)

    while (!filtersStack.isEmpty()) {
      Boolean operand = (Boolean) filtersStack.pop();
      if (filtersStack.isEmpty()) {
        return operand;
      }
      filtersStack.pop(); // operation - should be only "and"
      Boolean operand2 = (Boolean) filtersStack.pop();
      if (!(operand && operand2)) {
        return false;
      } else if (!filtersStack.isEmpty()) {
        filtersStack.push(Boolean.valueOf(operand && operand2));
      } else {
        return operand && operand2;
      }
    }
    return false;
  }

  static boolean evaluate(String fieldName, String fieldValue, String filter) {
    Matcher m = pGlobal.matcher(filter);
    if (m.find()) {
      String op = m.group();
      String[] operands = filter.split(op);
      if (operands == null || operands.length != 2
          || !operands[0].trim().equalsIgnoreCase(fieldName)) {
        return false;
      }
      if (op.equalsIgnoreCase("==")) {
        return fieldValue.matches(operands[1].trim());
      }
      if (op.equalsIgnoreCase("!=")) {
        return !fieldValue.matches(operands[1].trim());
      }
      if (op.equalsIgnoreCase(">")) {
        return (fieldValue.compareTo(operands[1].trim()) > 0) ? true : false;
      }
      if (op.equalsIgnoreCase(">=")) {
        return (fieldValue.compareTo(operands[1].trim()) >= 0) ? true : false;
      }
      if (op.equalsIgnoreCase("<")) {
        return (fieldValue.compareTo(operands[1].trim()) < 0) ? true : false;
      }
      if (op.equalsIgnoreCase("<=")) {
        return (fieldValue.compareTo(operands[1].trim()) <= 0) ? true : false;
      }
    }

    return false;
  }
}
