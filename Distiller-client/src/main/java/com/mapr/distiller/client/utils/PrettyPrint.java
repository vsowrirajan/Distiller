package com.mapr.distiller.client.utils;

import java.io.StringWriter;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrettyPrint {

  private static final Logger LOG = LoggerFactory.getLogger(PrettyPrint.class);

  public static String toPrettyString(String jsonString) {
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
}
