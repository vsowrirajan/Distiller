package com.mapr.distiller.cli.base;

/**
 * Returns string representation of the output
 */
public class TextCommandOutput extends CommandOutput {

  public TextCommandOutput(String response) {
    super(response);
  }

  public TextCommandOutput() {

  }

  @Override
  public String toString() {
    if (response == null) {
      return null;
    }
    return response;
  }

  @Override
  public String toPrettyString() {
    return CommandOutput.toPrettyStringStatic(response);
  }

}
