package com.mapr.distiller.cli.base;

/**
 * Returns string representation of the output
 * @author yufeldman
 *
 */
public class TextCommandOutput extends CommandOutput {

	
	public TextCommandOutput(byte[] buf) {
		super(buf);
	}

	public TextCommandOutput() {
		
	}
	@Override
	public String toString() {
		if ( buf == null ) {
			return null;
		}
		return new String(buf);
	}
	
	@Override
	public String toPrettyString() {
		return toString();
	}

}
