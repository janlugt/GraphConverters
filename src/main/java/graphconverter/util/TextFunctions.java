package graphconverter.util;

public class TextFunctions {
	public static String[] splitLine(String line) {
		return line.split("\t");
	}

	public static  boolean isComment(String line) {
		return !Character.isDigit(line.charAt(0));
	}
}
