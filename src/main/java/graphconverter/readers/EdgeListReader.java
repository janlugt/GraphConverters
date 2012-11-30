package graphconverter.readers;

import graphconverter.util.Graph;
import graphconverter.util.Settings;
import graphconverter.util.TextFunctions;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;


public class EdgeListReader {
	public static Graph read(String filename) throws IOException {
		// Input format: [source vertex id] [destination vertex id]\n
		
		// Create scanner for input
		FileInputStream fileInput = new FileInputStream(filename);
		BufferedInputStream bufferedInput = new BufferedInputStream(fileInput, Settings.BUFFERED_STREAM_SIZE);
		Scanner lineScanner = new Scanner(bufferedInput);
		lineScanner.useDelimiter(Pattern.compile(Settings.NEWLINE));
		
		// Read input to graph
		Graph graph = new Graph();
		while (lineScanner.hasNext()) {
			String line = lineScanner.next();
			if (TextFunctions.isComment(line)) {
				continue;
			}
			String[] vertices = TextFunctions.splitLine(line);
			Integer source = Integer.parseInt(vertices[0]);
			Integer dest = Integer.parseInt(vertices[1]);
			graph.addVertex(source);
			graph.addVertex(dest);
			graph.addEdge(source, dest);
		}
		return graph;
	}
}
