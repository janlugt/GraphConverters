package graphconverter.readers;


import graphconverter.util.Graph;
import graphconverter.util.Settings;
import graphconverter.util.TextFunctions;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;


public class AdjacencyListReader {
	public static Graph read(String filename, int nodeProps, int edgeProps) throws IOException {
		// Input format: [source vertex id] [destination vertex id]\n
		
		// Create scanner for input
		FileInputStream fileInput = new FileInputStream(filename);
		BufferedInputStream bufferedInput = new BufferedInputStream(fileInput, Settings.BUFFERED_STREAM_SIZE);
		Scanner lineScanner = new Scanner(bufferedInput);
		lineScanner.useDelimiter(Pattern.compile(Settings.NEWLINE));
		
		// Read input to map
		Graph graph = new Graph();
		while (lineScanner.hasNext()) {
			String line = lineScanner.next();
			if (TextFunctions.isComment(line)) {
				continue;
			}
			String[] vertices = TextFunctions.splitLine(line);
			Integer source = Integer.parseInt(vertices[0]);
			graph.addVertex(source);
			// Skip over edge values, so += 2
			for (int i = 1 + nodeProps; i < vertices.length; i += 1 + edgeProps) {
				Integer dest = Integer.parseInt(vertices[i]);
				graph.addVertex(dest);
				graph.addEdge(source, dest);
			}
		}
		return graph;
	}
}
