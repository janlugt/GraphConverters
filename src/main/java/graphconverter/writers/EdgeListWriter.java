package graphconverter.writers;


import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;

import util.Graph;
import util.NodeEdgeValues;
import util.Settings;

public class EdgeListWriter {
	
	private final static Random rand = new Random();
	
	public static void write(Graph graph, String filename, NodeEdgeValues printValues) throws IOException {
		FileOutputStream fos = new FileOutputStream(filename);
		BufferedOutputStream bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
		PrintStream out = new PrintStream(bos);

		for (Integer source : graph.keySet()) {
			List<Integer> dests = graph.get(source);
			for (Integer dest : dests) {
				out.print(source);
				out.print('\t');
				out.print(dest);
				printEdgeValue(out, printValues);
			}
			out.println();
		}

		out.close();
		bos.close();
		fos.close();
	}

	private static void printEdgeValue(PrintStream out,
			NodeEdgeValues printValues) {
		switch (printValues) {
		case DUMMY:
			out.print('\t');
			out.print(Settings.DUMMY_EDGE_VALUE);
			break;
		case RANDOM_INTS:
			out.print('\t');
			out.print(rand.nextInt(Settings.MAXIMUM_RANDOM_INT));
			break;
		case NONE:
			break;
		}
	}
}
