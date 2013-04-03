package graphconverter;

import graphconverter.readers.*;
import graphconverter.util.Graph;
import graphconverter.util.NodeEdgeValues;
import graphconverter.writers.*;

import java.io.FileNotFoundException;
import java.io.IOException;


public class Main {

	private String filename;
	private String filenameBase;
	
	private Main(String[] args) throws FileNotFoundException {
		if (args.length != 1) {
			System.out.println("Please provide a path to an input file as a parameter.");
			System.exit(-1);
		}
		filename = args[0];
		filenameBase = filename.substring(0, filename.length() - 4);
	}

	private void start() throws IOException {
		/*
		 * Uncomment any of the lines below to choose an input format
		 */
		//Graph graph = MultiThreadedEdgeListReader.read(filename);
		//Graph graph = EdgeListReader.read(filename);
		Graph graph = AdjacencyListReader.read(filename, 0, 0);
		//Graph graph = GreenMarlBinaryReader.read(filename);

		/*
		 * Print some debug stuff
		 */
		System.err.println("NumNodes: " + graph.totalVertexCount);
		System.err.println("NumEdges: " + graph.totalEdgeCount);
		
		/*
		 * Uncomment the line below to perform some checks for correctness
		 */
		//GraphChecker.check(graph);
		
		/*
		 * Uncomment any of the lines below to specify output formats
		 */
		AdjacencyListWriter.write(graph, filenameBase + ".adj", NodeEdgeValues.DUMMY);
		AdjacencyListWriter.write(graph, filenameBase + "_novalues.adj", NodeEdgeValues.NONE);
		AdjacencyListWriter.write(graph, filenameBase + "_random_ints.adj", NodeEdgeValues.RANDOM_INTS);
		GreenMarlBinaryWriter.write(graph, filenameBase + ".bin");
		SvcIIWriter.write(graph, filenameBase + "_svcii_1", 1, true);
		SvcIIWriter.write(graph, filenameBase + "_svcii_2", 2, true);
		SvcIIWriter.write(graph, filenameBase + "_svcii_4", 4, true);
		SvcIIWriter.write(graph, filenameBase + "_svcii_8", 8, true);
		SvcIIWriter.write(graph, filenameBase + "_svcii_4", 16, true);
		SvcIIWriter.write(graph, filenameBase + "_svcii_8", 32, true);
		SvcIIWriter.write(graph, filenameBase + "_svcii_4", 64, true);
		SvcIIWriter.write(graph, filenameBase + "_svcii_8", 128, true);
		AvroWriter.write(graph, filenameBase + ".avro");
	}
	
	public static void main(String[] args) {
		try {
			long start = System.currentTimeMillis();
			new Main(args).start();
			long stop = System.currentTimeMillis();
			System.err.printf("Converter ran in %.2f seconds\n", (float)(stop - start) / 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
