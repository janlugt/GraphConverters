package graphconverter;

import graphconverter.readers.AdjacencyListReader;
import graphconverter.readers.EdgeListReader;
import graphconverter.readers.GreenMarlBinaryReader;
import graphconverter.writers.AdjacencyListWriter;
import graphconverter.writers.AvroWriter;
import graphconverter.writers.GreenMarlBinaryWriter;
import graphconverter.writers.SvcIIWriter;

import java.io.FileNotFoundException;
import java.io.IOException;

import util.Graph;
import util.NodeEdgeValues;

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
		Graph graph = EdgeListReader.read(filename);
		//Graph graph = AdjacencyListReader.read(filename);
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
		SvcIIWriter.write(graph, filenameBase + "_svcii_1", 1);
		SvcIIWriter.write(graph, filenameBase + "_svcii_2", 2);
		SvcIIWriter.write(graph, filenameBase + "_svcii_4", 4);
		SvcIIWriter.write(graph, filenameBase + "_svcii_8", 8);
		AvroWriter.write(graph, filenameBase + ".avro");
	}
	
	public static void main(String[] args) {
		try {
			new Main(args).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
