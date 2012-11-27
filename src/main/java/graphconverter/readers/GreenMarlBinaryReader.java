package graphconverter.readers;


import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import util.Graph;

public class GreenMarlBinaryReader {
	public static Graph read(String filename) throws IOException {
		Graph graph = new Graph();
		FileInputStream fis = new FileInputStream(filename);
		FileChannel ch = fis.getChannel();
		MappedByteBuffer bb = ch.map(MapMode.READ_ONLY, 0, ch.size());
		bb.order(ByteOrder.BIG_ENDIAN);
		IntBuffer ib = bb.asIntBuffer();
		int magic = ib.get();
		if (magic != 0x03939999) {
			System.err.println("Error: magic number does not match.");
			System.exit(-1);
		}
		@SuppressWarnings("unused")
		int nodeIdentifierSize = ib.get();
		@SuppressWarnings("unused")
		int edgeIdentifierSize = ib.get();
		graph.totalVertexCount = ib.get();
		graph.totalEdgeCount = ib.get();

		System.err.println("NumNodes (from header): " + graph.totalVertexCount);
		System.err.println("NumEdges (from header): " + graph.totalEdgeCount);

		// Nodes
		int[] indices = new int[graph.totalVertexCount + 1];
		for (int i = 0; i < graph.totalVertexCount + 1; i++) {
			indices[i] = ib.get();
		}

		// Edges
		for (int i = 0; i < graph.totalVertexCount; i++) {
			graph.addVertex(i);
			for (int j = indices[i]; j < indices[i + 1]; j++) {
				graph.addEdge(i, ib.get());
			}
		}

		fis.close();
		return graph;
	}
}
