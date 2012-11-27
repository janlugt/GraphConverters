package graphconverter.writers;


import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.Map;

import util.Graph;
import util.Settings;

public class GreenMarlBinaryWriter {
	
	private static final int DUMMY_NODE = 1;
	
	public static void write(Graph graph, String filename) throws IOException {
		FileOutputStream fos = new FileOutputStream(filename);
		BufferedOutputStream bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
		
		// magic number, node identifier size, edge identifier, number of nodes, number of edges
		// node array
		// final dummy node
		// edge array
		long numInts = 5 + graph.totalVertexCount + DUMMY_NODE + graph.totalEdgeCount;
		
		long bytesToBeAllocated = numInts * 4;
		int bufferSize = 1024 * 1024 * 1024;
		int numBuffers = (int) Math.ceil(((double) bytesToBeAllocated) / bufferSize);
		ByteBuffer[] bbs = new ByteBuffer[numBuffers];
		IntBuffer[] ibs = new IntBuffer[numBuffers];
		for (int i = 0; i < numBuffers; i++) {
			bbs[i] = ByteBuffer.allocate((int) Math.min(bytesToBeAllocated, bufferSize));
			bytesToBeAllocated -= (int) Math.min(bytesToBeAllocated, bufferSize);
			bbs[i].order(ByteOrder.BIG_ENDIAN);
			ibs[i] = bbs[i].asIntBuffer();
		}
		int current = 0;

		ibs[current].put(0x03939999); // Magic number
		ibs[current].put(4); // Node identifier size
		ibs[current].put(4); // Edge identifier size
		ibs[current].put(graph.totalVertexCount);
		ibs[current].put(graph.totalEdgeCount);

		// Save keyset
		Integer[] keys = new Integer[graph.keySet().size()];
		graph.keySet().toArray(keys);

		// Convert edge destinations to sequential numbers
		Map<Integer, Integer> sourceIndex = new HashMap<Integer, Integer>();
		for (int i = 0; i < keys.length; i++) {
			sourceIndex.put(keys[i], i);
		}

		current = serializeVertices(graph, ibs, current, keys);
		current = serializeEdges(graph, ibs, current, keys, sourceIndex);

		// Write to file
		for (ByteBuffer bb : bbs) {
			bos.write(bb.array());
		}
		bos.close();
		fos.close();
	}
	
	private static int serializeVertices(Graph graph, IntBuffer[] ibs, int current, Integer[] keys) {
		int count = 0;
		for (Integer source : keys) {
			ibs[current].put(count);
			if (!ibs[current].hasRemaining()) {
				current++;
			}
			count += graph.get(source).size();
		}
		ibs[current].put(count); // final dummy element
		if (!ibs[current].hasRemaining()) {
			current++;
		}
		return current;
	}
	
	private static int serializeEdges(Graph graph, IntBuffer[] ibs, int current, Integer[] keys, Map<Integer, Integer> sourceIndex) {
		for (Integer source : keys) {
			for (Integer dest : graph.get(source)) {
				ibs[current].put(sourceIndex.get(dest));
				if (!ibs[current].hasRemaining()) {
					current++;
				}
			}
		}
		return current;
	}
}
