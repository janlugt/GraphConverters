package graphconverter.writers;

import graphconverter.util.Graph;
import graphconverter.util.Settings;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.Map;


public class GreenMarlBinaryWriter {
	
	private static final int DUMMY_NODE = 1;
	
	public static void write(Graph graph, String filename) throws IOException {
		ByteBuffer[] buffers = allocateBuffers(totalIntCount(graph));
		IntBuffer[] intbuffers = new IntBuffer[buffers.length];
		for (int i = 0; i < buffers.length; i++) {
			intbuffers[i] = buffers[i].asIntBuffer();
		}

		Integer[] keys = getVertexArray(graph);
		Map<Integer, Integer> sourceIndex = sequentializeVertexIds(keys);

		int current = 0;
		current = writeHeader(graph, intbuffers, current);
		current = writeVertices(graph, intbuffers, current, keys);
		current = writeEdges(graph, intbuffers, current, keys, sourceIndex);

		writeToFile(filename, buffers);
	}

	private static long totalIntCount(Graph graph) {
		// magic number, node identifier size, edge identifier, number of nodes, number of edges
		// node array
		// final dummy node
		// edge array
		long numInts = 5 + graph.totalVertexCount + DUMMY_NODE + graph.totalEdgeCount;
		return numInts;
	}

	private static ByteBuffer[] allocateBuffers(long numInts) {
		long bytesToBeAllocated = numInts * 4;
		int bufferSize = 1024 * 1024 * 1024;
		int numBuffers = (int) Math.ceil(((double) bytesToBeAllocated) / bufferSize);
		ByteBuffer[] bbs = new ByteBuffer[numBuffers];
		for (int i = 0; i < numBuffers; i++) {
			bbs[i] = ByteBuffer.allocate((int) Math.min(bytesToBeAllocated, bufferSize));
			bytesToBeAllocated -= (int) Math.min(bytesToBeAllocated, bufferSize);
			bbs[i].order(ByteOrder.BIG_ENDIAN);
		}
		return bbs;
	}

	private static Integer[] getVertexArray(Graph graph) {
		Integer[] keys = new Integer[graph.keySet().size()];
		graph.keySet().toArray(keys);
		return keys;
	}

	private static Map<Integer, Integer> sequentializeVertexIds(Integer[] keys) {
		Map<Integer, Integer> sourceIndex = new HashMap<Integer, Integer>();
		for (int i = 0; i < keys.length; i++) {
			sourceIndex.put(keys[i], i);
		}
		return sourceIndex;
	}

	private static int writeHeader(Graph graph, IntBuffer[] buffers, int current) {
		IntBuffer ib = buffers[current];
		ib.put(Settings.GREEN_MARL_MAGIC_NUMBER); // Magic number
		ib.put(Settings.GREEN_MARL_NODE_IDENTIFIER_SIZE); // Node identifier size
		ib.put(Settings.GREEN_MARL_EDGE_IDENTIFIER_SIZE); // Edge identifier size
		ib.put(graph.totalVertexCount);
		ib.put(graph.totalEdgeCount);
		return current;
	}
	
	private static int writeVertices(Graph graph, IntBuffer[] buffers, int current, Integer[] keys) {
		IntBuffer ib = buffers[current];
		int count = 0;
		for (Integer source : keys) {
			ib.put(count);
			if (!ib.hasRemaining() && current + 1 < buffers.length) {
				current++;
				ib = buffers[current];
			}
			count += graph.get(source).size();
		}
		ib.put(count); // final dummy element
		if (!ib.hasRemaining() && current + 1 < buffers.length) {
			current++;
			ib = buffers[current];
		}
		return current;
	}
	
	private static int writeEdges(Graph graph, IntBuffer[] buffers, int current, Integer[] keys, Map<Integer, Integer> sourceIndex) {
		IntBuffer ib = buffers[current];
		for (Integer source : keys) {
			for (Integer dest : graph.get(source)) {
				ib.put(sourceIndex.get(dest));
				if (!ib.hasRemaining() && current + 1 < buffers.length) {
					current++;
					ib = buffers[current];
				}
			}
		}
		return current;
	}

	private static void writeToFile(String filename, ByteBuffer[] buffers)
			throws FileNotFoundException, IOException {
		FileOutputStream fos = new FileOutputStream(filename);
		BufferedOutputStream bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
		for (ByteBuffer bb : buffers) {
			bos.write(bb.array());
		}
		bos.close();
		fos.close();
	}
}
