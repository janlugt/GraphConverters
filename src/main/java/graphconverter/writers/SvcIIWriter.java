package graphconverter.writers;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.Map;

import util.Graph;
import util.Settings;

public class SvcIIWriter {
	
	final static int SRC = 0, DEST = 1;
	
	public static void write(Graph graph, String filename, int numSegments) throws IOException {
		// Create directory
		new File(filename).mkdir();

		int[] vertexCounts = getVertexCountsPerSegment(graph, numSegments);
		Integer[] vertices = getVertexArray(graph);
		Map<Integer, vertexInfo> sourceIndex = sequentializeVertexIds(vertexCounts, vertices);
		int[][] outEdgeCount = getEdgeCounts(graph, numSegments, vertices, sourceIndex);
		vertices = null; // Allow vertices to be garbage-collected
		
		ByteBuffer[][][] buffers = allocateBuffers(numSegments, outEdgeCount);
		writeEdges(graph, sourceIndex, buffers);

		writeInfoFile(filename, numSegments, vertexCounts, outEdgeCount);
		writeEdgeFiles(filename, numSegments, buffers);
	}

	private static int[] getVertexCountsPerSegment(Graph graph, int numSegments) {
		int[] vertexCounts = new int[numSegments];
		for (int i = 0; i < numSegments; i++) {
			float verticesPerSegment = (float) graph.totalVertexCount / numSegments;
			vertexCounts[i] = (int) (Math.floor((numSegments+1) * verticesPerSegment) - Math.floor(numSegments * verticesPerSegment));
		}
		return vertexCounts;
	}

	private static Integer[] getVertexArray(Graph graph) {
		Integer[] vertices = new Integer[graph.totalVertexCount];
		graph.keySet().toArray(vertices);
		return vertices;
	}

	private static Map<Integer, vertexInfo> sequentializeVertexIds(
			int[] vertexCount, Integer[] vertices) {
		Map<Integer, vertexInfo> sourceIndex = new HashMap<Integer, vertexInfo>();
		int currentSegment = 0, vertexIndex = 0;
		for (int src : vertices) {
			sourceIndex.put(src, new vertexInfo(currentSegment, vertexIndex));
			vertexIndex++;
			if (vertexIndex == vertexCount[currentSegment]) {
				currentSegment++;
				vertexIndex = 0;
			}
		}
		return sourceIndex;
	}

	private static class vertexInfo {
		int inSegment, indexInSement;
		vertexInfo(int inSegment, int indexInSegment) {
			this.inSegment = inSegment;
			this.indexInSement = indexInSegment;
		}
	}

	private static int[][] getEdgeCounts(Graph graph, int numSegments,
			Integer[] vertices, Map<Integer, vertexInfo> sourceIndex) {
		int[][] outEdgeCount = new int[numSegments][numSegments];
		for (int src : vertices) {
			int srcSegment = sourceIndex.get(src).inSegment;
			for (int dest : graph.get(src)) {
				int destSegment = sourceIndex.get(dest).inSegment;
				outEdgeCount[srcSegment][destSegment]++;
			}
		}
		return outEdgeCount;
	}

	private static ByteBuffer[][][] allocateBuffers(int numSegments,
			int[][] outEdgeCount) {
		// Allocate buffers
		ByteBuffer[][][] buffers = new ByteBuffer[numSegments][numSegments][2];
		for (int i = 0; i < numSegments; i++) {
			for (int j = 0; j < numSegments; j++) {
				buffers[i][j][SRC] = ByteBuffer.allocate(4 * outEdgeCount[i][j]);
				buffers[i][j][SRC].order(ByteOrder.BIG_ENDIAN);
				buffers[i][j][DEST] = ByteBuffer.allocate(4 * outEdgeCount[i][j]);
				buffers[i][j][DEST].order(ByteOrder.BIG_ENDIAN);
			}
		}
		return buffers;
	}

	private static void writeEdges(Graph graph,
			Map<Integer, vertexInfo> sourceIndex, ByteBuffer[][][] buffers) {

		IntBuffer[][][] ibs = new IntBuffer[buffers.length][buffers.length][2];
		for (int i = 0; i < buffers.length; i++) {
			for (int j = 0; j < buffers[i].length; j++) {
				ibs[i][j][SRC] = buffers[i][j][SRC].asIntBuffer();
				ibs[i][j][DEST] = buffers[i][j][DEST].asIntBuffer();
			}
		}
		
		for (int src : graph.keySet()) {
			int srcSegment = sourceIndex.get(src).inSegment;
			int srcIndex = sourceIndex.get(src).indexInSement;
			for (int dest : graph.get(src)) {
				int destSegment = sourceIndex.get(dest).inSegment;
				int destIndex = sourceIndex.get(dest).indexInSement;
				ibs[srcSegment][destSegment][SRC].put(srcIndex);
				ibs[srcSegment][destSegment][DEST].put(destIndex);
			}
		}
	}

	private static void writeInfoFile(String filename, int numSegments,
			int[] vertexCount, int[][] outEdgeCount)
			throws FileNotFoundException, IOException {
		FileOutputStream fos;
		BufferedOutputStream bos;
		fos = new FileOutputStream(filename + File.separator + "info");
		bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
		
		int intsInHeader = 8 + numSegments + numSegments * numSegments;
		
		ByteBuffer bbHeader = ByteBuffer.allocate(intsInHeader * 4);
		bbHeader.order(ByteOrder.BIG_ENDIAN);
		IntBuffer ibHeader = bbHeader.asIntBuffer();
		
		ibHeader.put(31); // Version number
		ibHeader.put(0x00010041); // User info string (length 1, unicode char 'A')
		ibHeader.put(numSegments); // Number of segments
		ibHeader.put(0); // Root segment
		ibHeader.put(0); // Root offset
		ibHeader.put(0); // Labels
		ibHeader.put(0); // Tau
		ibHeader.put(0); // Dummy
		
		// Number of nodes per segment
		for (int i = 0; i < numSegments; i++) {
			ibHeader.put(vertexCount[i]);
		}
		
		// Number of edges from each segment to each other segment
		for (int i = 0; i < numSegments; i++) {
			for (int j = 0; j < numSegments; j++) {
				ibHeader.put(outEdgeCount[i][j]);
			}
		}
		
		// Write to file
		bos.write(bbHeader.array());
		bos.close();
		fos.close();
	}

	private static void writeEdgeFiles(String filename, int numSegments,
			ByteBuffer[][][] bbs) throws FileNotFoundException, IOException {
		FileOutputStream fos;
		BufferedOutputStream bos;
		// Write buffers to files
		for (int i = 0; i < numSegments; i++) {
			for (int j = 0; j < numSegments; j++) {
				fos = new FileOutputStream(filename + File.separator + "src-" + i + "-" + j);
				bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
				bos.write(bbs[i][j][SRC].array());
				bos.close();
				fos.close();
				
				fos = new FileOutputStream(filename + File.separator + "dest-" + i + "-" + j);
				bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
				bos.write(bbs[i][j][DEST].array());
				bos.close();
				fos.close();
			}
		}
	}
}
