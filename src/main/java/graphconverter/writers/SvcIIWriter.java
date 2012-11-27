package graphconverter.writers;


import java.io.BufferedOutputStream;
import java.io.File;
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
		FileOutputStream fos;
		BufferedOutputStream bos;
		
		// Create directory
		new File(filename).mkdir();

		// Calculate number of vertices per segment
		int[] vertexCount = new int[numSegments];
		for (int i = 0; i < numSegments; i++) {
			vertexCount[i] = vertexCountForSegment(graph, i, numSegments);
		}
		
		// Save vertices keyset (preserves order)
		Integer[] vertices = new Integer[graph.totalVertexCount];
		graph.keySet().toArray(vertices);

		// Convert vertex indices to sequential numbers per segment
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

		// Count edges
		int[][] outEdgeCount = new int[numSegments][numSegments];
		for (int src : vertices) {
			int srcSegment = sourceIndex.get(src).inSegment;
			for (int dest : graph.get(src)) {
				int destSegment = sourceIndex.get(dest).inSegment;
				outEdgeCount[srcSegment][destSegment]++;
			}
		}
		
		// Allow vertices to be garbage-collected
		vertices = null;
		
		// Allocate buffers
		ByteBuffer[][][] bbs = new ByteBuffer[numSegments][numSegments][2];
		IntBuffer[][][] ibs = new IntBuffer[numSegments][numSegments][2];
		for (int i = 0; i < numSegments; i++) {
			for (int j = 0; j < numSegments; j++) {
				bbs[i][j][SRC] = ByteBuffer.allocate(4 * outEdgeCount[i][j]);
				bbs[i][j][SRC].order(ByteOrder.BIG_ENDIAN);
				ibs[i][j][SRC] = bbs[i][j][SRC].asIntBuffer();
				bbs[i][j][DEST] = ByteBuffer.allocate(4 * outEdgeCount[i][j]);
				bbs[i][j][DEST].order(ByteOrder.BIG_ENDIAN);
				ibs[i][j][DEST] = bbs[i][j][DEST].asIntBuffer();
			}
		}
		
		// Write edges to buffers
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
		
		// Header
		fos = new FileOutputStream(filename + File.separator + "info");
		bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
		
		int intsInHeader = 8 + numSegments + numSegments * numSegments;
		
		ByteBuffer bbHeader = ByteBuffer.allocate(intsInHeader * 4);
		bbHeader.order(ByteOrder.BIG_ENDIAN);
		IntBuffer ibHeader = bbHeader.asIntBuffer();
		
		ibHeader.put(31); // Version number
		ibHeader.put(0x00010041); // User info string ('A')
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
	
	private static class vertexInfo {
		int inSegment, indexInSement;
		vertexInfo(int inSegment, int indexInSegment) {
			this.inSegment = inSegment;
			this.indexInSement = indexInSegment;
		}
	}
	
	private static int vertexCountForSegment(Graph graph, int segmentIndex, int segmentCount) {
		float verticesPerSegment = (float) graph.totalVertexCount / segmentCount;
		return (int) (Math.floor((segmentIndex+1) * verticesPerSegment) - Math.floor(segmentIndex * verticesPerSegment));
	}
}
