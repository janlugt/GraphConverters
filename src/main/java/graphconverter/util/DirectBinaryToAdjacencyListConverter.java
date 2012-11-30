package graphconverter.util;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class DirectBinaryToAdjacencyListConverter {

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("Please provide a path to a binary file as a parameter.");
			return;
		}
		String filenameBase = args[0].substring(0, args[0].length() - 4);

		// Output
		FileOutputStream fos = new FileOutputStream(filenameBase + ".adj");
		BufferedOutputStream bos = new BufferedOutputStream(fos, Settings.BUFFERED_STREAM_SIZE);
		PrintStream out = new PrintStream(bos);

		// Input
		FileInputStream fis = new FileInputStream(filenameBase + ".bin");
		FileChannel ch = fis.getChannel();
		MappedByteBuffer bb = ch.map(MapMode.READ_ONLY, 0, ch.size());
		bb.order(ByteOrder.BIG_ENDIAN);
		IntBuffer ib = bb.asIntBuffer();
		if (ib.get() != Settings.GREEN_MARL_MAGIC_NUMBER) {
			System.err.println("Error: magic number does not match.");
			System.exit(-1);
		}
		if(ib.get() != Settings.GREEN_MARL_NODE_IDENTIFIER_SIZE) {
			System.err.println("Invalid node identifier size.");
			System.exit(-1);
		}
		if(ib.get() != Settings.GREEN_MARL_EDGE_IDENTIFIER_SIZE) {
			System.err.println("Invalid node identifier size.");
			System.exit(-1);
		}
		int numNodes = ib.get();
		int numEdges = ib.get();

		// Print output
		System.err.println("NumNodes: " + numNodes);
		System.err.println("NumEdges: " + numEdges);

		// Nodes
		int[] indices = new int[numNodes + 1];
		for (int i = 0; i < numNodes + 1; i++) {
			indices[i] = ib.get();
		}

		// Edges
		for (int i = 0; i < numNodes; i++) {
			out.print(i);
			out.print('\t');
			out.print(Settings.DUMMY_NODE_VALUE);
			for (int j = indices[i]; j < indices[i + 1]; j++) {
				out.print('\t');
				out.print(ib.get());
				out.print('\t');
				out.print(Settings.DUMMY_EDGE_VALUE);
			}
			out.println();
		}

		out.close();
		bos.close();
		fos.close();
		fis.close();
	}
}
