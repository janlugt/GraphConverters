package graphconverter.readers;

import graphconverter.util.Graph;
import graphconverter.util.Settings;
import graphconverter.util.TextFunctions;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;


public class MultiThreadedEdgeListReader {
	
	private final static String marker = new String();

	public static Graph read(String filename) throws IOException {
		// Input format: [source vertex id] [destination vertex id]\n

		// Create scanner for input
		FileInputStream fileInput = new FileInputStream(filename);
		BufferedInputStream bufferedInput = new BufferedInputStream(fileInput,
				Settings.BUFFERED_STREAM_SIZE);
		Scanner lineScanner = new Scanner(bufferedInput);
		lineScanner.useDelimiter(Pattern.compile(Settings.NEWLINE));

		// Create graph
		Graph graph = new Graph();

		// Set up queue
		BlockingQueue<String> q = new ArrayBlockingQueue<String>(100);
		
		// Producer
		Producer p = new Producer(q, lineScanner);
		Thread tp = new Thread(p);
		tp.start();
		
		// Consumer
		Consumer c = new Consumer(q, graph);
		Thread tc = new Thread(c);
		tc.start();

		// Wait for the consumer to finish
		try {
			tc.join();
		} catch (InterruptedException e) {
		}
		return graph;
	}

	static class Producer implements Runnable {
		private final BlockingQueue<String> queue;
		private final Scanner lineScanner;

		Producer(BlockingQueue<String> q, Scanner s) {
			queue = q;
			lineScanner = s;
		}

		public void run() {
			try {
				while (lineScanner.hasNext()) {
					queue.put(lineScanner.next());
				}
				queue.put(marker);
			} catch (InterruptedException ex) {
			}
		}
	}

	static class Consumer implements Runnable {
		private final BlockingQueue<String> queue;
		private final Graph graph;

		Consumer(BlockingQueue<String> q, Graph g) {
			queue = q;
			graph = g;
		}

		public void run() {
			try {
				while (true) {
					String line = queue.take();
					if (line == marker) {
						return;
					} else {
						consume(line);
					}
				}
			} catch (InterruptedException ex) {
			}
		}

		void consume(String line) {
			if (TextFunctions.isComment(line)) {
				return;
			}
			String[] vertices = TextFunctions.splitLine(line);
			Integer source = Integer.parseInt(vertices[0]);
			Integer dest = Integer.parseInt(vertices[1]);
			graph.addVertex(source);
			graph.addVertex(dest);
			graph.addEdge(source, dest);
		}
	}
}
