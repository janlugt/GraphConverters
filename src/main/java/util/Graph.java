package util;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

public class Graph extends TreeMap<Integer, List<Integer>> {

	private static final long serialVersionUID = -6578407579009331030L;

	public int totalVertexCount = 0;
	public int totalEdgeCount = 0;
	
	public void addEdge(Integer source, Integer dest) {
		this.get(source).add(dest);
		totalEdgeCount += 1;
	}

	public void addVertex(Integer vertex) {
		if (!this.containsKey(vertex)) {
			this.put(vertex, new LinkedList<Integer>());
			totalVertexCount += 1;
		}
	}
	
}
