package util;

import java.util.HashSet;
import java.util.Set;

public class GraphChecker {
	public static void check(Graph graph) {
		// check whether graph is multigraph
		Set<Integer> set = new HashSet<Integer>();
		outer: for (Integer source : graph.keySet()) {
			set.clear();
			for (Integer dest : graph.get(source)) {
				if (!set.add(dest)) {
					System.err.println("Warning: graph is a multigraph.");
					break outer;
				}
			}
		}
		// check whether graph contains self-edges
		outer: for (Integer source : graph.keySet()) {
			for (Integer dest : graph.get(source)) {
				if (source.equals(dest)) {
					System.err.println("Warning: graph contains self-edges.");
					break outer;
				}
			}
		}
	}
}
