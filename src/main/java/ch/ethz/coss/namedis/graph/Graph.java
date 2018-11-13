package ch.ethz.coss.namedis.graph;

import java.io.Serializable;
import java.util.List;

public class Graph implements Serializable
{
	private static final long serialVersionUID = -3547528954667572117L;
	
	public List<Node> nodes;
	public List<Edge> edges;
}
