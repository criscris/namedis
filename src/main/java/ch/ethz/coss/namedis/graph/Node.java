package ch.ethz.coss.namedis.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Node implements Serializable
{
	private static final long serialVersionUID = 8741439826129088441L;
	
	public int nodeIndex;
	public List<Edge> edges = new ArrayList<>();
	
	public Node(int nodeIndex)
	{
		this.nodeIndex = nodeIndex;
	}
}
