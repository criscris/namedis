package ch.ethz.coss.namedis.graph;

import java.io.Serializable;

public class Edge implements Serializable
{
	private static final long serialVersionUID = 3568394358735405429L;
	
	public Node node1;
	public Node node2;
	
	public float weight;
	public IWeight weightData;
	
	public Edge(Node node1, Node node2, IWeight weightData) 
	{
		this.node1 = node1;
		this.node2 = node2;
		this.weightData = weightData;
	}
	
	public Edge(Node node1, Node node2, float weight) 
	{
		this.node1 = node1;
		this.node2 = node2;
		this.weight = weight;
	}	
}
