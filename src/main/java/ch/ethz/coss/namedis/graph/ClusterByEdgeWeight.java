package ch.ethz.coss.namedis.graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ClusterByEdgeWeight 
{
	public static void main(String[] args) throws Exception
	{

	}

	float weightThreshold;
	List<Node> nodes;
	List<Edge> edges;
	
	
	public ClusterByEdgeWeight(float weightThreshold, List<Node> nodes, List<Edge> edges)
	{
		this.weightThreshold = weightThreshold;
		this.nodes = nodes;
		this.edges = edges;
	}
	
	public List<List<Node>> exec()
	{
		// start with n clusters (where n is the number of nodes)
		ArrayList<List<Node>> clusters = new ArrayList<>();
		for (int i=0; i<nodes.size(); i++)
		{
			ArrayList<Node> cluster = new ArrayList<>();
			cluster.add(nodes.get(i));
			clusters.add(cluster);
		}
		
		// merge clusters which share an edge with >= weightThreshold
		for (Edge edge : edges)
		{
			if (edge.weight < weightThreshold) continue;
			
			List<Node> firstCluster = clusters.get(edge.node1.nodeIndex);
			List<Node> secondCluster = clusters.get(edge.node2.nodeIndex);
			
			if (firstCluster == secondCluster) continue; // already merged
			
			// merge two clusters by extending the first, and replacing all occurences of the second one with the first one
			firstCluster.addAll(secondCluster);
			
			for (Node node : secondCluster)
			{
				clusters.set(node.nodeIndex, firstCluster);
			}
			
			// secondCluster no longer has a reference, i.e. disappears from the list of clusters
		}
			
		return new ArrayList<>(new HashSet<>(clusters)); // make clusters only occur once
	}
}
