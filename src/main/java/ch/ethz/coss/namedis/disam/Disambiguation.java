package ch.ethz.coss.namedis.disam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.ethz.coss.namedis.disam.DisambiguationParams.ParamName;
import ch.ethz.coss.namedis.graph.ClusterByEdgeWeight;
import ch.ethz.coss.namedis.graph.Edge;
import ch.ethz.coss.namedis.graph.Graph;
import ch.ethz.coss.namedis.graph.HierarchicalClustering2;
import ch.ethz.coss.namedis.graph.IWeight;
import ch.ethz.coss.namedis.graph.Node;



public class Disambiguation 
{
	public Graph g;
	Map<Integer, Integer> pubIDtoIndex = new HashMap<>();
	public List<Integer> pubIDs;
	DisambiguationParams params;
	
	class WeightComputation implements IWeight
	{
		private static final long serialVersionUID = 1L;
		
		LinkWeight weight;
		
		public WeightComputation(LinkWeight weight)
		{
			this.weight = weight;
		}

		public float computeWeight() 
		{
			return  params.get(ParamName.SelfCitation) * weight.selfCitation + 
					params.get(ParamName.CoAuthors) * weight.coAuthors + 
					params.get(ParamName.References) * weight.references + 
					params.get(ParamName.Citations) * weight.citations + 
					params.get(ParamName.Affiliations) * weight.affiliations;
		}

		public String getStringRepresentation() 
		{
			return null;
		}
	}
	
	public Disambiguation(List<LinkWeight> linkWeights)
	{
		g = new Graph();
		g.nodes = new ArrayList<>();
		pubIDs = new ArrayList<>();
		
		g.edges = new ArrayList<>(linkWeights.size());
		
		for (LinkWeight link : linkWeights)
		{
			Node node1 = getOrCreateNode(link.pubID1);
			Node node2 = getOrCreateNode(link.pubID2);
			
			Edge edge = new Edge(node1, node2, new WeightComputation(link));
			g.edges.add(edge);
		}
	}
	
	private Node getOrCreateNode(int pubID)
	{
		Integer index = pubIDtoIndex.get(pubID);
		if (index == null)
		{
			index = pubIDs.size();
			Node node = new Node(index);
			
			pubIDs.add(pubID);
			pubIDtoIndex.put(pubID, index);
			g.nodes.add(node);
			return node;
		}
		
		return g.nodes.get(index);
	}
	
	public List<List<Integer>> exec(DisambiguationParams params) 
	{
		// apply parameters
		this.params = params;
//		float namePrior = NamePrior.getPrior(g.nodes.size());
		for (Edge edge : g.edges)
		{
			edge.weight = edge.weightData.computeWeight();
			
			// include name prior
//			edge.weight *= namePrior;
		}
		
		// clustering
		ClusterByEdgeWeight clusterByEdgeWeight = new ClusterByEdgeWeight(1f, g.nodes, g.edges);
		List<List<Node>> clustersList0 = clusterByEdgeWeight.exec();
		
		// merging
		List<List<Node>> clustersList = clustersList0;
		if (params.get(ParamName.ClusterMergeSum) > 0f) // otherwise, don't use cluster merging
		{
			HierarchicalClustering2 hierarchicalClustering = new HierarchicalClustering2();
			clustersList = hierarchicalClustering.refine(clustersList0, g.nodes.size(), g.edges, 
					params.get(ParamName.ClusterMergeSum),
					params.get(ParamName.ClusterMergeLink),
					params.get(ParamName.ClusterSize1));
		}
		
		// return pub ids
		List<List<Integer>> pubClusters = new ArrayList<>(clustersList.size());
		for (List<Node> cluster : clustersList)
		{
			List<Integer> clu = new ArrayList<>(cluster.size());
			for (Node node : cluster)
			{
				clu.add(pubIDs.get(node.nodeIndex));
			}
			pubClusters.add(clu);
		}
		return pubClusters;
	}
}
