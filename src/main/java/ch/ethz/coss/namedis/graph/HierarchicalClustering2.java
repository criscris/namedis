package ch.ethz.coss.namedis.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HierarchicalClustering2 
{
	public static long debugTime1 = 0;
	public static long debugTime2 = 0;
	public static long debugTime3 = 0;
	public static long debugTime4 = 0;
	
	public List<List<Node>> refine(List<List<Node>> clusters, int nodeCount, List<Edge> edges, 
			float weightThresholdForMergingClusters, float singleEdgeThreshold, float clusterSize1MergeThreshold)
	{
		// a little bit of pre-computation
		long t1 = System.currentTimeMillis();
		
		int[] nodeToClusterIndex = new int[nodeCount];
		int[] clusterSizes = new int[clusters.size()];
		for (int i=0; i<nodeToClusterIndex.length; i++) nodeToClusterIndex[i] = -1;
		for (int i=0; i<clusters.size(); i++)
		{
			List<Node> cluster = clusters.get(i);
			clusterSizes[i] = cluster.size();
			for (Node node : cluster)
			{
				nodeToClusterIndex[node.nodeIndex] = i;
			}
		}
		
		// determine similarity between clusters
		long t2 = System.currentTimeMillis();
		Map<String, Float> clusterToClusterWeightMap = new HashMap<>();
		for (Edge edge : edges)
		{
			if (edge.weight < singleEdgeThreshold) continue;
			
			int fromCluster = nodeToClusterIndex[edge.node1.nodeIndex];
			int toCluster = nodeToClusterIndex[edge.node2.nodeIndex];
			if (fromCluster < 0 || toCluster < 0 || fromCluster == toCluster) continue;
			
			
			// edge is between different clusters
			String edgeName = Math.min(fromCluster, toCluster) + "_" + Math.max(fromCluster, toCluster);
			Float oldValue = clusterToClusterWeightMap.get(edgeName);
			if (oldValue == null) oldValue = 0f;
			clusterToClusterWeightMap.put(edgeName, oldValue + edge.weight);
		}
		
		
		List<Node> clusterNodes = new ArrayList<>();
		List<Edge> clusterEdges = new ArrayList<>();
		for (int i=0; i<clusters.size(); i++) clusterNodes.add(new Node(i));
		
		for (Entry<String, Float> entry : clusterToClusterWeightMap.entrySet())
		{
			float weight = entry.getValue();
			String clusterToClusterName = entry.getKey();
			int i1 = clusterToClusterName.indexOf("_");
			int fromCluster = Integer.parseInt(clusterToClusterName.substring(0, i1));
			int toCluster = Integer.parseInt(clusterToClusterName.substring(i1 + 1));
			
			// normalize weight by cluster sizes
			weight /= clusterSizes[fromCluster] * clusterSizes[toCluster];
			
			boolean merge = weight > weightThresholdForMergingClusters;
			if (weight >= clusterSize1MergeThreshold && Math.min(clusters.get(fromCluster).size(), clusters.get(toCluster).size()) <= 1) merge = true;
			
			if (merge)
			{
				Node nodei = clusterNodes.get(fromCluster);
				Node nodej = clusterNodes.get(toCluster);
				
				Edge edge = new Edge(nodei, nodej, weight);
				nodei.edges.add(edge);
				nodej.edges.add(edge);
				clusterEdges.add(edge);
			}
		}
		
		// cluster the clusters
		long t3 = System.currentTimeMillis();
		
		ClusterByEdgeWeight clusterByEdgeWeight = new ClusterByEdgeWeight(0, clusterNodes, clusterEdges);
		List<List<Node>> clustersOfClusters = clusterByEdgeWeight.exec();
		
		
		
		// put together nodes of clustered clusters
		long t4 = System.currentTimeMillis();
		List<List<Node>> mergedClusters = new ArrayList<>();
		
		for (List<Node> clusterOfClusters : clustersOfClusters)
		{
			List<Node> mergedCluster = new ArrayList<>();
			mergedClusters.add(mergedCluster);
			
			// each node here is itself a cluster
			for (Node node : clusterOfClusters)
			{
				List<Node> originalCluster = clusters.get(node.nodeIndex);
				mergedCluster.addAll(originalCluster);
			}
		}
		long t5 = System.currentTimeMillis();
		
		debugTime1 += t2 - t1;
		debugTime2 += t3 - t2;
		debugTime3 += t4 - t3;
		debugTime4 += t5 - t4;
		
		return mergedClusters;
	}
}
