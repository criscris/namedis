package ch.ethz.coss.namedis;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import ch.ethz.coss.namedis.disam.Disambiguation;
import ch.ethz.coss.namedis.disam.DisambiguationParams;
import ch.ethz.coss.namedis.disam.LinkWeight;
import ch.ethz.coss.namedis.disam.PubProp;
import ch.ethz.coss.namedis.disam.PublicationSimilarity;
import xyz.koral.IO;
import xyz.koral.compute.Tuple2;

public class NameDis_Step3_Disam 
{
	static final DisambiguationParams params = new DisambiguationParams(
			0.75f, // "s"
			0.54f, // "a"
			0.19f, // "r"
			1.02f, //  "c"
			0f, 
			
			0.011f, // "b3"
			0.19f, //  "b2"
			0.49f); // "b4"
	static final boolean outputSize1Clusters = false;

	public static void main(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.out.println("USAGE: ambiguousNamesFile clustersOutFile");
			return;
		}
		File amgigousNamesFile = new File(args[0]); 
		File outFile = new File(args[1]);
		System.out.println("Writing to " + outFile.getAbsolutePath());
		System.out.println(IO.jvmInfo());
		
		long time = System.currentTimeMillis();
		AtomicLong count = new AtomicLong();	
		List<DisambiguatedName> clusteredNames = 
		IO.readJSONStreamParallel(IO.istream(amgigousNamesFile), AmbiguousName.class)
		.filter(name -> name.name != null)
		.map(namedisComp)
		.peek(name -> 
		{
			long c = count.incrementAndGet();
			if (c % 50000 == 0) System.out.println(c + " " + (System.currentTimeMillis() - time) + " ms.");
		})
		.filter(name -> name != null)
		.collect(Collectors.toList());
		System.out.println(count + " " + (System.currentTimeMillis() - time) + " ms.");
		
		System.out.println(clusteredNames.size() + " names with " + clusteredNames.stream().mapToLong(n -> n.clusters.length).sum() + " clusters.");	
		IO.writeJSONStream(clusteredNames.stream().sorted((name1, name2) -> name1.name.compareTo(name2.name)), IO.ostream(outFile));

		System.out.println(IO.jvmInfo());
		System.out.println((System.currentTimeMillis() - time) + " ms total exec time.");
	}
	
	
	public static Function<AmbiguousName, Tuple2<String, List<LinkWeight>>> edgeComputation = name -> 
	{
		List<LinkWeight> weights = new ArrayList<>();
		for (int i=0; i<name.pubs.size(); i++)
		{
			PubProp pubi = name.pubs.get(i);
			
			for (int j=i+1; j<name.pubs.size(); j++)
			{
				PubProp pubj = name.pubs.get(j);
				LinkWeight weight = PublicationSimilarity.computeWeight(pubi, pubj);
				if (weight != null)
				{
					weights.add(weight);
				}
			}
		}
		return new Tuple2<String, List<LinkWeight>>(name.name, weights);
	};
	
	public static BiFunction<Tuple2<String, List<LinkWeight>>, DisambiguationParams, DisambiguatedName> clusterComputation = (tuple, params) ->
	{
		Disambiguation dis = new Disambiguation(tuple._2);
		List<List<Integer>> clusters = dis.exec(params);
		int minClusterSize = outputSize1Clusters ? 1 : 2;
		clusters = clusters.stream().filter(c -> c.size() >= minClusterSize).collect(Collectors.toList());
		if (clusters.size() == 0) return null;
		
		Collections.sort(clusters, (c1, c2) -> c2.size() - c1.size());
		DisambiguatedName name = new DisambiguatedName();
		name.name = tuple._1;
		name.clusters = new int[clusters.size()][];
		for (int i=0; i<clusters.size(); i++)
		{
			List<Integer> cluster = clusters.get(i);
			name.clusters[i] = new int[cluster.size()];
			for (int j=0; j<cluster.size(); j++) name.clusters[i][j] = cluster.get(j);
		}
		return name;
	};
	
	public static Function<AmbiguousName, DisambiguatedName> namedisComp = aname ->
		clusterComputation.apply(edgeComputation.apply(aname), params);
		
	public static BiFunction<AmbiguousName, DisambiguationParams, DisambiguatedName> namedisCompP = (aname, params) ->
		clusterComputation.apply(edgeComputation.apply(aname), params);
}


