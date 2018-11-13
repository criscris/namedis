package ch.ethz.coss.namedis;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import ch.ethz.coss.namedis.disam.DisambiguationParams;
import ch.ethz.coss.namedis.disam.PubProp;
import xyz.koral.IO;
import xyz.koral.compute.Tuple2;
import xyz.koral.table.Table;

public class NameDis_Step2b_Params 
{
	public static void main(String[] args) 
	{
		System.out.println("NameDis_Step2b_Params 20180429 6:03");
		if (args.length != 4)
		{
			System.out.println("USAGE: recallFile precisionFile namesFile outFile");
			return;
		}
		System.out.println(Arrays.toString(args));
		long time = System.currentTimeMillis();
		
		File recallFile = new File(args[0]);
		Map<String, int[][]> recallClusters = IO.readJSONStream(IO.istream(recallFile), DisambiguatedName.class).collect(Collectors.toMap(d -> d.name, d -> d.clusters));
		System.out.println(recallClusters.size() + " names with recall info");
		
		File precisionFile = new File(args[1]);
		Table namePairs = Table.csvToText(IO.readCSV(IO.istream(precisionFile)));
		System.out.println(namePairs.nrows() + " name pairs for precision.");
		
		File namesFile = new File(args[2]);
		List<AmbiguousName> names = IO.readJSONStream(IO.istream(namesFile), AmbiguousName.class)
				.filter(name -> recallClusters.containsKey(name.name))
				.collect(Collectors.toList());
		
		System.out.println((System.currentTimeMillis() - time) + " ms for loading data.");
		System.out.println(IO.jvmInfo());
		
		int bins = 100;
		DisambiguationParams params = NameDis_Step3_Disam.params;
		
		long t1 = System.currentTimeMillis();
		List<DisambiguatedName> disNames = names.stream().map(name -> NameDis_Step3_Disam.namedisCompP.apply(name, params)).collect(Collectors.toList());
		Table recall = recall(params, bins, disNames, recallClusters);
		System.out.println((System.currentTimeMillis() - t1) + " ms for recall.");
		
		t1 = System.currentTimeMillis();
		Table precision = precision(params, bins, namePairs, names);
		System.out.println((System.currentTimeMillis() - t1) + " ms for precision.");
		
		IO.writeCSV(Table.colBind(recall, precision).toCSV(), IO.ostream(new File(args[3])));

		System.out.println((System.currentTimeMillis() - time) + " ms total exec time.");
	}
	
	
	public static Table recall(DisambiguationParams params, int bins, List<DisambiguatedName> names, Map<String, int[][]> recallClusters)
	{
		int binSize = names.size() / bins;
		return Table.numeric(IntStream.range(0, bins).mapToDouble(b -> {
			double fracSum = 0;
			int n = 0;
			for (int j=0; j<binSize; j++)
			{
				DisambiguatedName name = names.get(b*binSize + j);
				if (name == null) continue;
				Tuple2<Double, Integer> result = recall(name, recallClusters);
				if (result != null)
				{
					fracSum += result._1;
					n += result._2;
				}
			}
			return n <= 2 ? -1 : fracSum / n;
		}))
		.setColNames_m("recall");
	}
	
	static Tuple2<Double, Integer> recall(DisambiguatedName name, Map<String, int[][]> recallClusters)
	{
		int[][] expected = recallClusters.get(name.name);
		if (expected == null) return null;
		
		List<Set<Integer>> actual = new ArrayList<>();
		for (int[] c : name.clusters) actual.add(IntStream.of(c).boxed().collect(Collectors.toSet()));
		
		double fracSum = 0;
		int n = 0;
		for (int[] e : expected)
		{
			int maxRecall = 0;
			for (Set<Integer> a : actual)
			{
				int recall = 0;
				for (int e_ : e) if (a.contains(e_)) recall++;
				maxRecall = Math.max(recall, maxRecall);
			}
			double maxRecallFrac = (double) maxRecall / e.length;
			fracSum += maxRecallFrac;
			n++;
		}
		return n == 0 ? null : new Tuple2<>(fracSum, n);
	}
	
	public static Table precision(DisambiguationParams params, int bins, Table namePairs, List<AmbiguousName> names)
	{
		Map<String, AmbiguousName> names_ = names.stream().collect(Collectors.toMap(name -> name.name, name -> name));
		int binSize = namePairs.nrowsI() / bins;
		return Table.numeric(IntStream.range(0, bins).mapToDouble(b -> 
		{
			double fracSum = 0;
			int n = 0;
			for (int j=0; j<binSize; j++)
			{
				Tuple2<Double, Integer> result = precision(params, namePairs, b*binSize + j, names_);
				if (result != null)
				{
					fracSum += result._1;
					n += result._2;
				}
			}
			return n <= 2 ? -1 : fracSum / n;
		}))
		.setColNames_m("precision");
	}
	
	static Tuple2<Double, Integer> precision(DisambiguationParams params, Table namePairs, int i, Map<String, AmbiguousName> names)
	{
		AmbiguousName name1 = names.get(namePairs.getS(i, 0));
		if (name1 == null) return null;
		
		AmbiguousName name2 = names.get(namePairs.getS(i, 1));
		if (name2 == null) return null;
		
		Set<Integer> name1Pubs = new HashSet<>();
		for (PubProp pp : name1.pubs) name1Pubs.add(pp.id);
		
		AmbiguousName name = new AmbiguousName();
		name.name = "artificial";
		name.pubs = new ArrayList<>(name1.pubs);
		name.pubs.addAll(name2.pubs);
		DisambiguatedName disName = NameDis_Step3_Disam.namedisCompP.apply(name, params);
		if (disName == null) return null;
		
		double fracSum = 0;
		int n = 0;
		for (int[] cluster : disName.clusters)
		{
			int pubCount = 0;
			for (int pub : cluster) if (name1Pubs.contains(pub)) pubCount++;
			
			if (pubCount <= cluster.length/2)
			{
				// it's a name2 cluster
				pubCount = cluster.length - pubCount;
			}
			
			fracSum += (double) pubCount / cluster.length;
			n++;
		}
		
		return n == 0 ? null : new Tuple2<>(fracSum, n);
	}
	
	/**
	 * wos
	 * 1909486 names with recall info
	 * 58739 names could not be disambiguated.
Computed 11855635 recalls in 100936 ms.
ms recall=0.9211356937980255
3525261 ms total exec time.

	ms
	98235 names with recall info
	127 names could not be disambiguated.
Computed 450968 recalls in 11609 ms.
ms recall=0.9622719978845906
4471360 ms total exec time.
	 * @return

	public static double recall(File recallClustersFile, File ambiguousNamesFile)
	{

		
		long time = System.currentTimeMillis();
		AtomicLong count = new AtomicLong();
		AtomicLong errorCount = new AtomicLong();
		List<DisambiguatedName> clusteredNames = 
		IO.readJSONStreamParallel(IO.istream(ambiguousNamesFile), AmbiguousName.class)
		.filter(name -> name.name != null && recallClusters.containsKey(name.name))
		.map(NameDis_Step3_Disam.namedisComp)
		.peek(name -> 
		{
			long c = count.incrementAndGet();
			if (c % 50000 == 0) System.out.println(c + " " + (System.currentTimeMillis() - time) + " ms.");
			if (name == null) errorCount.incrementAndGet();
		})
		.filter(name -> name != null)
		.collect(Collectors.toList());
		System.out.println(errorCount.get() + " names could not be disambiguated.");
		
		return recall(clusteredNames, recallClusters);
	}
	
	public static double recall(List<DisambiguatedName> clusteredNames, Map<String, int[][]> recallClusters)
	{
		long time = System.currentTimeMillis();
		double fracSum = 0;
		int n = 0;
		for (DisambiguatedName name : clusteredNames)
		{
			int[][] expected = recallClusters.get(name.name);
			if (expected == null) continue;
			
			List<Set<Integer>> actual = new ArrayList<>();
			for (int[] c : name.clusters) actual.add(IntStream.of(c).boxed().collect(Collectors.toSet()));
			
			for (int[] e : expected)
			{
				int maxRecall = 0;
				for (Set<Integer> a : actual)
				{
					int recall = 0;
					for (int e_ : e) if (a.contains(e_)) recall++;
					maxRecall = Math.max(recall, maxRecall);
				}
				double maxRecallFrac = (double) maxRecall / e.length;
				fracSum += maxRecallFrac;
				n++;
			}
		}
		System.out.println("Computed " + n + " recalls in " + (System.currentTimeMillis() - time) + " ms.");
		
		return n == 0 ? 0 : fracSum / n;
	}
	
		 */
}
