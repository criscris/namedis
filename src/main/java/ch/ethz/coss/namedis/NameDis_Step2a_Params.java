package ch.ethz.coss.namedis;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import ch.ethz.coss.namedis.disam.PubProp;
import xyz.koral.IO;
import xyz.koral.table.Table;

public class NameDis_Step2a_Params 
{
	public static void main(String[] args) 
	{
		if (args.length != 4)
		{
			System.out.println("USAGE: recallFile namesFile sampledNamesFile_out sampleNamePairsFile_out");
			return;
		}
		long time = System.currentTimeMillis();
		
		File recallFile = new File(args[0]);
		System.out.println("recallFile " + recallFile.getAbsolutePath());
		File namesFile = new File(args[1]);
		System.out.println("namesFile " + namesFile.getAbsolutePath());
		
		List<AmbiguousName> sampledNames = sample(50000, recallFile, namesFile, 123456); // sample from ambiguous where we have recall info.
		Table namePairs = namePairs(sampledNames, 7890); // random name pairs for measuring precision 
		
		IO.writeJSONStream(sampledNames.stream(), IO.ostream(new File(args[2])));
		IO.writeCSV(namePairs.toCSV(), IO.ostream(new File(args[3])));
		
		System.out.println((System.currentTimeMillis() - time) + " ms total exec time.");
	}
	
	static List<AmbiguousName> sample(int nMax, File recallClustersFile, File ambiguousNamesFile, long seed)
	{
		Random rnd = new Random(seed);
		long time = System.currentTimeMillis();
		
		List<String> recallNames = IO.readJSONStream(IO.istream(recallClustersFile), DisambiguatedName.class).map(d -> d.name).collect(Collectors.toList());
		System.out.println(recallNames.size() + " names with recall info.");
		Collections.shuffle(recallNames, rnd);
		recallNames = recallNames.subList(0,  Math.min(recallNames.size(), nMax*5)); // already reduce the number of names here to not load all names in memory
		Set<String> recallNames_ = new HashSet<>(recallNames);
		
		List<AmbiguousName> names = 
		IO.readJSONStreamParallel(IO.istream(ambiguousNamesFile), AmbiguousName.class)
		.filter(name -> name.name != null && recallNames_.contains(name.name))
		.sorted()
		.collect(Collectors.toList());
		
		Collections.shuffle(names);
		List<AmbiguousName> result = names.stream().limit(nMax).collect(Collectors.toList());
		System.out.println((System.currentTimeMillis() - time) + " ms for " + result.size() + " samples.");
	
		return result;
	}
	
	static Table namePairs(List<AmbiguousName> names, long seed)
	{
		long time = System.currentTimeMillis();
		Random rnd = new Random(seed);
		Table nn = names.stream()
		.map(a1 -> 
		{
			Set<Integer> pubs = a1.pubs.stream().map(pub -> pub.id).collect(Collectors.toSet());
			String otherName = null;
			while (otherName == null)
			{
				AmbiguousName a2 = names.get(rnd.nextInt(names.size()));
				if (a2.name.equals(a1.name)) continue;
				boolean overlap = false;
				for (PubProp pp : a2.pubs)
				{
					if (pubs.contains(pp.id)) 
					{
						overlap = true;
						break;
					}
				}
				if (!overlap) otherName = a2.name;
			}
			
			return new String[] { a1.name, otherName };
		})
		.collect(Table.textCollector())
		.setColNames_m("name1", "name2");
		System.out.println((System.currentTimeMillis() - time) + " ms for " + nn.nrows() + " namePairs.");
		
		return nn;
	}
	
}
