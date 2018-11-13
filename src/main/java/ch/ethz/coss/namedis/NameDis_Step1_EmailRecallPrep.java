package ch.ethz.coss.namedis;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.ethz.coss.namedis.data.PubMeta;
import xyz.koral.IO;

public class NameDis_Step1_EmailRecallPrep 
{
	public static void main(String[] args)
	{
		if (args.length != 2)
		{
			System.out.println("USAGE: emailFile pubMetaFile recallClustersOutFile");
			return;
		}
		
		Map<Integer, String[]> pubIndexToEmails = 
		IO.readJSONStream(IO.istream(new File(args[0])), PubEmails.class)
		.collect(Collectors.toMap(p -> p.i, p -> Stream.of(p.emails).distinct().toArray(n -> new String[n]))); // make sure emails occur only once
		System.out.println(pubIndexToEmails.size() + " ms pubs with email info.");
		
		Map<String, List<PubMeta>> pubsByName = NameDis_Step1_DataPrep.pubsByName(new File(args[1]));
		System.out.println(pubsByName.size() + " unqiue ms names.");
		
		IO.writeJSONStream(namesWithEmailClusters(pubIndexToEmails, pubsByName), IO.ostream(new File(args[2])));
		System.out.println("ms done");
	}
	
	/**
	 * 
	 * @param pubIndexToEmails
	 * @param pubsByName
	 * @return it's not a disambiguated name, instead a name with recall clusters, i.e. publications that should be clustered together
	 */
	static Stream<DisambiguatedName> namesWithEmailClusters(Map<Integer, String[]> pubIndexToEmails, Map<String, List<PubMeta>> pubsByName)
	{
		return pubsByName.entrySet()
		.parallelStream()
		.filter(e -> 
		{
			int n = e.getValue().size();
			return n >= 2 && n <= NameDis_Step1_DataPrep.maxNameSize;
		})
		.map(e -> 
		{
			Map<String, List<Integer>> emailToPubs = new HashMap<>();
			for (PubMeta pub : e.getValue())
			{
				String[] emails = pubIndexToEmails.get(pub.i);
				if (emails != null)
				{
					for (String email : emails)
					{
						List<Integer> pubList = emailToPubs.get(email);
						if (pubList == null)
						{
							pubList = new ArrayList<>();
							emailToPubs.put(email, pubList);
						}
						pubList.add(pub.i);
					}
				}
			}
			
			DisambiguatedName d = new DisambiguatedName();
			d.name = e.getKey();
			d.clusters = emailToPubs.values().stream().filter(l -> l.size() >= 2).map(l -> l.stream().mapToInt(i -> i).toArray()).toArray(n -> new int[n][]);		
			if (d.clusters.length == 0) return null;
			return d;
		})
		.filter(d -> d != null)
		.sorted((d1, d2) -> d1.name.compareTo(d2.name));
	}

}

class PubEmails
{
	public int i;
	public String[] emails;
	
	public PubEmails()
	{
		
	}
	
	public PubEmails(int i, String[] emails)
	{
		this.i = i;
		this.emails = emails;
	}
}