package ch.ethz.coss.namedis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.gson.Gson;

import ch.ethz.coss.namedis.data.AuthorMeta;
import ch.ethz.coss.namedis.data.PubMeta;
import ch.ethz.coss.namedis.disam.PubProp;
import xyz.koral.IO;

public class NameDis_Step1_DataPrep 
{
	static final int maxNameSize = 10000;

	public static void main(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.out.println("USAGE: pubMetaFile ambiguousNamesOutFile");
			return;
		}
		File wosFile = new File(args[0]); 
		File outFile = new File(args[1]);
		System.out.println("Writing to " + outFile.getAbsolutePath());
		System.out.println(IO.jvmInfo());
		
		long t1 = System.currentTimeMillis();
		Map<String, List<PubMeta>> pubsByName = pubsByName(wosFile);
		
		List<String> names = pubsByName.keySet().stream().collect(Collectors.toList());
		Collections.sort(names);
		Map<String, Integer> nameToIndex = new HashMap<>();
		for (int i=0; i<names.size(); i++) nameToIndex.put(names.get(i), i);
		
		
		long t2 = System.currentTimeMillis();
		class Stats
		{
			long count;
			long countMaxExcluded;
			long count1Excluded;
		}
		Stats stats = new Stats();
		
		Gson gson = new Gson();
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(IO.ostream(outFile), Charset.forName("UTF-8")));
		
		 
		pubsByName.entrySet()
		.parallelStream()
		.filter(e -> {
			stats.count++;
			if (stats.count % (pubsByName.size() / 50) == 0) System.out.println(stats.count + "/" + pubsByName.size() + " " + (System.currentTimeMillis() - t2) + " ms.");
			boolean exclude = false;
			
			int n = e.getValue().size();
			if (n > maxNameSize)
			{
				stats.countMaxExcluded++;
				exclude = true;
			}
			else if (n < 2)
			{
				stats.count1Excluded++;
				exclude = true;
			}
			
			return !exclude;
		})
		.forEach(e -> {
			AmbiguousName an = new AmbiguousName();
			an.name = e.getKey();
			an.pubs = new ArrayList<>();
			
			
			/* better have a global name <-> index
			// have integers instead of author name strings
			Map<String, Integer> authorNameToIndex = new HashMap<>(); // set of all co-authors within the pubs of the ambiguous name
			int index = 0;
			for (PubMeta pub : e.getValue())
			{
				for (AuthorMeta a : pub.authors)
				{
					String wa = wosAuthor(a.name);
					if (wa == null) continue;
					if (wa.equals(e.getKey())) continue; // do not include cluster author name
					if (authorNameToIndex.containsKey(wa)) continue;
					authorNameToIndex.put(wa, index);
					index++;
				}
			}*/
			
			for (PubMeta pub : e.getValue())
			{
				PubProp prop = new PubProp();
				an.pubs.add(prop);
				
				prop.id = pub.i;
				prop.year = pub.year;
				
				// co-authors
				List<Integer> authors = new ArrayList<>();
				for (AuthorMeta a : pub.authors)
				{
					String wa = wosAuthor(a.name);
					if (wa == null) continue;
					if (wa.equals(an.name)) continue; // no self

					Integer i = nameToIndex.get(wa);
					if (i == null) continue;
					authors.add(i);
				}
				if (authors.size() > 1) Collections.sort(authors);
				prop.coauthors = new int[authors.size()];
				for (int i=0; i<prop.coauthors.length; i++) prop.coauthors[i] = authors.get(i);
				
				prop.references = pub.refs;
				if (prop.references == null) prop.references = new int[0];
				if (prop.references.length > 1) Arrays.sort(prop.references);
				
				prop.citations = pub.cits;
				if (prop.citations == null) prop.citations = new int[0];
				if (prop.citations.length > 1) Arrays.sort(prop.citations);
				
				prop.affiliations = new int[0]; // not yet included
			}
			
			synchronized(writer)
			{
				try 
				{
					writer.write(gson.toJson(an));
					writer.write("\n");
				} 
				catch (IOException ex) 
				{
					throw new Error(ex);
				}
			}
		});
		writer.close();
		
		System.out.println((System.currentTimeMillis() - t1) + " ms total exec time. count=" + stats.count + ". too big=" + stats.countMaxExcluded + ". only 1 paper=" + stats.count1Excluded);
		System.out.println(IO.jvmInfo());
	}
	
	public static Map<String, List<PubMeta>> pubsByName(File pubFile)
	{
		long t1 = System.currentTimeMillis();
		Map<String, List<PubMeta>> pubsByName = new HashMap<>();
		IO.readJSONStreamParallel(IO.istream(pubFile), PubMeta.class)
		.map(pub -> { // gc unneeded info
			PubMeta p = new PubMeta();
			p.i = pub.i;
			p.year = pub.year;
			p.cits = pub.cits;
			p.refs = pub.refs;
			p.authors  = new AuthorMeta[pub.authors.length];
			for (int i=0; i<pub.authors.length; i++)
			{
				p.authors[i] = new AuthorMeta();
				p.authors[i].name = pub.authors[i].name;
			}
			
			return p;
		})
		.sequential()
		.forEach(pub -> {
			for (AuthorMeta author : pub.authors)
			{
				String name = wosAuthor(author.name);
				if (name == null) continue;
				List<PubMeta> pubs = pubsByName.get(name);
				if (pubs == null)
				{
					pubs = new ArrayList<>();
					pubsByName.put(name, pubs);
				}
				pubs.add(pub);
			}
		});
		
		/* takes forever
		.flatMap(pub -> Stream.of(pub.authors)
					.map(author -> wosAuthor(author.name))
					.filter(name -> name != null)
					.collect(Collectors.toSet())
					.stream()
					.map(name -> new Tuple2<String, WosXmlPub>(name, pub))
		)
		.collect(Collectors.groupingBy(t -> t._1, Collectors.mapping(t -> t._2, Collectors.toList())));
		*/
		
		System.out.println(pubsByName.size() + " names in " + (System.currentTimeMillis() - t1) + " ms.");
		System.out.println(IO.jvmInfo());
		return pubsByName;
	}
	
	// valid name: lastname, a[bc]
	public static String wosAuthor(String name)
	{
		if (name == null) return null;
		int i1 = name.indexOf(", ");
		if (i1 == -1) return null;
		if (name.length() < i1 + 3) return null;
		name = name.substring(0, i1 + 3).toLowerCase();
		return name.length() < 5 ? null : name;
	}
}

class AmbiguousName implements Comparable<AmbiguousName>
{
	public String name;
	public List<PubProp> pubs;
	
	public int compareTo(AmbiguousName o) 
	{
		return name.compareTo(o.name);
	}
}
