package ch.ethz.coss.namedis.data;

public class PubMeta 
{
	public int i;
	public String uid;
	public String doi;
	public String type;
	public String isoj;
	public int year;

	public int[] refs;
	public int[] cits;
	
	public AuthorMeta[] authors;
	
	public String title;
	public String[] subjects;
	public String[] keywords;
	public String[] urls;
	public String summary;
}
