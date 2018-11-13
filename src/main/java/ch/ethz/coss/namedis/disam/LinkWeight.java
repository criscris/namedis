package ch.ethz.coss.namedis.disam;

import java.io.Serializable;

public class LinkWeight implements Serializable
{
	private static final long serialVersionUID = -5458812321178083222L;
	
	public int pubID1;
	public int pubID2;
	
	public float selfCitation;
	public float coAuthors;
	public float references;
	public float citations;
	public float affiliations;
}
