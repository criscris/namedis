package ch.ethz.coss.namedis.graph;

import java.io.Serializable;

public interface IWeight extends Serializable
{
	float computeWeight();
	
	String getStringRepresentation();
}
