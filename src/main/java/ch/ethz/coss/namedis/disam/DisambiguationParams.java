package ch.ethz.coss.namedis.disam;

import java.util.Arrays;

public class DisambiguationParams 
{
	public enum ParamName
	{
		SelfCitation,
		CoAuthors,
		References,
		Citations,
		Affiliations,
		
		ClusterMergeSum,
		ClusterMergeLink,
		ClusterSize1
	}
	
	public static String[] shortNames = new String[] { "s", "a", "r", "c", "u", "b3", "b2", "b4" };
	
	float[] params;
	
	public void set(int index, float value)
	{
		params[index] = value;
	}
	
	public void set(ParamName name, float value)
	{
		params[name.ordinal()] = value;
	}
	
	public float get(int index)
	{
		return params[index];
	}
	
	public float get(ParamName name)
	{
		return params[name.ordinal()];
	}
	
	public DisambiguationParams()
	{
		params = new float[ParamName.values().length];
	}
	
	public DisambiguationParams(String line)
	{
		this();
		
		for (int i=0; i<shortNames.length; i++)
		{
			int i1 = line.indexOf(shortNames[i] + "=");
			if (i1 != -1)
			{
				int i2 = line.indexOf(" ", i1);
				if (i2 == -1) i2 = line.length();
				String value = line.substring(i1 + shortNames[i].length() + 1, i2);
				
				try
				{
					params[i] = Float.parseFloat(value);
				}
				catch (Exception ex)
				{
					
				}
			}
		}
	}
	
	public DisambiguationParams(float... params)
	{
		this.params = params;
	}
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<params.length; i++)
		{
			if (i > 0 ) sb.append(" ");
			sb.append(shortNames[i] + "=" + params[i]);
		}
		
		return sb.toString();
	}
	
	public DisambiguationParams copy()
	{
		DisambiguationParams p = new DisambiguationParams();
		p.params = Arrays.copyOf(params, params.length);
		return p;
	}
	
	public int size()
	{
		return params.length;
	}
}
