package ch.ethz.coss.namedis.disam;

import java.util.Arrays;


public class PublicationSimilarity 
{
	public static LinkWeight computeWeight(PubProp pub, PubProp pubOther)
	{
		if (Math.abs(pub.year - pubOther.year) > 5) return null;
		
		LinkWeight weight = new LinkWeight();
		weight.pubID1 = pub.id;
		weight.pubID2 = pubOther.id;
		
		// self citation
		boolean x1 = Arrays.binarySearch(pub.references, pubOther.id) >= 0;
		boolean x2 = Arrays.binarySearch(pubOther.references, pub.id) >= 0;
		
		if (x1 || x2) weight.selfCitation = 1f;
		if (x1 && x2) weight.selfCitation = 1f;
		
		boolean jaccard = true;
		
		// share co-authors
		if (jaccard)
		{
			float authIntersection = getNoOfEqualNumbersFromSortedArrays(pub.coauthors, pubOther.coauthors);
			float authUnion = pub.coauthors.length + pubOther.coauthors.length - authIntersection;
			weight.coAuthors = authUnion == 0 ? 0 : authIntersection / authUnion;
		}
		else
		{
			int minNoOfCoAuthors = Math.min(pub.coauthors.length, pubOther.coauthors.length);
			weight.coAuthors = minNoOfCoAuthors == 0 ? 0 : ((float) getNoOfEqualNumbersFromSortedArrays(pub.coauthors, pubOther.coauthors) / minNoOfCoAuthors);
		}
		
		// share references
		weight.references = getNoOfEqualNumbersFromSortedArrays(pub.references, pubOther.references);
		
		// share citations
		if (jaccard)
		{
			float citIntersection = getNoOfEqualNumbersFromSortedArrays(pub.citations, pubOther.citations);
			float citUnion = pub.citations.length + pubOther.citations.length - citIntersection;
			weight.citations = citUnion == 0 ? 0 : citIntersection / citUnion;
		}
		else
		{
			int minNoOfCits = Math.min(pub.citations.length, pubOther.citations.length);
			weight.citations = minNoOfCits == 0 ? 0 : ((float) getNoOfEqualNumbersFromSortedArrays(pub.citations, pubOther.citations) / minNoOfCits);
			
		}
		

		
		// share affiliations
		weight.affiliations =  getNoOfEqualNumbersFromSortedArrays(pub.affiliations, pubOther.affiliations);

		return (weight.selfCitation > 0 || weight.coAuthors > 0 || weight.references > 0 || weight.citations > 0 || weight.affiliations > 0) ? weight : null;
	}
	
	/**
	 * arrays must be sorted (ascending) and unique elements
	 * @return number of equal elements
	 */
	static final int getNoOfEqualNumbersFromSortedArrays(int[] arr1, int[] arr2)
	{
		int equalCount = 0;
		if (arr2.length == 0) return 0;
		for (int i=0, j=0; i<arr1.length; i++)
		{
			int a1 = arr1[i];
			int a2 = arr2[j];
			
			while (a2 < a1) 
			{
				j++;
				if (j == arr2.length) return equalCount;
				a2 = arr2[j];
			}
			
			if (a1 == a2) equalCount++;
		}
		
		return equalCount;
	}
}
