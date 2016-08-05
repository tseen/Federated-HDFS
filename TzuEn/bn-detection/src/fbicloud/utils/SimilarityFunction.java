package fbicloud.utils ;

//import org.apache.commons.math3.stat.correlation.PearsonsCorrelation ;
//import org.apache.commons.math3.ml.distance.EuclideanDistance ;


public class SimilarityFunction
{
	public SimilarityFunction() {}
	
	
	// cosine similarity
	public double cosineSimilarity ( double[] pFeatures, double[] qFeatures )
	{
		double innerP = 0.0 ;
		double normA = 0.0 ;
		double normB = 0.0 ;
		double cosineSim = 0.0 ;
		
		for ( int i = 0 ; i < 20 ; i ++ ) // 20 features
		{
			innerP += pFeatures[i] * qFeatures[i] ;
			normA += Math.pow( pFeatures[i], 2 );
			normB += Math.pow( qFeatures[i], 2 );
		}
		cosineSim = innerP / ( Math.sqrt(normA) * Math.sqrt(normB) ) ;
		// test
		//System.out.println( "cosineSim = " + cosineSim ) ;
		
		return cosineSim ;
	}
	
	// cosine similarity for grouping different src-dst IP ( 3rd grouping )
	public double cosineSimilarity3rdGrouping ( double[] pFeatures, double[] qFeatures )
	{
		int[] index = { 0, 1, 2, 4, 7, 12, 13, 15, 17, 18 } ;
		int num = 10 ;
		
		// new feature vector array
		double[] pFV = new double[num] ;
		double[] qFV = new double[num] ;
		
		// assign value to pFV, qFV
		for ( int i = 0 ; i < num ; i ++ )
		{
			pFV[i] = pFeatures[ index[i] ] ;
			qFV[i] = qFeatures[ index[i] ] ;
			// test
			System.out.println( i + " : pFV = " + pFV[i] ) ;
			System.out.println( i + " : qFV = " + qFV[i] ) ;
		}
		
		// variable
		double innerP = 0.0 ;
		double normA = 0.0 ;
		double normB = 0.0 ;
		double cosineSim = 0.0 ;
		
		for ( int i = 0 ; i < num ; i ++ )
		{
			innerP += pFV[i] * qFV[i] ;
			normA += Math.pow( pFV[i], 2 );
			normB += Math.pow( qFV[i], 2 );
		}
		cosineSim = innerP / ( Math.sqrt(normA) * Math.sqrt(normB) ) ;
		// test
		System.out.println( "cosineSim = " + cosineSim ) ;
		
		return cosineSim ;
	}
	
	/*// correlation distance
	public double correlationDistance ( double[] pFeatures, double[] qFeatures )
	{
		int[] index = { 2, 3, 5, 8, 9, 10, 13, 14, 17, 19 } ;
		int num = 10 ;
		
		// new feature vector array
		double[] pFV = new double[num] ;
		double[] qFV = new double[num] ;
		
		// assign value to pFV, qFV
		for ( int i = 0 ; i < num ; i ++ )
		{
			pFV[i] = pFeatures[ index[i] ] ;
			qFV[i] = qFeatures[ index[i] ] ;
			// test
			System.out.println( i + " : pFV = " + pFV[i] ) ;
			System.out.println( i + " : qFV = " + qFV[i] ) ;
		}
		
		// correlation coefficient
		PearsonsCorrelation psc = new PearsonsCorrelation() ;
		double coefficient = psc.correlation( pFV, qFV ) ;
		// test
		System.out.println( "coefficient = " + coefficient ) ;
		
		return coefficient ;
	}
	*/
	
	// similarity function used by Kai-Wei
	public double exponentialSimilarity ( double[] pFeatures, double[] qFeatures, double similarity )
	{
		double top = 0.0 ;
		double bottom = 0.0 ;
		double expoSimilarity = 0.0 ;
		double flag = 0.0 ;
		int count = 0 ;
		
		for ( int i = 0 ; i < 20 ; i ++ )
		{
			top = Math.abs( pFeatures[i] - qFeatures[i] ) ;
			bottom = Math.abs( pFeatures[i] + qFeatures[i] ) ;
			expoSimilarity = Math.exp( -top/bottom ) ;
			
			if ( expoSimilarity > similarity )
			{
				count ++ ;
			}
		}
		if ( (count/20) > similarity )
		{
			flag = 1.0 ;
		}
		return flag ;
	}
	
	// similarity function used by Kai-Wei ( 3rd grouping )
	public double exponentialSimilarity3rdGrouping ( double[] pFeatures, double[] qFeatures, double similarity )
	{
		int[] index = { 1, 2, 3, 4, 13 } ;
		int num = 5 ;
		
		// new feature vector array
		double[] pFV = new double[num] ;
		double[] qFV = new double[num] ;
		
		// assign value to pFV, qFV
		for ( int i = 0 ; i < num ; i ++ )
		{
			pFV[i] = pFeatures[ index[i] ] ;
			qFV[i] = qFeatures[ index[i] ] ;
			// test
			System.out.println( i + " : pFV = " + pFV[i] ) ;
			System.out.println( i + " : qFV = " + qFV[i] ) ;
		}
		
		double top = 0.0 ;
		double bottom = 0.0 ;
		double expoSimilarity = 0.0 ;
		double flag = 1.0 ;
		
		for ( int i = 0 ; i < num ; i ++ )
		{
			top = Math.abs( pFV[i] - qFV[i] ) ;
			bottom = Math.abs( pFV[i] + qFV[i] ) ;
			expoSimilarity = Math.exp( -top/bottom ) ;
			// test
			System.out.println( i + " : expoSimilarity = " + expoSimilarity ) ;
			
			if ( expoSimilarity < similarity )
			{
				flag = 0.0 ;
				break ;
			}
		}
		return flag ;
	}
	
	
	// EuclideanDistance
	public int euclideanDistance ( double[] pFeatures, double[] qFeatures, double distance )
	{
		int flag = 0 ;
		
		double dif = 0.0 ;
		double squareSum = 0.0 ;
		double euDistance = 0.0 ;
		for ( int i = 0 ; i < 20 ; i ++ )
		{
			dif = Math.abs( pFeatures[i] - qFeatures[i] ) ;
			squareSum += Math.pow( dif, 2 ) ;
		}
		euDistance = Math.sqrt( squareSum ) ;
		// test
		System.out.println( "euDistance = " + euDistance ) ;
		
		if ( euDistance < distance )
		{
			flag = 1 ;
		}
		
		return flag ;
	}
	
	// EuclideanDistance for selected FV
	public int euclideanDistanceSelectedFV ( double[] pFeatures, double[] qFeatures, double distance )
	{
		// select features
		int[] index = { 0, 1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 14, 15, 18 } ;
		int num = 14 ;
		
		// new feature vector array
		double[] pFV = new double[num] ;
		double[] qFV = new double[num] ;
		
		// assign value to pFV, qFV
		for ( int i = 0 ; i < num ; i ++ )
		{
			pFV[i] = pFeatures[ index[i] ] ;
			qFV[i] = qFeatures[ index[i] ] ;
		}
		
		
		int flag = 0 ;
		
		double dif = 0.0 ;
		double squareSum = 0.0 ;
		double euDistance = 0.0 ;
		for ( int i = 0 ; i < num ; i ++ )
		{
			dif = Math.abs( pFV[i] - qFV[i] ) ;
			squareSum += Math.pow( dif, 2 ) ;
		}
		euDistance = Math.sqrt( squareSum ) ;
		// test
		//System.out.println( "euDistance = " + euDistance ) ;
		
		if ( euDistance < distance )
		{
			flag = 1 ;
		}
		
		return flag ;
	}
	
	// EuclideanDistance for graph
	public int euclideanDistanceSelectedFVForGraph ( double[] pFeatures, double[] qFeatures, double distance )
	{
		// select features
		int[] index = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 17, 18, 19 } ;
		int num = 18 ;
		
		// new feature vector array
		double[] pFV = new double[num] ;
		double[] qFV = new double[num] ;
		
		// assign value to pFV, qFV
		for ( int i = 0 ; i < num ; i ++ )
		{
			pFV[i] = pFeatures[ index[i] ] ;
			qFV[i] = qFeatures[ index[i] ] ;
		}
		
		
		int flag = 0 ;
		
		double dif = 0.0 ;
		double squareSum = 0.0 ;
		double euDistance = 0.0 ;
		for ( int i = 0 ; i < num ; i ++ )
		{
			dif = Math.abs( pFV[i] - qFV[i] ) ;
			squareSum += Math.pow( dif, 2 ) ;
		}
		euDistance = Math.sqrt( squareSum ) ;
		// test
		System.out.println( "euDistance = " + euDistance ) ;
		
		if ( euDistance < distance )
		{
			flag = 1 ;
		}
		
		return flag ;
	}
	
}
