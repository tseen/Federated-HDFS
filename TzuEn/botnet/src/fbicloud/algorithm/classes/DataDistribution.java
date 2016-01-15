package fbicloud.algorithm.classes ;

import java.util.ArrayList ;
import java.math.BigDecimal ;
import java.math.RoundingMode ;


public class DataDistribution
{
	long max ;
	long min ;
	double mean ;
	double std ;
	
	public DataDistribution ( ArrayList<Long> tmpList )
	{
		// calculate max, min, mean, STD
		if ( tmpList.size() != 0 )
		{
			long sum = 0 ;
			double temp = 0 ;
			
			this.max = Long.MIN_VALUE ;
			this.min = Long.MAX_VALUE ;
			for ( int i = 0 ; i < tmpList.size() ; i ++ )
			{
				sum += tmpList.get(i) ;
				if ( tmpList.get(i) > this.max )
				{
					this.max = tmpList.get(i) ;
				}
				if ( tmpList.get(i) < this.min )
				{
					this.min = tmpList.get(i) ;
				}
			}
			// mean
			this.mean = (double) sum / tmpList.size() ;
			
			// calculate STD
			for ( long tmpLong : tmpList )
			{
				temp += ( tmpLong - this.mean ) * ( tmpLong - this.mean ) ;
			}
			this.std = Math.sqrt ( temp / tmpList.size() ) ;
		}
		// don't have to calculate max, min, mean, STD
		else
		{
			this.max = -1 ;
			this.min = -1 ;
			this.mean = -1 ;
			this.std = -1 ;
		}
	}
	
	public long getMax()
	{
		return max ;
	}
	public long getMin()
	{
		return min ;
	}
	public double getMean()
	{
		return round ( mean, 5 ) ;
	}
	public double getStd()
	{
		return round ( std, 5 ) ;
	}
	
	public double round ( double value, int places )
	{
		if ( places < 0 )
		{
			throw new IllegalArgumentException() ;
		}
		
		BigDecimal bd = new BigDecimal ( value ) ;
		bd = bd.setScale ( places, RoundingMode.HALF_UP ) ;
		return bd.doubleValue() ;
	}
}
