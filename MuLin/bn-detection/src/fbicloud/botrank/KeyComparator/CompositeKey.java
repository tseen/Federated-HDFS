package fbicloud.botrank.KeyComparator ;

import java.io.DataInput ;
import java.io.DataOutput ;
import java.io.IOException ;
import org.apache.hadoop.io.WritableComparable ;
import org.apache.hadoop.io.WritableUtils ;
/**
 * This key is a composite key. 
 * The "actual" key is the k1_tuples. The secondary sort will be performed against the k2_time.
 */
public class CompositeKey implements WritableComparable<CompositeKey>
{
	private String k1_tuples ;
	private long k2_time ;
	
	
	public CompositeKey () {}
	
	public CompositeKey ( String tuples, long time )
	{
		this.k1_tuples = tuples ;
		this.k2_time = time ;
	}
	
	public String k1toString ()
	{
		return k1_tuples ;
	}
	
	@Override
	public String toString ()
	{
		return (new StringBuilder()).append(k1_tuples).append(',').append(k2_time).toString() ;
	}
	
	@Override
	public void readFields ( DataInput in ) throws IOException
	{
		k1_tuples = WritableUtils.readString(in) ;
		k2_time = WritableUtils.readVLong(in) ;
	}
	
	@Override
	public void write ( DataOutput out ) throws IOException
	{
		WritableUtils.writeString(out, k1_tuples) ;
		WritableUtils.writeVLong(out, k2_time) ;
	}
	
	@Override
	public int compareTo(CompositeKey o)
	{
		if (!k1_tuples.equals(o.k1_tuples)){
			return k1_tuples.compareTo(o.k1_tuples);
		}
		else if (k2_time!=o.k2_time){
			return k2_time<o.k2_time ? -1 : 1;
		}
		else{
			return 0;
		}
	}
	
	public void setTuples(String tuples)
	{
		this.k1_tuples = tuples;
	}
	
	public void setTime(long time)
	{
		this.k2_time = time;
	}
	
	public String getTuples()
	{
		return k1_tuples;
	}
	
	public long getTime()
	{
		return k2_time;
	}
}
