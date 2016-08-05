package fbicloud.botrank ;

import java.io.DataInput ;
import java.io.DataOutput ;
import java.io.IOException ;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable ;
import org.apache.hadoop.io.WritableUtils ;
/**
 * This key is a composite key. 
 * The "actual" key is the k1_tuples. The secondary sort will be performed against the k2_time.
 */
public class CompositeValue implements WritableComparable<CompositeValue>
{
	private int i ;
	private float f;
	private String s;
	
	
	
	public CompositeValue () {}
	
	public CompositeValue ( int i, float f )
	{
		this.i = i ;
		this.f = f ;
		this.s = "" ;
	}
	public CompositeValue (  String s)
	{
		this.i = 0 ;
		this.f = 0 ;
		this.s = s ;
	}
	public CompositeValue ( int i, float f , String s)
	{
		this.i = i ;
		this.f = f ;
		this.s = s ;
	}
	
	public String getString(){
		 return s;
	}
	public float getFloat(){
		 return f;
	}
	public int getInt(){
		 return i;
	}
	
	@Override
	public String toString ()
	{
		return Integer.toString(i) + "\t" + Float.toString(f) + "\t"+ s ;
	}
	
	@Override
	public void readFields ( DataInput in ) throws IOException
	{
		i = in.readInt();
		f = in.readFloat();
		s = in.readUTF() ;
	}
	
	@Override
	public void write ( DataOutput out ) throws IOException
	{
		out.writeInt(i);
		out.writeFloat(f);
		out.writeUTF(s);
	}
	
	@Override
	public int compareTo(CompositeValue o)
	{
		if (!(i==o.i)){
			return -1;
		}
		else if (f!=o.f){
		
				return 1;
		}
		else{
			return 0;
		}
	}
	
	
}
