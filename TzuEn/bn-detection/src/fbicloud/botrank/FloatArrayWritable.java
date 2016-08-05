package fbicloud.botrank;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

public class FloatArrayWritable implements WritableComparable<FloatArrayWritable> {
	private FloatWritable[] values;
	private static Random r;

	public FloatArrayWritable() {
		r = new Random(System.nanoTime());
	}

	public FloatArrayWritable(FloatWritable[] iw) {
		r = new Random(System.nanoTime());
		values = iw.clone();
	}

	public FloatArrayWritable(FloatArrayWritable law) {
		values = law.values.clone();
	}
	
	public void set(FloatWritable[] iw){
		r = new Random(System.nanoTime());
		values = iw.clone();
	}

	public FloatWritable[] getArray() {
		return values;
	}

	@Override
	public String toString() {
		String str = values.length + ":";
		for (int i = 0; i < values.length; i++) {
			str += values[i].get() + "|";
		}
		return str;
	}

	public void readFields(DataInput in) {
		try {
			int len = in.readInt();
//			System.out.println("Deserializing: " + in.toString() + " of length "
					//+ len);
			values = new FloatWritable[len]; // construct values
			for (int i = 0; i < values.length; i++) {
				FloatWritable value = new FloatWritable();
//				System.out.println("Trying to read longwritable " + i);
				value.readFields(in); // read a value
				values[i] = value; // store it in values
			}
		} catch (Exception ie) {
			values = new FloatWritable[1];
			values[0] = new FloatWritable(-1);
			System.err.println("Can't deserialize: " + ie.getMessage());
		}
	}

	public void write(DataOutput out) throws IOException {
//		System.out.println("Serializing: " + this);
		out.writeInt(values.length); // write values
		for (int i = 0; i < values.length; i++) {
			values[i].write(out);
		}
	}

	public int compareTo(FloatArrayWritable arg0) {
		// Compare two longs randomly so that the output is shuffled randomly and
		// not according to their values
		if (r.nextBoolean())
			return -1;
		else
			return 1;
	}
	
	@Override
	public int hashCode() {
	   return super.hashCode();
	}

	/*
	 * public int compareTo(FloatArrayWritable o) {
	 * 
	 * }
	 */
}

