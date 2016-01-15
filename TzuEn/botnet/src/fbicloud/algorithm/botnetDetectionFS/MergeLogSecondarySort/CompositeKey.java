package fbicloud.algorithm.botnetDetectionFS.MergeLogSecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
/**
 * This key is a composite key. 
 * The "actual" key is the k1_str. The secondary sort will be performed against the k2_time.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
	private String k1_str;
	private long k2_time;
	public CompositeKey() {
	}
	public CompositeKey(String str, long time){
		this.k1_str = str;
		this.k2_time = time;
	}
	public String k1toString() {
		return k1_str;
	}
	@Override
	public String toString() {
		return (new StringBuilder()).append(k1_str).append(',').append(k2_time).toString();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		k1_str = WritableUtils.readString(in);
		k2_time = WritableUtils.readVLong(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, k1_str);
		WritableUtils.writeVLong(out, k2_time);
	}
	@Override
		public int compareTo(CompositeKey o) {
			if (!k1_str.equals(o.k1_str)){
				return k1_str.compareTo(o.k1_str);
			}
			else if (k2_time!=o.k2_time){
				return k2_time<o.k2_time ? -1 : 1;
			}
			else{
				return 0;
			}
		}
	public void setSameKey(String str) {
		this.k1_str = str;
	}
	public void setTime(long time) {
		this.k2_time = time;
	}
	public String getSameKey() {
		return k1_str;
	}
	public long getTime() {
		return k2_time;
	}
}
