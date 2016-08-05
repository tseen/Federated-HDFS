package fbicloud.botrank.MergeLogKeyComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}
	@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKey key1 = (CompositeKey) w1;
			CompositeKey key2 = (CompositeKey) w2;
			// (first check on five tuples)
			if (!key1.getSameKey().equals(key2.getSameKey())){ 
				return key1.getSameKey().compareTo(key2.getSameKey());
			}
			if(key1.getTime()!=key2.getTime()){ 
				return key1.getTime()<key2.getTime() ? -1 : 1;
			} 
			else{
				return 0; 
			}
		}
}
