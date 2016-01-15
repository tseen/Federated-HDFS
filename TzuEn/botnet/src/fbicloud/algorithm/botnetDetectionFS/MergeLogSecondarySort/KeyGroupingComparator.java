package fbicloud.algorithm.botnetDetectionFS.MergeLogSecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class KeyGroupingComparator extends WritableComparator {
	protected KeyGroupingComparator() {
		super(CompositeKey.class, true);//Call the parent class "WritableComparator" constructor
	}
	@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKey key1 = (CompositeKey) w1;
			CompositeKey key2 = (CompositeKey) w2;
			// (check on five tuples)
			if (!key1.getSameKey().equals(key2.getSameKey())){ 
				return key1.getSameKey().compareTo(key2.getSameKey());
			}
			else{
				return 0;
			}
		}
}
