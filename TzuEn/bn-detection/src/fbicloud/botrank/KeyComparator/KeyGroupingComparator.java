package fbicloud.botrank.KeyComparator ;

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
			// (check on tuples)
			if (!key1.getTuples().equals(key2.getTuples())){ 
				return key1.getTuples().compareTo(key2.getTuples());
			}
			else{
				return 0;
			}
		}
}
