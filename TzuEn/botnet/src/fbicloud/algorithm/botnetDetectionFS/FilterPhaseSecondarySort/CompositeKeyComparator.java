package fbicloud.algorithm.botnetDetectionFS.FilterPhaseSecondarySort;

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
			// (first check on tuples)
			if (!key1.getTuples().equals(key2.getTuples())){ 
				return key1.getTuples().compareTo(key2.getTuples());
			}
			if(key1.getTime()!=key2.getTime()){ 
				return key1.getTime()<key2.getTime() ? -1 : 1;
			} 
			else{
				return 0; 
			}
		}
}
