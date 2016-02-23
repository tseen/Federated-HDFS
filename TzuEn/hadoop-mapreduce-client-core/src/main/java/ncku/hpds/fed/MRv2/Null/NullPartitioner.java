package ncku.hpds.fed.MRv2.Null;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class NullPartitioner<K,V> extends Partitioner<K, V> {
	
	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		return (int) (Math.random() * numReduceTasks); // this would return a random value in the range
	}
}

