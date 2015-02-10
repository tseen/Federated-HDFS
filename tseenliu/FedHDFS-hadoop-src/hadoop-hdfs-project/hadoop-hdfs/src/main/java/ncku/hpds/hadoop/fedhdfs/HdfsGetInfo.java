package ncku.hpds.hadoop.fedhdfs;

import java.net.URI;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsGetInfo {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String Uri1 = args[0];
		String Uri2 = args[1];
		String Uri3 = args[2];
		
		
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		Configuration conf3 = new Configuration();
		
		// conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");
		
		conf1.set("fs.default.name", "hdfs://10.3.1.34:9000");
		conf2.set("fs.default.name", "hdfs://10.3.1.33:9000");
		conf3.set("fs.default.name", "hdfs://10.0.3.1:9000");

		ShowInfo.print_info(Uri1, conf1);
		ShowInfo.print_info(Uri2, conf2);
		ShowInfo.print_info(Uri3, conf3);
	}
}