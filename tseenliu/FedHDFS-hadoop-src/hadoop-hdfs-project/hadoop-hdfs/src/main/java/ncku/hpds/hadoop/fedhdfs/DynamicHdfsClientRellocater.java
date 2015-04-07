package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class DynamicHdfsClientRellocate {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		//Configuration conf = new HdfsConfiguration();
		DistributedFileSystem cluster1 = new DistributedFileSystem();
		
		System.out.println(cluster1.getConf().toString());
		
	}
}
