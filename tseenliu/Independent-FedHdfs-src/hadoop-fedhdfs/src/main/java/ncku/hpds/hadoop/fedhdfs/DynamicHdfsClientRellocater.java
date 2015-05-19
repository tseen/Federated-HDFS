package ncku.hpds.hadoop.fedhdfs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;
import org.w3c.dom.Element;

public class DynamicHdfsClientRellocater {

	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub
		String Alex = args[0];
		File XMfile = new File("etc/hadoop/fedhadoop-clusters.xml"); 
		TopcloudSelector top = new TopcloudSelector(XMfile, Alex);
		System.out.println("ANS :" + top.getTopCloud());
	}
}
