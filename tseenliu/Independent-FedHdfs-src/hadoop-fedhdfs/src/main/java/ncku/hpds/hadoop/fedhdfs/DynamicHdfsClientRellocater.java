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
		String file = args[0];
		File XMfile = new File("Alex.xml"); 
		//TopcloudSelector top = new TopcloudSelector(XMfile, Alex);
		
		boolean minTag = true;
		TopcloudSelector top = new TopcloudSelector(file, minTag);
		
		FedHdfsConParser.setSupernamenodeConf(XMfile);
		
		System.out.println("ANS :" + top.getTopCloud());
		
		System.out.println(SuperNamenodeInfo.getSuperNamenodeAddress());
		System.out.println(SuperNamenodeInfo.getFedUserConstructGNPort());
		System.out.println(SuperNamenodeInfo.getGlobalNamespaceServerPort());
		System.out.println(SuperNamenodeInfo.getGNQueryServerPort());
		System.out.println(SuperNamenodeInfo.getAlpha());
		
		SuperNamenodeInfo.show();
		
	}
}