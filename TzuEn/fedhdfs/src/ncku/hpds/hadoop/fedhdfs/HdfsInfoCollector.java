package ncku.hpds.hadoop.fedhdfs;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.Vector;

import ncku.hpds.hadoop.fedhdfs.shell.InfoAggreration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;
import org.w3c.dom.Element;

public class HdfsInfoCollector {
	
	private static File FedConfpath = SuperNamenode.XMfile;
	private static FedHdfsConParser hdfsIpList = new FedHdfsConParser(FedConfpath);
	private static Vector<Element> theElements = hdfsIpList.getElements();
	private long hdfsRemaining;
	private long dataSize;
	
	private String address = SuperNamenodeInfo.getSuperNamenodeAddress();
	private int port = SuperNamenodeInfo.getGlobalNamespaceServerPort();
	
	public long getHdfsRemaining(String hostName) throws IOException {
		
		Configuration conf = new Configuration();

		for (int i = 0; i < theElements.size(); i++) {
			if (hostName.equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
				conf.set( "fs.defaultFS", "hdfs://" + FedHdfsConParser.getValue("fs.default.name", theElements.elementAt(i)));
			}
		}
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem dfs = (DistributedFileSystem) fs;
		FsStatus ds = dfs.getStatus();
		hdfsRemaining = ds.getRemaining();
		//System.out.println(hostName + ": DFS Remaining: " + hdfsRemaining + " (" + StringUtils.byteDesc(hdfsRemaining) + ")");
			
        return hdfsRemaining;
	}

	public long getDataSize(String globalfileInput, String hostName) throws Throwable {
		
		Socket client = new Socket();
		ObjectInputStream ObjectIn;
		InetSocketAddress isa = new InetSocketAddress(address, port);
		try {
			client.connect(isa, 10000);
			ObjectIn = new ObjectInputStream(client.getInputStream());
			
			//received object
			GlobalNamespaceObject GN = new GlobalNamespaceObject();
			try {
				GN = (GlobalNamespaceObject) ObjectIn.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if (GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().containsKey(globalfileInput)) {
				
				String PhysicalPath = GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalfileInput).get(hostName).getPath().toString();
				dataSize = getLenOfData(hostName, PhysicalPath);
			}
			
			ObjectIn.close();
			ObjectIn = null;
			client.close();
			
		} catch (java.io.IOException e) {
			System.out.println("Socket connection error");
			System.out.println("IOException :" + e.toString());
		}
		//System.out.println("Data Size: " + dataSize + " (" + StringUtils.byteDesc(dataSize) + ")");
		return dataSize;
	}
	
	private long getLenOfData(String hostName, String PhysicalPath) throws IOException {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://" + FedHdfsConParser.getHdfsUri(FedConfpath, hostName));
		
		InfoAggreration info = new InfoAggreration();
		info.recursivelySumOfLen(PhysicalPath, conf);
		return info.getSumOfLen();
	}

}
