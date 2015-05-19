package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.w3c.dom.Element;

import ncku.hpds.hadoop.fedhdfs.FedHdfsConParser;
import ncku.hpds.hadoop.fedhdfs.GlobalNamespaceObject;
import ncku.hpds.hadoop.fedhdfs.PathInfo;

public class LsGlobalNamespace {

	private String address = "127.0.0.1";
	private int port = 8764;
	
	java.io.File path = new java.io.File("etc/hadoop/fedhadoop-clusters.xml");
	FedHdfsConParser hdfsIpList = new FedHdfsConParser(path);
	Vector<Element> theElements = hdfsIpList.getElements();
	
	public LsGlobalNamespace() {
		System.out.println("AirDrive");
		System.out.println("PDrive");
	}
	
	public LsGlobalNamespace(String pathArgument) {
		GlobalNamespaceClient(pathArgument);
	}
	
	public LsGlobalNamespace(String pathArgument, boolean recursive) {
		if (recursive = true) {
			recursiveGlobalNamespaceClient(pathArgument);	
		}
	}
	
	private void GlobalNamespaceClient(String pathArgument) {
		
		Socket client = new Socket();
		ObjectInputStream ObjectIn;
		InetSocketAddress isa = new InetSocketAddress(this.address, this.port);
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
			
			/* TODO */
			if(pathArgument.contains("/")) {
				
				String[] globalPath = pathArgument.split("/");
				if (globalPath.length == 2) {
					String globalFile = globalPath[1];
	                if (GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().containsKey(globalFile)) {
	        			
	        			//HashMap<String, PathInfo> values = TestLogicalMappingTable.get("FedRDD");
	        			for (Map.Entry<String, PathInfo> entry : GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalFile).entrySet()) {
	        				
	        				String hostName = entry.getKey();
	        				String PhysicalPath = GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalFile).get(hostName).getPath().toString();;
	        				
	        				gnFileStatus(hostName, PhysicalPath);
							System.out.println(" " + "AirDrive" + "/" + globalFile + "/" + hostName + PhysicalPath);
	        			}
	        		}
				}
				else if (globalPath.length > 2) {
					String globalFile = globalPath[1];
					String hostName = globalPath[2];
					int tmp = pathArgument.indexOf("/user");
					String path = pathArgument.substring(tmp, pathArgument.length());
					System.out.println(globalFile);
					System.out.println(hostName);
					System.out.println(path);
					
					Configuration[] conf = new Configuration[theElements.size()];
					for (int i = 0; i < theElements.size(); i++) {
						conf[i] = new Configuration();
						conf[i].set("fs.defaultFS", "hdfs://" + FedHdfsConParser.getValue("fs.default.name", theElements.elementAt(i)));
					}
					for (int i = 0; i < theElements.size(); i++) {
		    			if (hostName.equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
		    				Ls.printFiles(path, conf[i], hostName);
		    			}
		    		}
				}
			}
			else if (pathArgument.equals("AirDrive")){
				
				/* TODO list global File Information */
				for (Entry<String, HashMap<String, PathInfo>> entry : GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().entrySet()) {
					
					String globalFile = entry.getKey();
					HashMap<String, PathInfo> GlobalFileValues = entry.getValue();
					PathInfo tmpPathInfo = new PathInfo();
					ArrayList<PathInfo> PathsInfo = new ArrayList<PathInfo>();
					HashSet<String> setOfOwner = new HashSet<String>();
					HashSet<String> setOfGroup = new HashSet<String>();
					
					ArrayList<Long> eachOfGloalfileLens = new ArrayList<Long>();
					ArrayList<Short> eachOfGloalfileReplicas = new ArrayList<Short>();
					long sumOfLen = 0;
					short maxOfReplica;
					
					for (Map.Entry<String, PathInfo> SubEntry : GlobalFileValues.entrySet()) {
						
						String hostName = SubEntry.getKey();
						String PhysicalPath = SubEntry.getValue().getPath().toString();
						File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
						Configuration conf = new Configuration();
						conf.set("fs.defaultFS", "hdfs://" + FedHdfsConParser.getHdfsUri(FedConfpath, hostName));
						InfoAggreration info = new InfoAggreration();
						info.recursivelySumOfLen(PhysicalPath, conf);
						info.maxOfReplica(PhysicalPath, conf);
						eachOfGloalfileLens.add(info.getSumOfLen());
						eachOfGloalfileReplicas.add(info.getMaxOfReplica());
						
						PathInfo valuesOfHost = SubEntry.getValue();
						PathsInfo.add(valuesOfHost);	
						setOfOwner.add(valuesOfHost.getOwner());
						setOfGroup.add(valuesOfHost.getGroup());
					}
					for (int i = 0; i < eachOfGloalfileLens.size(); i++) {
						sumOfLen +=  eachOfGloalfileLens.get(i);
					}
					maxOfReplica = Collections.max(eachOfGloalfileReplicas);
					tmpPathInfo.setModificationTime(pathInfoItegrate.getModificationTime(PathsInfo).getModificationTime());
					//tmpPathInfo.setLength(pathInfoItegrate.getLenSummarize(PathsInfo).getLength());
					tmpPathInfo.setLength(sumOfLen);
					//tmpPathInfo.setReplication(pathInfoItegrate.getReplication(PathsInfo).getReplication());
					tmpPathInfo.setReplication(maxOfReplica);
					tmpPathInfo.setPermission(null);
					SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm");
					System.out.print("rw-r--r--"/*tmpPathInfo.getPermission().toString()*/
							+ String.format("%4d", tmpPathInfo.getReplication())
							+ " "
							+ setOfOwner
							+ " "
							+ setOfGroup + " "
							+ String.format("%11d", tmpPathInfo.getLength()) + " (" + StringUtils.byteDesc(tmpPathInfo.getLength()) + ")"
							+ " "
							+ f.format(new Timestamp(tmpPathInfo.getModificationTime())));
					
					System.out.println(" " + "AirDrive" + "/" + globalFile);
				}
			}
			
			ObjectIn.close();
			ObjectIn = null;
			client.close();
			
		} catch (java.io.IOException e) {
			System.out.println("Socket connection error");
			System.out.println("IOException :" + e.toString());
		}
	}
	
	private void recursiveGlobalNamespaceClient(String pathArgument) {
		
		Socket client = new Socket();
		ObjectInputStream ObjectIn;
		InetSocketAddress isa = new InetSocketAddress(this.address, this.port);
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
			
			if(pathArgument.equals("AirDrive")) {
				
				for (Entry<String, HashMap<String, PathInfo>> entry : GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().entrySet()) {
					String globalFile = entry.getKey();
					HashMap<String, PathInfo> GlobalFileValues = entry.getValue();
					
					for (Map.Entry<String, PathInfo> SubEntry : GlobalFileValues.entrySet()) {
						
						String hostName = SubEntry.getKey();
						String PhysicalPath = GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalFile).get(hostName).getPath().toString();
						
						LsGlobalNamespace.getFileStatus(hostName, PhysicalPath);
						System.out.println(" " + "AirDrive" + "/" + globalFile + "/" + hostName + PhysicalPath);
					}
				}
			}
			else {
				
				String[] globalPath = pathArgument.split("/");
				if (globalPath.length > 2) {
					System.err.println("Usage: -lsr [AirDrive]/[globalfile]");
					System.exit(2);
				}
				String globalFile = globalPath[1];
				
				Configuration[] conf = new Configuration[theElements.size()];
				for (int i = 0; i < theElements.size(); i++) {
					conf[i] = new Configuration();
					conf[i].set("fs.defaultFS", "hdfs://" + FedHdfsConParser.getValue("fs.default.name", theElements.elementAt(i)));
				}
				
				for (Map.Entry<String, PathInfo> entry : GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalFile).entrySet()) {
					String hostName = entry.getKey();
					String PhysicalPath = GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalFile).get(hostName).getPath().toString();
					
					for (int i = 0; i < theElements.size(); i++) {
		    			if (hostName.equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
		    				Lsr.printFilesRecursively(PhysicalPath, conf[i]);
		    			}
		    		}
				}
			}
			ObjectIn.close();
			ObjectIn = null;
			client.close();
			
		} catch (java.io.IOException e) {
			System.out.println("Socket connection error");
			System.out.println("IOException :" + e.toString());
		}
	}
	
	private void gnFileStatus(String hostName, String PhysicalPath) throws IOException {
		
		File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://" + FedHdfsConParser.getHdfsUri(FedConfpath, hostName));
		InfoAggreration info = new InfoAggreration();
		info.recursivelySumOfLen(PhysicalPath, conf);
		info.maxOfReplica(PhysicalPath, conf);
		FileSystem FS = FileSystem.get(URI.create(PhysicalPath), conf);
		Path Path = new Path(PhysicalPath);
		FileStatus fileStatus = FS.getFileStatus(Path);

		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		System.out.print(fileStatus.getPermission().toString()
				+ String.format("%4d", info.getMaxOfReplica())
				+ " "
				+ fileStatus.getOwner()
				+ " "
				+ fileStatus.getGroup()+ " "
				+ String.format("%11d", info.getSumOfLen())
				+ " "
				+ f.format(new Timestamp(fileStatus.getModificationTime())));
		
	}
	
	private static void getFileStatus(String hostName, String PhysicalPath) throws IOException {
		
		File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://" + FedHdfsConParser.getHdfsUri(FedConfpath, hostName));
		
		FileSystem FS = FileSystem.get(URI.create(PhysicalPath), conf);
		Path Path = new Path(PhysicalPath);
		FileStatus fileStatus = FS.getFileStatus(Path);

		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		System.out.print(fileStatus.getPermission().toString()
				+ String.format("%4d", fileStatus.getReplication())
				+ " "
				+ fileStatus.getOwner()
				+ " "
				+ fileStatus.getGroup()+ " "
				+ String.format("%11d", fileStatus.getLen())
				+ " "
				+ f.format(new Timestamp(fileStatus.getModificationTime())));
		
	}
}

class pathInfoItegrate{
	
	public static PathInfo getModificationTime(ArrayList<PathInfo> tmpPathInfo) {
		
		PathInfo maxModificationTime = Collections.max(tmpPathInfo, new Comparator<PathInfo>() {
					public int compare(PathInfo first, PathInfo second) {
						if (first.getModificationTime() > second.getModificationTime())
							return 1;
						else if (first.getModificationTime() < second.getModificationTime())
							return -1;
						return 0;
					}
				});
		return maxModificationTime;
	}
	
	public static PathInfo getReplication(ArrayList<PathInfo> tmpPathInfo) {
		
		PathInfo maxReplication = Collections.max(tmpPathInfo, new Comparator<PathInfo>() {
					public int compare(PathInfo first, PathInfo second) {
						if (first.getReplication() > second .getReplication())
							return 1;
						else if (first.getReplication() < second .getReplication())
							return -1;
						return 0;
					}
				});
		return maxReplication;
		
	}
	
	public static PathInfo getLenSummarize(ArrayList<PathInfo> tmpPathInfo) {
		
		PathInfo sumOfLen = Collections.max(tmpPathInfo, new Comparator<PathInfo>() {
					public int compare(PathInfo first, PathInfo second) {
						if (first.getLength() > second.getLength())
							return 1;
						else if (first.getLength() < second.getLength())
							return -1;
						return 0;
					}
				});
		return sumOfLen;	
	}	
}