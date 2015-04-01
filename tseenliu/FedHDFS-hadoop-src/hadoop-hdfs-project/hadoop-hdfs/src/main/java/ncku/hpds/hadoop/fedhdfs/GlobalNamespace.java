package ncku.hpds.hadoop.fedhdfs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.w3c.dom.Element;

public class GlobalNamespace {
	
	 private static java.io.File path = new java.io.File("etc/hadoop/fedhadoop-clusters.xml");
	 private static FedHdfsConParser hdfsIpList = new FedHdfsConParser(path);
	 private static Vector<Element> theElements = hdfsIpList.getElements();
	 private static Configuration[] conf = new Configuration[theElements.size()];
	 
	 //private static PhysicalVolumeManager physicalDrive = new PhysicalVolumeManager();
	 private static LogicalVolumeManager test = new LogicalVolumeManager();
	 
	
	public void setFedConf(){
		for (int i = 0; i < theElements.size(); i++) {
			conf[i] = new Configuration();
			conf[i].set(
					"fs.default.name",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theElements.elementAt(i)));
		}
	}
	
	public void DynamicConstructPD() throws IOException{
		
		PhysicalVolumeManager physicalDrive = new PhysicalVolumeManager();
		for (int i = 0; i < theElements.size(); i++) {
			physicalDrive.addfsElementToArrayLists("/user/hpds", conf[i]);
		}
		physicalDrive.updataPTable(theElements); //Important
		physicalDrive.hashTableDownload();
	}
	
	public void UserDefinedVD(String logicalName, String hostName, String path) throws IOException{
		
		//LogicalVolumeManager test = new LogicalVolumeManager();
		test.addElementsToLTable(logicalName, hostName, path);
		test.showHashTable();
		test.hashTableDownload();
	
	}
	

	
	/*public static void main(String[] args) throws Exception {

		String[] uri = args;
		

		java.io.File path = new java.io.File("etc/hadoop/fedhadoop-clusters.xml");
		FedHdfsConParser hdfsIpList = new FedHdfsConParser(path);
		Vector<Element> theElements = hdfsIpList.getElements();
		Configuration[] conf = new Configuration[theElements.size()];

		for (int i = 0; i < theElements.size(); i++) {
			conf[i] = new Configuration();
			conf[i].set(
					"fs.default.name",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theElements.elementAt(i)));
		}

		PhysicalVolumeManager physicalDrive = new PhysicalVolumeManager();
		for (int i = 0; i < theElements.size(); i++) {
			physicalDrive.addfsElementToArrayLists("/user/hpds", conf[i]);
		}
		//test.ShowAllfsElements();
		
		physicalDrive.updataPTable(theElements); //Important
		
		//physicalDrive.ShowHashTable();
		
		physicalDrive.ShowVectorSize();
		
		physicalDrive.ShowHashTableSize();
		
		physicalDrive.HashTableDownload();
		
		
		
		
		
		
		
		/*  DEMO TEST */
		
		/*final Scanner in = new Scanner(System.in);
		System.out.println("Usage: [generic options]" );
		System.out.println("[-ShowClustersNum]" );
		System.out.println("[-ShowHashTableSize]" );
		System.out.println("[-ShowHashTable]" );
		System.out.println("[-CheckDirectory | Files]" );
		System.out.println("[-CheckHostName]" );
		System.out.print("Please Input arguments : " );
		while (in.hasNext()) {
			final String line = in.nextLine();
			System.out.println("Please Input arguments : " );
			
			
			if ("-ShowClustersNum".equalsIgnoreCase(line)){
				System.out.println(" The ShowClustersNum is : " + physicalDrive.DShowVectorSize());
			}
			
			if ("-ShowHashTableSize".equalsIgnoreCase(line)){
				System.out.println(" The ShowHashTableSize is : " + physicalDrive.DShowHashTableSize());
			}
			
			if ("-ShowHashTable".equalsIgnoreCase(line)){
				System.out.println(physicalDrive.DShowHashTable().toString());
			}
			
			if ("-CheckHostName".equalsIgnoreCase(line)){
				System.out.println(" The Cluster is exist (Y/N) : " + physicalDrive.DgetAhostName(line.substring(15)));
			}
			
			if ("-CheckDirectory | Files".equalsIgnoreCase(line)){
				System.out.println(" The ShowHashTableSize is : " + physicalDrive.DcheckAclusterPaths(line.substring(24)));
			}
			
			if ("end".equalsIgnoreCase(line)) {
				System.out.println("Ending one thread");
				break;
			}
			
			else{
				System.out.print("Please Input arguments : " );			
			}
			
		}
		
	}*/
}