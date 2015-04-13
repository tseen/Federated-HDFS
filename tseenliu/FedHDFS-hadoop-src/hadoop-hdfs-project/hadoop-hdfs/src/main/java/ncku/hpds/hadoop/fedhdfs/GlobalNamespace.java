package ncku.hpds.hadoop.fedhdfs;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
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

public class GlobalNamespace implements Serializable {
	
	 private static File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
	 private static FedHdfsConParser FedhdfsConfList = new FedHdfsConParser(FedConfpath);
	 private static Vector<Element> theFedhdfsElements = FedhdfsConfList.getElements();
	 private static Configuration[] conf = new Configuration[theFedhdfsElements.size()];
	 
	 private PhysicalVolumeManager physicalDrive = new PhysicalVolumeManager();
	 private LogicalVolumeManager logicalDrive = new LogicalVolumeManager();
	 
	
	public void setFedConf(){
		for (int i = 0; i < theFedhdfsElements.size(); i++) {
			conf[i] = new Configuration();
			conf[i].set(
					"fs.defaultFS",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theFedhdfsElements.elementAt(i)));
		}
	}
	
	public void DynamicConstructPD() throws IOException{
		
		//PhysicalVolumeManager physicalDrive = new PhysicalVolumeManager();
		for (int i = 0; i < theFedhdfsElements.size(); i++) {
			physicalDrive.addfsPathElementToArrayLists("/user/hpds", conf[i]);
		}
		physicalDrive.updataPhysicalTable(theFedhdfsElements); //construct a physical mapping
		physicalDrive.physicalMappingDownload();
		physicalDrive.getFsPathElements().clear();
	}
	
	public void sput(String globalFileName, String hostName, String clusterPath) {
		logicalDrive.TestPutLogicalTable(globalFileName, hostName, clusterPath);
		logicalDrive.showLogicalHashTable();
	}
	
	public void put(String globalFileName, String hostName, String clusterPath) {
		logicalDrive.putToLogicalTable(globalFileName, hostName, clusterPath);
		logicalDrive.showLogicalHashTable();
	}
	
	public void mkdir(String globalFileName) {
		logicalDrive.mkdirGlobalFileName(globalFileName);
		logicalDrive.showLogicalHashTable();
	}
	
	public void rmdir(String globalFileName) {
		logicalDrive.rmdirGlobalFileNam(globalFileName);
		logicalDrive.showLogicalHashTable();
	}
	
	public void rm(String globalFileName, String hostName) {
		logicalDrive.rmLogicalTable(globalFileName, hostName);
		logicalDrive.showLogicalHashTable();
	}
	
	public PhysicalVolumeManager getPhysicalDrive() {
		return physicalDrive;
	}
	
	public LogicalVolumeManager getLogicalDrive() {
		return logicalDrive;
	}
}