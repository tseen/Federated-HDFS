package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import ncku.hpds.hadoop.fedhdfs.shell.Ls;

import org.apache.hadoop.conf.Configuration;

public class LogicalVolumeManager implements Serializable {

	private HashMap<String, HashMap<String, PathInfo>> TestLogicalMappingTable = new HashMap<String, HashMap<String, PathInfo>>();
	
	public void TestPutLogicalTable(String globalFileName, String hostName, String clusterPath) {

		if (TestLogicalMappingTable.containsKey(globalFileName)) {

			while (TestLogicalMappingTable.get(globalFileName).containsKey(hostName)) {
				TestLogicalMappingTable.get(globalFileName).remove(hostName);
				break;
			}
			TestLogicalMappingTable.get(globalFileName).put(hostName, LogicalVolumeManager.getFileStatus(hostName, clusterPath));

		} else {

			TestLogicalMappingTable.put(globalFileName, new HashMap<String, PathInfo>());

			while (TestLogicalMappingTable.get(globalFileName).containsKey(hostName)) {
				TestLogicalMappingTable.get(globalFileName).remove(hostName);
				break;
			}
			TestLogicalMappingTable.get(globalFileName).put(hostName, LogicalVolumeManager.getFileStatus(hostName, clusterPath));
		}

	}
	
	public void mkdirGlobalFileName(String globalFileName) {
		
		TestLogicalMappingTable.put(globalFileName, new HashMap<String, PathInfo>());
	}
	
	public void putToLogicalTable(String globalFileName, String hostName, String clusterPath) {

		if (TestLogicalMappingTable.containsKey(globalFileName)) {

			while (TestLogicalMappingTable.get(globalFileName).containsKey(hostName)) {
				TestLogicalMappingTable.get(globalFileName).remove(hostName);
				break;
			}
			TestLogicalMappingTable.get(globalFileName).put(hostName, LogicalVolumeManager.getFileStatus(hostName, clusterPath));
		}
	}
	
	public void rmdirGlobalFileNam(String globalFileName) {
		
		TestLogicalMappingTable.remove(globalFileName);
		
	}
	
	public void rmLogicalTable(String globalFileName, String hostName) {

		TestLogicalMappingTable.get(globalFileName).remove(hostName);
		
	}
	
	public static PathInfo getFileStatus(String hostName, String path) {
		
		File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://" + FedHdfsConParser.getHdfsUri(FedConfpath, hostName));
		PathInfo tmpPathInfo = new PathInfo();
		try {
			tmpPathInfo.setPath(Ls.getPath(path, conf));
			tmpPathInfo.SetModificationTime(Ls.getModificationTime(path, conf));
			tmpPathInfo.SetPermission(Ls.getPermission(path, conf));
			tmpPathInfo.setReplication(Ls.getReplication(path, conf));
			tmpPathInfo.setOwner(Ls.getOwner(path, conf));
			tmpPathInfo.setGroup(Ls.getGroup(path, conf));
			tmpPathInfo.setLength(Ls.getLen(path, conf));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmpPathInfo;
	}

	public void checkGlobalFileName(String globalFileName) {
		System.out.println(TestLogicalMappingTable.containsKey(globalFileName));
	}

	public boolean showCheckGlobalFileName(String globalFileName) {
		return TestLogicalMappingTable.containsKey(globalFileName);
	}

	public void checkAfedcluster(String globalFileName, String hostname) {
		System.out.println(TestLogicalMappingTable.get(globalFileName).containsKey(
				hostname));
	}

	public boolean showCheckAfedcluster(String globalFileName, String hostname) {
		return TestLogicalMappingTable.get(globalFileName).containsKey(hostname);
	}

	public boolean checkAphysicalPath(String globalFileName, String physicalPath) {
		return TestLogicalMappingTable.get(globalFileName).containsValue(physicalPath);
	}

	public void showAfedPhysicalPathInfo(String globalFileName, String hostname) {
		System.out.println(TestLogicalMappingTable.get(globalFileName).get(hostname));
	}

	public Set<Entry<String, HashMap<String, PathInfo>>> getLogicalHashTable() {
		return TestLogicalMappingTable.entrySet();
	}

	public void showLogicalHashTable() {
		System.out.println(TestLogicalMappingTable.entrySet());
	}
	
	public HashMap<String, HashMap<String, PathInfo>> getLogicalMappingTable() {
		return TestLogicalMappingTable;
	}
	
	public void TestlogicalMappingDownload() {
		try {
			// ----------------------------declare for
			// writing--------------------------
			// We don't need "DataOutputStream"
			File fout = new File("FedFSImage/LHashTable.txt");
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			// ----------------------------write into
			// file.txt---------------------------
			for (String key : TestLogicalMappingTable.keySet()) {
				bw.write(key + " --> " + TestLogicalMappingTable.get(key) + "\n");
			}
			// bw.write(PhysicalMappingTable.entrySet().toString());
			bw.flush();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}

		System.out.println("\n[INFO] TestLogical Table download successful");
	}

}