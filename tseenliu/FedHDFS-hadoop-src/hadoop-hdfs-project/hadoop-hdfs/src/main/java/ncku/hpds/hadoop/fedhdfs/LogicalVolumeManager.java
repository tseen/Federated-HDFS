package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;

public class LogicalVolumeManager {

	private static HashMap<String, HashMap<String, String>> LogicalMappingTable = new HashMap<String, HashMap<String, String>>();

	// private static HashMap<String, String> innerMap;

	public void addElementsToLTable(String logicalName, String hostName,
			String path) {

		if (LogicalMappingTable.containsKey(logicalName)) {

			while (LogicalMappingTable.get(logicalName).containsKey(hostName)) {
				LogicalMappingTable.get(logicalName).remove(hostName);
				break;
			}
			LogicalMappingTable.get(logicalName).put(hostName, path);

		} else {

			LogicalMappingTable.put(logicalName, new HashMap<String, String>());

			while (LogicalMappingTable.get(logicalName).containsKey(hostName)) {
				LogicalMappingTable.get(logicalName).remove(hostName);
				break;
			}
			LogicalMappingTable.get(logicalName).put(hostName, path);
		}

		/*
		 * if (innerMap == null){ innerMap = new HashMap<String, String>();
		 * LogicalMappingTable.put(logicalName, innerMap); }
		 * 
		 * LogicalMappingTable.get(logicalName).put("c02",
		 * "/user/hpds/Directory1");
		 * 
		 * innerMap.put(hostName, path); innerMap.put("c34",
		 * "/user/hpds/Folder2/file.txt"); innerMap.put("c33",
		 * "/user/hpds/Folder1"); innerMap.put("c02", "/user/hpds/Directory1");
		 * innerMap.put("c09", "/user/hpds/Folder/Directory1");
		 * innerMap.put("c13", "/user/hpds/Folder2/Directory2");
		 * innerMap.put("c19", "/user/hpds/Folder3/Directory4");
		 */

	}

	public void showHashTable() {
		System.out.println(LogicalMappingTable.entrySet());
	}

	public void hashTableDownload() {
		try {
			// ----------------------------declare for
			// writing--------------------------
			// We don't need "DataOutputStream"
			File fout = new File("FedFSImage/LHashTable.txt");
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			// ----------------------------write into
			// file.txt---------------------------
			for (String key : LogicalMappingTable.keySet()) {
				bw.write(key + " --> " + LogicalMappingTable.get(key) + "\n");
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

		System.out.println("\n[INFO] Logical Table download successful .");
	}

}