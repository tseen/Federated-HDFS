package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

public class LogicalVolumeManager implements Serializable {

	private HashMap<String, HashMap<String, String>> LogicalMappingTable = new HashMap<String, HashMap<String, String>>();

	public void putLogicalTable(String globalFileName, String hostName,
			String path) {

		if (LogicalMappingTable.containsKey(globalFileName)) {

			while (LogicalMappingTable.get(globalFileName).containsKey(hostName)) {
				LogicalMappingTable.get(globalFileName).remove(hostName);
				break;
			}
			LogicalMappingTable.get(globalFileName).put(hostName, path);

		} else {

			LogicalMappingTable.put(globalFileName, new HashMap<String, String>());

			while (LogicalMappingTable.get(globalFileName).containsKey(hostName)) {
				LogicalMappingTable.get(globalFileName).remove(hostName);
				break;
			}
			LogicalMappingTable.get(globalFileName).put(hostName, path);
		}

	}

	public void checkGlobalFileName(String globalFileName) {
		System.out.println(LogicalMappingTable.containsKey(globalFileName));
	}

	public boolean showCheckGlobalFileName(String globalFileName) {
		return LogicalMappingTable.containsKey(globalFileName);
	}

	public void checkAfedcluster(String globalFileName, String hostname) {
		System.out.println(LogicalMappingTable.get(globalFileName).containsKey(
				hostname));
	}

	public boolean showCheckAfedcluster(String globalFileName, String hostname) {
		return LogicalMappingTable.get(globalFileName).containsKey(hostname);
	}

	public boolean checkAphysicalPath(String globalFileName, String physicalPath) {
		return LogicalMappingTable.get(globalFileName).containsValue(physicalPath);
	}

	public void showAfedPhysicalPath(String globalFileName, String hostname) {
		System.out.println(LogicalMappingTable.get(globalFileName).get(hostname));
	}

	public Set<Entry<String, HashMap<String, String>>> getLogicalHashTable() {
		return LogicalMappingTable.entrySet();
	}

	public void showLogicalHashTable() {
		System.out.println(LogicalMappingTable.entrySet());
	}
	
	public HashMap<String, HashMap<String, String>> getLogicalMappingTable() {
		return LogicalMappingTable;
	}

	public void logicalMappingDownload() {
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