package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

public class LogicalVolumeManager {

	private static HashMap<String, HashMap<String, String>> LogicalMappingTable = new HashMap<String, HashMap<String, String>>();

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

	}

	public void checkAlogicalFile(String logicalFile) {
		System.out.println(LogicalMappingTable.containsKey(logicalFile));
	}

	public boolean DcheckAlogicalFile(String logicalFile) {
		return LogicalMappingTable.containsKey(logicalFile);
	}

	public void checkAfedcluster(String logicalFile, String hostname) {
		System.out.println(LogicalMappingTable.get(logicalFile).containsKey(
				hostname));
	}

	public boolean DcheckAfedcluster(String logicalFile, String hostname) {
		return LogicalMappingTable.get(logicalFile).containsKey(hostname);
	}

	public boolean checkAfedPhysicalPath(String logicalFile, String physicalPath) {
		return LogicalMappingTable.get(logicalFile).containsValue(physicalPath);
	}

	public void showAfedPhysicalPath(String logicalFile, String hostname) {
		System.out.println(LogicalMappingTable.get(logicalFile).get(hostname));
	}

	public Set<Entry<String, HashMap<String, String>>> DShowHashTable() {
		return LogicalMappingTable.entrySet();
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