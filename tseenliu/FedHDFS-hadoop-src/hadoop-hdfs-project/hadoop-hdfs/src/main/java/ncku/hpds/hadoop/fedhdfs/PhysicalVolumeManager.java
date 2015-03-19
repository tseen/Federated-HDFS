package ncku.hpds.hadoop.fedhdfs;

import java.awt.List;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.*;
import org.w3c.dom.Element;

import java.io.*;
import java.util.*;

public class PhysicalVolumeManager {
	
	private Vector<ArrayList<String>> fsElements = new Vector<ArrayList<String>>();
	private ArrayList<String> tmpFsElement;
	private final HashMap<String, String> PhysicalMappingTable = new HashMap<String, String>();
	
	public void addfsElementToArrayLists(String Uri, Configuration conf) throws IOException {
		tmpFsElement = new ArrayList<String>();
		makefsElement(Uri, conf);
		fsElements.addElement(tmpFsElement);
	}
	
	private void makefsElement(String Uri, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(Uri));
		
		for (int i = 0; i < status.length; i++) {
			if (status[i].isDirectory()) {
				tmpFsElement.add(status[i].getPath().toString());
				makefsElement(status[i].getPath().toString(), conf);
			} else{
				tmpFsElement.add(status[i].getPath().toString());
			}
		}
	}
	
	public void updataPTable(Vector<Element> theElements){
		int VectorSize = fsElements.size();
		//System.out.println("\nThe size of the VectorSize : " + VectorSize);
		
		for (int i = 0; i < VectorSize; i++){
			for (int j = 0; j < fsElements.get(i).size(); j++){
				PhysicalMappingTable.put(fsElements.get(i).get(j), FedHdfsConParser.getValue("HostName",
						theElements.elementAt(i)));
			}
		}
		
		/*String item0 = fsElements.get(0);
		String item1 = fsElements.get(1);
		String item25 = fsElements.get(25);
		System.out.println("The item is the index 0 is: " + item0);
		System.out.println("The item is the index 1 is: " + item1);
		System.out.println("The item is the index 25 is: " + item25);*/
		
		//PhysicalMappingTable.put(fsElements.get(size), "1");
	}
	
	public void ShowAllfsElements(){
		System.out.println(fsElements);	
	}
	
	public void ShowVectorSize(){
		int VectorSize = fsElements.size();
		System.out.println("\n[INFO] Number of Federated Clusters : " + VectorSize);
	}
	
	public int DShowVectorSize(){
		int VectorSize = fsElements.size();
		System.out.println("\nThe size of the VectorSize : " + VectorSize);
		return VectorSize;
	}
	
	public void ShowHashTableSize(){
		int VectorSize = fsElements.size();
		int HTelements = 0;
		for (int i = 0; i < VectorSize; i++){
			HTelements += fsElements.get(i).size();	
			}
		System.out.println("\n[INFO] Number of the HashTableElements : " + HTelements);
	}
	
	public int DShowHashTableSize(){
		int VectorSize = fsElements.size();
		int HTelements = 0;
		for (int i = 0; i < VectorSize; i++){
			HTelements += fsElements.get(i).size();	
			}
		return HTelements;
	}
	
	public void ShowHashTable(){
		System.out.println(PhysicalMappingTable.entrySet());
	}
	
	public Set<Entry<String, String>> DShowHashTable(){
		return PhysicalMappingTable.entrySet();
	}
	
	public void checkAclusterPaths(String hostName){
		System.out.println(PhysicalMappingTable.containsValue(hostName));
	}
	
	public boolean DcheckAclusterPaths(String hostName){
		return PhysicalMappingTable.containsValue(hostName);
	}
	
	public void checkAhostName(String pathName){
		System.out.println(PhysicalMappingTable.containsKey(pathName));
	}
	
	public boolean DcheckAhostName(String pathName){
		return PhysicalMappingTable.containsKey(pathName);
	}
	
	public void getAhostName(String pathName){
		System.out.println(PhysicalMappingTable.get(pathName));
	}
	
	public String DgetAhostName(String pathName){
		return PhysicalMappingTable.get(pathName);
	}
	
	public void HashTableDownload(){
		try{
			//----------------------------declare for writing--------------------------
			//We don't need "DataOutputStream"
			File fout = new File("FedFSImage/HashTable.txt");
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			//----------------------------write into file.txt---------------------------
			for (String key: PhysicalMappingTable.keySet()){
				bw.write(key  + " --> "+ PhysicalMappingTable.get(key) + "\n");
			}
			//bw.write(PhysicalMappingTable.entrySet().toString());
			bw.flush();	
		}catch (FileNotFoundException ex) {
			ex.printStackTrace();
		}catch (IOException ex) {
			ex.printStackTrace();
		}catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
		
		System.out.println("\n[INFO] Physical Table download successful .");
	} 
	
	
	
	/*public static void updateTable(String Uri, Configuration conf, String hostName) throws IOException {
		 
		 FileSystem fs = FileSystem.get(conf);
		 FileStatus[] status = fs.listStatus(new Path(Uri));
		 
		for (int i = 0; i < status.length; i++) {
			
			if (status[i].isDirectory()) {
				
				//System.out.println(status[i].getPath().toString());
				
				PhysicalMappingTable.put(status[i].getPath().toString(), hostName);
				
				updateTable(status[i].getPath().toString(), conf, hostName);
				
            } else {
                    //System.out.println(status[i].getPath().toString());
                    
                    PhysicalMappingTable.put(status[i].getPath().toString(), hostName);
                    
                } 
			
			//System.out.println(status[i].getPath().toString());
			//PhysicalMappingTable.put(status[i].getPath().toString(), hostName);
			
			
		 }
		
		System.out.println(PhysicalMappingTable.keySet());

		 
	 }*/

}



