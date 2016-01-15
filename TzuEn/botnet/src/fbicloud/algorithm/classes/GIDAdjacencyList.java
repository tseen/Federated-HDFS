package fbicloud.algorithm.classes;

import java.util.*;

public class GIDAdjacencyList {
	int GID;
	ArrayList<String> neighborList = new ArrayList<String>();
	
	//GID-1001    GID-403:0.00,1.00,1.00,0.00,135.50,135.50,0.00,1.00,1.00,11673.84;GID-403:0.00,1.00,1.00,0.00,135.50,135.50,0.00,1.00,1.00,11673.84
	public GIDAdjacencyList(int gID,String str){
		String [] tmpStr = str.split(";");
		this.GID = gID;
		for(int i=0;i<tmpStr.length;i++ ){
			this.neighborList.add(tmpStr[i]);
		}
	}
	public int getGID(){
		return this.GID;
	}
	public int getNeighborNumber(){
		return neighborList.size();
	}
	public int getGIDByIdx(int index){
		return Integer.parseInt(neighborList.get(index).split(":")[0].split("-")[1]);
	}
	public String getFeatureByIdx(int index){
		return neighborList.get(index).split(":")[1];
	}
}
