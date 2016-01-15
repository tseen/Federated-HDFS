package fbicloud.algorithm.classes;

import java.util.Collection;

public class FVInfo {
	int ID;
	Collection<String> srcIPsCol;
	
	public FVInfo(int fvID, Collection<String> fvSrcIPs){
		this.ID = fvID;
		this.srcIPsCol = fvSrcIPs;
	}
	public int getID(){
		return ID;
	}
	public Collection<String> getSrcIPsCol(){
		return srcIPsCol;
	}
}
