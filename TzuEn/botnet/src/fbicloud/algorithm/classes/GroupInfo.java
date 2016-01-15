package fbicloud.algorithm.classes;

public class GroupInfo {
	int ID;
	boolean isMerge;
	String feature;
	String srcIPs;
	String dstIPs;
	
	public GroupInfo(int gID, String gfeature, String gSrcIPs, String gDstIPs, boolean gIsMerge){
		this.ID = gID;
		this.feature = gfeature;
		this.srcIPs = gSrcIPs;
		this.dstIPs = gDstIPs;
		this.isMerge = gIsMerge;
	}
	public void setIsMerge(boolean gIsMerge){
		this.isMerge = gIsMerge;
	}
	public int getID(){
		return ID;
	}
	public String getFeature(){
		return feature;
	}
	public String getSrcIPs(){
		return srcIPs;
	}
	public String getDstIPs(){
		return dstIPs;
	}
	public boolean getIsMerge(){
		return isMerge;
	}
}
