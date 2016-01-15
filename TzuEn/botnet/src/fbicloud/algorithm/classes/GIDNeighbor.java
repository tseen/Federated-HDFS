package fbicloud.algorithm.classes;

public class GIDNeighbor {
	private int GID;
	private String neighbor;

	//GID-1001    GID-403:0.00,1.00,1.00,0.00,135.50,135.50,0.00,1.00,1.00,11673.84;GID-403:0.00,1.00,1.00,0.00,135.50,135.50,0.00,1.00,1.00,11673.84
	public GIDNeighbor(int gID,String str){
		this.GID = gID;
		this.neighbor = str;
	}
	public int getGID(){
		return this.GID;
	}
	public String getNeighbor(){
		return this.neighbor;
	}
}
