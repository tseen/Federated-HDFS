package fbicloud.algorithm.classes;

import org.apache.hadoop.io.Text;

public class host2hostBehavior extends Text{
	boolean isEmit = false;
	boolean visit = false;
	int cid = 0;
	
	String dstIP,IPN, OPN, TPN, IBT, OBT, TBT, DUR, ATI, STD;

	public host2hostBehavior(String dstip, String ipn, String opn, String tpn, String ibt, String obt, String tbt, String dur, String ati, String std){
		this.dstIP = dstip;
		this.IPN = ipn;
		this.OPN = opn;
		this.TPN = tpn;
		this.IBT = ibt;
		this.OBT = obt;
		this.TBT = tbt;
		this.DUR = dur;
		this.ATI = ati;
		this.STD = std;
	}

	public void setVisited(boolean flg){
		visit = flg;
	}
	public boolean isVisited(){
		return visit;
	}
	public void setCid(int value){
		cid = value;
	}
	public int getCid(){
		return cid;
	}

	public void setEmit(boolean ismit){
		isEmit = ismit;
	}
	public boolean isEmit(){
		return isEmit;
	}

	public String getFeatures(){
		return  IPN + "," + OPN + "," + TPN + "," + 
				IBT + "," + OBT + "," + TBT + "," +
			   	DUR + "," + ATI + "," + STD;
	}
	public String getVector(){
		return  dstIP + "\t" + 
				IPN + "," + OPN + "," + TPN + "," + 
				IBT + "," + OBT + "," + TBT + "," +
			   	DUR + "," + ATI + "," + STD;
	}
}
