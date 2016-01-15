package fbicloud.algorithm.classes;

import org.apache.hadoop.io.Text;

public class hostBehavior extends Text{
	boolean isEmit = false;
	boolean visit = false;
	int cid = 0;
	
	String srcIP,dstIPs,IPN, OPN, TPN, IBT, OBT, TBT, DUR, ATI, STD;

	public hostBehavior(String srcip,String dstips, String ipn, String opn, String tpn, String ibt, String obt, String tbt, String dur, String ati, String std){
		this.srcIP = srcip;
		this.dstIPs = dstips;
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
	public String getVector(){//Feature Vector	srcIP	dstIPs
		return  IPN + "," + OPN + "," + TPN + "," + 
				IBT + "," + OBT + "," + TBT + "," +
			   	DUR + "," + ATI + "," + STD + "\t"+
				srcIP + "\t" + dstIPs;
	}
}
