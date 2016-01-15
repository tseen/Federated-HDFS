package fbicloud.algorithm.classes;

import org.apache.hadoop.io.Text;

public class flowGroup extends Text{
	boolean isEmit = false;
	boolean visit = false;
	int cid = 0;
	long first = 0;
	String srcIP,dstIP,IPN, OPN, TPN, IBT, OBT, TBT, DUR, LOSS;

	public flowGroup(long first, String srcip ,String dstip, String ipn, String opn, String tpn, String ibt, String obt, String tbt, String dur, String loss){
		setFirst(first);
		setSrcIP(srcip);
		setDstIP(dstip);
		setIPN(ipn);
		setOPN(opn);
		setTPN(tpn);
		setIBT(ibt);
		setOBT(obt);
		setTBT(tbt);
		setDUR(dur);
		setLOSS(loss);
	}

	public boolean isVisited(){
		return visit;
	}
	public void setVisited(boolean flg){
		visit = flg;
	}

	public int getCid(){
		return cid;
	}
	public void setCid(int value){
		cid = value;
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
			   	DUR + "," + LOSS;
	}
	public String getVector(){
		return  first + "," + 
				IPN + "," + OPN + "," + TPN + "," + 
				IBT + "," + OBT + "," + TBT + "," +
			   	DUR + "," + LOSS;
	}
	
	public void setFirst(long first){
		this.first = first;
	}
	public void setSrcIP(String srcip){
		this.srcIP = srcip;
	}
	public void setDstIP(String dstip){
		this.dstIP = dstip;
	}
	public void setIPN(String IPN){
		this.IPN = IPN;
	}
	public void setOPN(String OPN){
		this.OPN = OPN;
	}
	public void setTPN(String TPN){
		this.TPN = TPN;
	}
	public void setIBT(String IBT){
		this.IBT = IBT;
	}
	public void setOBT(String OBT){
		this.OBT = OBT;
	}
	public void setTBT(String TBT){
		this.TBT = TBT;
	}
	public void setDUR(String DUR){
		this.DUR = DUR;
	}
	public void setLOSS(String LOSS){
		this.LOSS = LOSS;
	}
}
