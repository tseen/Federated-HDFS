package fbicloud.algorithm.classes;

import org.apache.hadoop.io.Text;

public class DataObject extends Text{
	boolean visit = false;
	int cid = 0;
	String stime, pair, IPN, OPN, TPN, IBT, OBT, TBT, DUR, sFLN, sFLR;
	String gid, address;
	
	public DataObject(){
		setStime("none");
		setPair("none");
		setIPN("none");
		setOPN("none");
		setTPN("none");
		setIBT("none");
		setOBT("none");
		setTBT("none");
		setDUR("none");
		setsFLN("none");
		setsFLR("none");
	}
	
	public DataObject(String stime, String pair, String ipn, String opn, String tpn, String ibt, String obt, String tbt, String dur, String sfln, String sflr){
		setStime(stime);
		setPair(pair);
		setIPN(ipn);
		setOPN(opn);
		setTPN(tpn);
		setIBT(ibt);
		setOBT(obt);
		setTBT(tbt);
		setDUR(dur);
		setsFLN(sfln);
		setsFLR(sflr);
	}
	
	public void setFlow(String stime, String pair, String ipn, String opn, String tpn, String ibt, String obt, String tbt, String dur, String sfln, String sflr){
		this.stime = stime;
		this.pair = pair;
		this.IPN = ipn;
		this.OPN = opn;
		this.TPN = tpn;
		this.IBT = ibt;
		this.OBT = obt;
		this.TBT = tbt;
		this.DUR = dur;
		this.sFLN = sfln;
		this.sFLR = sflr;
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
	public String getVector(){
		return stime + " " + pair + " " + IPN + " " + OPN + " " + TPN + " " + IBT + " " + OBT + " " + TBT + " "+ DUR + " " + sFLN + " " + sFLR;
	}
	
	public void setStime(String stime){
		this.stime = stime;
	}
	public void setPair(String pair){
		this.pair = pair;
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
	public void setsFLN(String sFLN){
		this.sFLN = sFLN;
	}
	public void setsFLR(String sFLR){
		this.sFLR = sFLR;
	}
}
