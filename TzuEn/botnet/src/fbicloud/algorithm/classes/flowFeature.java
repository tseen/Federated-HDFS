package fbicloud.algorithm.classes;
import org.apache.hadoop.io.Text;

public class flowFeature extends Text{
	boolean visit = false;
	int cid = 0;
	String srcIP,dstIP,IPN, OPN, TPN, IBT, OBT, TBT, DUR, sFLN, sFLR;
	
	public flowFeature(){
		setSrcIP("none");
		setDstIP("none");
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
	
	public flowFeature(String srcip,String dstip, String ipn, String opn, String tpn, String ibt, String obt, String tbt, String dur, String sfln, String sflr){
		setSrcIP(srcip);
		setDstIP(dstip);
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
	
	public void setFlow(String srcip, String dstip, String ipn, String opn, String tpn, String ibt, String obt, String tbt, String dur, String sfln, String sflr){
		this.srcIP = srcip;
		this.dstIP = dstip;
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
	public String getFeatures(){
		return  IPN + "," + OPN + "," + TPN + "," + 
				IBT + "," + OBT + "," + TBT + "," +
			   	DUR + "," + sFLN + "," + sFLR;
	}
	public String getVector(){
		return  IPN + "," + OPN + "," + TPN + "," + 
				IBT + "," + OBT + "," + TBT + "," +
			   	DUR + "," + sFLN + "," + sFLR + "\t" + srcIP + "\t" + dstIP;
	}
	
	public void setDstIP(String dstip){
		this.dstIP = dstip;
	}
	public void setSrcIP(String srcip){
		this.srcIP = srcip;
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
