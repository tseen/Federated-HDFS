package fbicloud.utils ;

import org.apache.hadoop.io.Text ;


public class FlowGroupFS extends Text
{
	boolean visit = false ;
	int cid = 0 ;
	
	String timestamp, srcIP, dstIP;
	String S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean; 
	String D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean; 
	String ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean, ToT_Byte_STD; 
	String ToT_Prate, ToT_Brate, ToT_BTransferRatio ,DUR ,Loss;
	
	double[] FGFeatures = new double[21];
	
	public FlowGroupFS
	(String timestamp, String srcip ,String dstip, 
	 String S2D_noP, String S2D_noB, String S2D_Byte_Max, String S2D_Byte_Min, String S2D_Byte_Mean,
	 String D2S_noP, String D2S_noB, String D2S_Byte_Max, String D2S_Byte_Min, String D2S_Byte_Mean,
	 String ToT_noP, String ToT_noB, String ToT_Byte_Max, String ToT_Byte_Min, String ToT_Byte_Mean, String ToT_Byte_STD,
	 String ToT_Prate, String ToT_Brate, String ToT_BTransferRatio, String DUR, String Loss  ){
		this.timestamp = timestamp;
		this.srcIP = srcip;
		this.dstIP = dstip;
		
		this.S2D_noP = S2D_noP;
		this.S2D_noB = S2D_noB;
		this.S2D_Byte_Max = S2D_Byte_Max;
		this.S2D_Byte_Min = S2D_Byte_Min;
		this.S2D_Byte_Mean = S2D_Byte_Mean;
		
		this.D2S_noP = D2S_noP;
		this.D2S_noB = D2S_noB;
		this.D2S_Byte_Max = D2S_Byte_Max;
		this.D2S_Byte_Min = D2S_Byte_Min;
		this.D2S_Byte_Mean = D2S_Byte_Mean;
		
		this.ToT_noP = ToT_noP;
		this.ToT_noB = ToT_noB;
		this.ToT_Byte_Max = ToT_Byte_Max;
		this.ToT_Byte_Min = ToT_Byte_Min;
		this.ToT_Byte_Mean = ToT_Byte_Mean;
		this.ToT_Byte_STD = ToT_Byte_STD;
		
		this.ToT_Prate = ToT_Prate;
		this.ToT_Brate = ToT_Brate;
		this.ToT_BTransferRatio = ToT_BTransferRatio;
		this.DUR = DUR;
		this.Loss = Loss;	
		
		FGFeatures[0] = Double.parseDouble(S2D_noP);
		FGFeatures[1] = Double.parseDouble(S2D_noB);
		FGFeatures[2] = Double.parseDouble(S2D_Byte_Max);
		FGFeatures[3] = Double.parseDouble(S2D_Byte_Min);
		FGFeatures[4] = Double.parseDouble(S2D_Byte_Mean);
		
		FGFeatures[5] = Double.parseDouble(D2S_noP);
		FGFeatures[6] = Double.parseDouble(D2S_noB);
		FGFeatures[7] = Double.parseDouble(D2S_Byte_Max);
		FGFeatures[8] = Double.parseDouble(D2S_Byte_Min);
		FGFeatures[9] = Double.parseDouble(D2S_Byte_Mean);
		
		FGFeatures[10] = Double.parseDouble(ToT_noP);
		FGFeatures[11] = Double.parseDouble(ToT_noB);
		FGFeatures[12] = Double.parseDouble(ToT_Byte_Max);
		FGFeatures[13] = Double.parseDouble(ToT_Byte_Min);
		FGFeatures[14] = Double.parseDouble(ToT_Byte_Mean);
		FGFeatures[15] = Double.parseDouble(ToT_Byte_STD);
		
		FGFeatures[16] = Double.parseDouble(ToT_Prate);
		FGFeatures[17] = Double.parseDouble(ToT_Brate);
		FGFeatures[18] = Double.parseDouble(ToT_BTransferRatio);
		FGFeatures[19] = Double.parseDouble(DUR);
		FGFeatures[20] = Double.parseDouble(Loss);
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
	
	public double[] getFeatures(){
		return  FGFeatures;
	}
	
	public String getVector(){
		return  timestamp + "," +
		   		S2D_noP + "," + S2D_noB + "," + S2D_Byte_Max + "," + S2D_Byte_Min + "," + S2D_Byte_Mean + "," +
		   		D2S_noP + "," + D2S_noB + "," + D2S_Byte_Max + "," + D2S_Byte_Min + "," + D2S_Byte_Mean + "," +
		   		ToT_noP + "," + ToT_noB + "," + ToT_Byte_Max + "," + ToT_Byte_Min + "," + ToT_Byte_Mean + "," + ToT_Byte_STD + "," +
				ToT_Prate + "," + ToT_Brate + "," + ToT_BTransferRatio + "," + DUR + "," + Loss;
	}
}
