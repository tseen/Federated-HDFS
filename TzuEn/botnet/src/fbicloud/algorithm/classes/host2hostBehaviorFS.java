package fbicloud.algorithm.classes;

import org.apache.hadoop.io.Text;

public class host2hostBehaviorFS extends Text{
	boolean isEmit = false;
	boolean visit = false;
	int cid = 0;
	
	String dstIP;
	String S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean; 
	String D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean; 
	String ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean, ToT_Byte_STD; 
	String ToT_Prate, ToT_Brate, ToT_BTransferRatio ,DUR ,Loss;
	String IAT_Max, IAT_Min, IAT_Mean, IAT_STD;

	double[] Features = new double[24];

	public host2hostBehaviorFS
	(String dstip,
	 String S2D_noP, String S2D_noB, String S2D_Byte_Max, String S2D_Byte_Min, String S2D_Byte_Mean,
	 String D2S_noP, String D2S_noB, String D2S_Byte_Max, String D2S_Byte_Min, String D2S_Byte_Mean,
	 String ToT_noP, String ToT_noB, String ToT_Byte_Max, String ToT_Byte_Min, String ToT_Byte_Mean, String ToT_Byte_STD,
	 String ToT_Prate, String ToT_Brate, String ToT_BTransferRatio, String DUR,
	 String IAT_Max,String IAT_Min,String IAT_Mean,String IAT_STD ){
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
		 
		this.IAT_Max = IAT_Max;
		this.IAT_Min = IAT_Min;
		this.IAT_Mean = IAT_Mean;
		this.IAT_STD = IAT_STD;		 

		Features[0] = Double.parseDouble(S2D_noP);
		Features[1] = Double.parseDouble(S2D_noB);
		Features[2] = Double.parseDouble(S2D_Byte_Max);
		Features[3] = Double.parseDouble(S2D_Byte_Min);
		Features[4] = Double.parseDouble(S2D_Byte_Mean);
		
		Features[5] = Double.parseDouble(D2S_noP);
		Features[6] = Double.parseDouble(D2S_noB);
		Features[7] = Double.parseDouble(D2S_Byte_Max);
		Features[8] = Double.parseDouble(D2S_Byte_Min);
		Features[9] = Double.parseDouble(D2S_Byte_Mean);
		
		Features[10] = Double.parseDouble(ToT_noP);
		Features[11] = Double.parseDouble(ToT_noB);
		Features[12] = Double.parseDouble(ToT_Byte_Max);
		Features[13] = Double.parseDouble(ToT_Byte_Min);
		Features[14] = Double.parseDouble(ToT_Byte_Mean);
		Features[15] = Double.parseDouble(ToT_Byte_STD);
		
		Features[16] = Double.parseDouble(ToT_Prate);
		Features[17] = Double.parseDouble(ToT_Brate);
		Features[18] = Double.parseDouble(ToT_BTransferRatio);
		Features[19] = Double.parseDouble(DUR);
	
		Features[20] = Double.parseDouble(IAT_Max);
		Features[21] = Double.parseDouble(IAT_Min);
		Features[22] = Double.parseDouble(IAT_Mean);
		Features[23] = Double.parseDouble(IAT_STD);
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

	public double[] getFeatures(){
		return  Features;
	}
	public String getVector(){
		return  dstIP + "\t" + 
		   		S2D_noP + "," + S2D_noB + "," + S2D_Byte_Max + "," + S2D_Byte_Min + "," + S2D_Byte_Mean + "," +
		   		D2S_noP + "," + D2S_noB + "," + D2S_Byte_Max + "," + D2S_Byte_Min + "," + D2S_Byte_Mean + "," +
		   		ToT_noP + "," + ToT_noB + "," + ToT_Byte_Max + "," + ToT_Byte_Min + "," + ToT_Byte_Mean + "," + ToT_Byte_STD + "," +
				ToT_Prate + "," + ToT_Brate + "," + ToT_BTransferRatio + "," + DUR + "," +
				IAT_Max + "," + IAT_Min + "," + IAT_Mean + "," + IAT_STD;
	}
}
