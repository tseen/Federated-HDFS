package fbicloud.utils ;

import org.apache.hadoop.io.Text ;


public class HostBehaviorFS extends Text
{
	boolean visit = false ;
	int cid = 0 ;

	String timestamp, srcIP, dstIPs ;
	String srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean ;
	String dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean ;
	String total_NumOfPkts, total_NumOfBytes, total_Byte_Max, total_Byte_Min, total_Byte_Mean, total_Byte_STD ;
	String total_PktsRate, total_BytesRate, total_BytesTransferRatio, duration ;
	
	double[] features = new double[20] ;
	
	public HostBehaviorFS(	String timestamp, String srcIP, String dstIPs,
							String srcToDst_NumOfPkts, String srcToDst_NumOfBytes, String srcToDst_Byte_Max, String srcToDst_Byte_Min, String srcToDst_Byte_Mean,
							String dstToSrc_NumOfPkts, String dstToSrc_NumOfBytes, String dstToSrc_Byte_Max, String dstToSrc_Byte_Min, String dstToSrc_Byte_Mean,
							String total_NumOfPkts, String total_NumOfBytes, String total_Byte_Max, String total_Byte_Min, String total_Byte_Mean, String total_Byte_STD,
							String total_PktsRate, String total_BytesRate, String total_BytesTransferRatio, String duration )
	{
		this.timestamp = timestamp ;
		this.srcIP = srcIP ;
		this.dstIPs = dstIPs ;
		
		this.srcToDst_NumOfPkts = srcToDst_NumOfPkts ;
		this.srcToDst_NumOfBytes = srcToDst_NumOfBytes ;
		this.srcToDst_Byte_Max = srcToDst_Byte_Max ;
		this.srcToDst_Byte_Min = srcToDst_Byte_Min ;
		this.srcToDst_Byte_Mean = srcToDst_Byte_Mean ;
		
		this.dstToSrc_NumOfPkts = dstToSrc_NumOfPkts ;
		this.dstToSrc_NumOfBytes = dstToSrc_NumOfBytes ;
		this.dstToSrc_Byte_Max = dstToSrc_Byte_Max ;
		this.dstToSrc_Byte_Min = dstToSrc_Byte_Min ;
		this.dstToSrc_Byte_Mean = dstToSrc_Byte_Mean ;
		
		this.total_NumOfPkts = total_NumOfPkts ;
		this.total_NumOfBytes = total_NumOfBytes ;
		this.total_Byte_Max = total_Byte_Max ;
		this.total_Byte_Min = total_Byte_Min ;
		this.total_Byte_Mean = total_Byte_Mean ;
		this.total_Byte_STD = total_Byte_STD ;
		
		this.total_PktsRate = total_PktsRate ;
		this.total_BytesRate = total_BytesRate ;
		this.total_BytesTransferRatio = total_BytesTransferRatio ;
		this.duration = duration ;
		
		features[0] = Double.parseDouble(srcToDst_NumOfPkts) ;
		features[1] = Double.parseDouble(srcToDst_NumOfBytes) ;
		features[2] = Double.parseDouble(srcToDst_Byte_Max) ;
		features[3] = Double.parseDouble(srcToDst_Byte_Min) ;
		features[4] = Double.parseDouble(srcToDst_Byte_Mean) ;
		
		features[5] = Double.parseDouble(dstToSrc_NumOfPkts) ;
		features[6] = Double.parseDouble(dstToSrc_NumOfBytes) ;
		features[7] = Double.parseDouble(dstToSrc_Byte_Max) ;
		features[8] = Double.parseDouble(dstToSrc_Byte_Min) ;
		features[9] = Double.parseDouble(dstToSrc_Byte_Mean) ;
		
		features[10] = Double.parseDouble(total_NumOfPkts) ;
		features[11] = Double.parseDouble(total_NumOfBytes) ;
		features[12] = Double.parseDouble(total_Byte_Max) ;
		features[13] = Double.parseDouble(total_Byte_Min) ;
		features[14] = Double.parseDouble(total_Byte_Mean) ;
		features[15] = Double.parseDouble(total_Byte_STD) ;
		
		features[16] = Double.parseDouble(total_PktsRate) ;
		features[17] = Double.parseDouble(total_BytesRate) ;
		features[18] = Double.parseDouble(total_BytesTransferRatio) ;
		features[19] = Double.parseDouble(duration) ;
	}
	
	public void setVisited(boolean flg)
	{
		visit = flg ;
	}
	
	public boolean isVisited()
	{
		return visit ;
	}
	
	public void setCid(int value)
	{
		cid = value ;
	}
	public int getCid()
	{
		return cid ;
	}
	
	public double[] getFeatures()
	{
		return  features;
	}
	
	public String getFVString()
	{
		return  timestamp + "\t" + srcIP + "\t" + dstIPs + "\t" + 
				srcToDst_NumOfPkts + "," + srcToDst_NumOfBytes + "," + srcToDst_Byte_Max + "," + srcToDst_Byte_Min + "," + srcToDst_Byte_Mean + "," +
		   		dstToSrc_NumOfPkts + "," + dstToSrc_NumOfBytes + "," + dstToSrc_Byte_Max + "," + dstToSrc_Byte_Min + "," + dstToSrc_Byte_Mean + "," +
		   		total_NumOfPkts + "," + total_NumOfBytes + "," + total_Byte_Max + "," + total_Byte_Min + "," + total_Byte_Mean + "," + total_Byte_STD + "," +
				total_PktsRate + "," + total_BytesRate + "," + total_BytesTransferRatio + "," + duration ;
	}
	
	public String getSrcIP()
	{
		return srcIP ;
	}
}
