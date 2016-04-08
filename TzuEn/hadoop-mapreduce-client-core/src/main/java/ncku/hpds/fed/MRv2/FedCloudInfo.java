/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.util.HashMap;
import java.util.Map;

public class FedCloudInfo {
	private long inputSize;
	private String cloudName = "";
	private int regionMapTime;
	private int regionMapStartTime;
	private int wanSpeedCount = 0;
	private double wanSpeed;
	private Map<String, Double> wanSpeedMap = new HashMap<String, Double>();

	public FedCloudInfo(String Name){
		cloudName = Name;
	}

	public long getInputSize() {
		return inputSize;
	}

	public void setInputSize(long inputSize) {
		this.inputSize = inputSize;
	}

	public String getCloudName() {
		return cloudName;
	}

	public void setCloudName(String cloudName) {
		this.cloudName = cloudName;
	}

	public int getRegionMapTime() {
		return regionMapTime;
	}
	
	public void setRegionMapStartTime(int r) {
		this.regionMapStartTime = r;
		//System.out.println(this.cloudName+" time:"+this.regionMapTime+" size:"+this.inputSize);
	}
	public void setRegionMapTime(int regionMapTime) {
		this.regionMapTime = regionMapTime;
		System.out.println(this.cloudName+" time:"+this.regionMapTime+"-"+this.regionMapStartTime+" size:"+this.inputSize);
		System.out.println(this.cloudName+" speed:"+(double)((double)this.inputSize/((double)this.regionMapTime-(double)this.regionMapStartTime)));
	}

	public double getWanSpeed() {
		return wanSpeed;
	}

	public void setWanSpeed(float wanSpeed) {
		this.wanSpeed = wanSpeed;
		System.out.println(this.cloudName+" WAN speed:"+ this.wanSpeed);
	}

	public Map<String, Double> getWanSpeedMap() {
		return wanSpeedMap;
	}

	public void setWanSpeedMap(Map<String, Double> wanSpeedMap) {
		this.wanSpeedMap = wanSpeedMap;
	}
	public void setWanSpeed(String dest, double speed){
		//System.out.println("WAN speed:"+cloudName+"->"+dest+":"+speed);
		wanSpeedMap.put(dest, speed);
		wanSpeedCount++;
	}
	
	public void printWanSpeed(Map<String, Double> downSpeed){
		for (Map.Entry<String, Double> wan : wanSpeedMap
				.entrySet()) {
			System.out.println("CollectAllWAN:"+cloudName+">"+wan.getKey()+"="+wan.getValue());
			if(!cloudName.equals(wan.getKey())){
				Double previousValue =  downSpeed.get(wan.getKey());
				if(previousValue == null) 
					previousValue =  0d ;
				downSpeed.put(wan.getKey(), previousValue + wan.getValue());
			}
		}
	}

	public int getWanSpeedCount() {
		return wanSpeedCount;
	}

}
