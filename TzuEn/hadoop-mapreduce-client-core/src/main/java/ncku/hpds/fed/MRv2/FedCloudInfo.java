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
	private float wanSpeed;
	private Map<String, Float> wanSpeedMap = new HashMap<String, Float>();

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
		System.out.println(this.cloudName+" speed:"+(float)((float)this.inputSize/((float)this.regionMapTime-(float)this.regionMapStartTime)));
	}

	public float getWanSpeed() {
		return wanSpeed;
	}

	public void setWanSpeed(float wanSpeed) {
		this.wanSpeed = wanSpeed;
		System.out.println(this.cloudName+" WAN speed:"+ this.wanSpeed);
	}

	public Map<String, Float> getWanSpeedMap() {
		return wanSpeedMap;
	}

	public void setWanSpeedMap(Map<String, Float> wanSpeedMap) {
		this.wanSpeedMap = wanSpeedMap;
	}
	public void setWanSpeed(String dest, float speed){
		System.out.println("WAN speed:"+cloudName+"->"+dest+":"+speed);
		wanSpeedMap.put(dest, speed);
	}

}
