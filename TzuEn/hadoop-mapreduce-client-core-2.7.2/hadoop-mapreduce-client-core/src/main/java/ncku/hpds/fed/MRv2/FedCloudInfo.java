/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.util.HashMap;
import java.util.Map;

public class FedCloudInfo {
	private long inputSize;
	private double inputSize_normalized = 0;

	private String cloudName = "";
	private int regionMapTime;
	private int regionMapStartTime;
	private int wanSpeedCount = 0;
	private double wanSpeed;
	private int availableMB = 0;
	private int activeNodes = 0;
	private double availableMB_normalized = 0;
	private double availableVcores_normalized = 0;
	private int availableVcores = 0;
	private boolean isMBset = false;
	private boolean isNodesset = false;
	private boolean isVcoresset = false;

	
	private Map<String, Double> wanSpeedMap = new HashMap<String, Double>();
	private double minWanSpeed = 0d;
	private double minWanSpeed_normalized = 0d;
	
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
	
	public void collectWanSpeedandGetPartitionStrategy(Map<String, Double> downSpeed){
		for (Map.Entry<String, Double> wan : wanSpeedMap
				.entrySet()) {
			System.out.println("CollectAllWAN:"+cloudName+">"+wan.getKey()+"="+wan.getValue());
			if(!cloudName.equals(wan.getKey())){
				Double currentMinimum =  downSpeed.get(wan.getKey());
				if(currentMinimum == null) 
					currentMinimum =  Double.MAX_VALUE ;
				if(wan.getValue() < currentMinimum)
					downSpeed.put(wan.getKey(), wan.getValue());
				else
					downSpeed.put(wan.getKey(), currentMinimum);
				//downSpeed.put(wan.getKey(), currentMinimum + wan.getValue());
			}
		}
	}

	public int getWanSpeedCount() {
		return wanSpeedCount;
	}

	public int getAvailableMB() {
		return availableMB;
	}

	public void setAvailableMB(int availableMB) {
		System.out.println("setAvailableMB:"+cloudName+","+availableMB);
		isMBset = true;
		this.availableMB = availableMB;
	}

	public int getActiveNodes() {
		return activeNodes;
	}

	public void setActiveNodes(int activeNodes) {
		System.out.println("setActiveNodes:"+cloudName+","+activeNodes);
		isNodesset = true;
		this.activeNodes = activeNodes;
	}

	public int getAvailableVcores() {
		return availableVcores;
	}

	public void setAvailableVcores(int availableVcores) {
		System.out.println("setAvailableVcores:"+cloudName+","+availableVcores);
		isVcoresset = true;
		this.availableVcores = availableVcores;
	}

	public boolean isMBset() {
		return isMBset;
	}

	public void setMBset(boolean isMBset) {
		this.isMBset = isMBset;
	}

	public boolean isNodesset() {
		return isNodesset;
	}

	public void setNodesset(boolean isNodesset) {
		this.isNodesset = isNodesset;
	}

	public boolean isVcoresset() {
		return isVcoresset;
	}

	public void setVcoresset(boolean isVcoresset) {
		this.isVcoresset = isVcoresset;
	}


	public double getInputSize_normalized() {
		return inputSize_normalized;
	}

	public void setInputSize_normalized(double inputSize_normalized) {
		this.inputSize_normalized = inputSize_normalized;
	}

	public double getAvailableMB_normalized() {
		return availableMB_normalized;
	}

	public void setAvailableMB_normalized(double availableMB_normalized) {
		this.availableMB_normalized = availableMB_normalized;
	}


	public double getAvailableVcores_normalized() {
		return availableVcores_normalized;
	}

	public void setAvailableVcores_normalized(double availableVcores_normalized) {
		this.availableVcores_normalized = availableVcores_normalized;
	}

	public double getMinWanSpeed() {
		return minWanSpeed;
	}

	public void setMinWanSpeed(double minWanSpeed) {
		this.minWanSpeed = minWanSpeed;
	}

	public double getMinWanSpeed_normalized() {
		return minWanSpeed_normalized;
	}

	public void setMinWanSpeed_normalized(double minWanSpeed_normalized) {
		this.minWanSpeed_normalized = minWanSpeed_normalized;
	}

}
