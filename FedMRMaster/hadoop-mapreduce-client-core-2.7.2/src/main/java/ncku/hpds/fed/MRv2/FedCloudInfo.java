/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.util.HashMap;
import java.util.Map;

public class FedCloudInfo {
	private long regionStartTime = 0;
	private long regionMapStopTime = 0;
	private long interStartTime = 0;
	private long interStopTime = 0;
	private long topStartTime = 0;
	private long topStopTime = 0;
	private long wanWaitingTime = 0;
	private long reduceWaitingTime = 0;
	private long lastWanTime = 0;
	private long lastReduceTime = 0;
	private long regionMap = 0;
	private long inter = 0;
	private long top = 0;
	
	boolean interStart = false;
	
	private long mapInputSize = 0;
	private double mapInputSize_normalized = 0;
	
	private long reduceInputSize = 0;
	
	private long interSize = 0;
	private double mapFactor = 0;
	private double interSize_normalized = 0;
	
	private double mapSpeed = 0;
	private double mapSpeed_normalized = 0;
	private double transferSpeed = 0;
	private double reduceSpeed = 0;
	private double reduceSpeed_normalized = 0;

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
	private String downLinkSpeed = "";
	private String downLinkSpeed_normalized = "";

	private double minWanSpeed = 0d;
	private double minWanSpeed_normalized = 0d;
	
	public FedCloudInfo(String Name){
		cloudName = Name;
	}
	public void clearInfo(){
		 
    //  this.mapInputSize = 0;
    //  this.mapInputSize_normalized = 0;

    this.reduceInputSize = 0;

    // this.interSize = 0;
    this.interSize_normalized = 0;

    this.reduceSpeed_normalized = 0;

    this.wanSpeedCount = 0;
    this.wanSpeed = 0;
    this.availableMB = 0;
    this.activeNodes = 0;
    this.availableMB_normalized = 0;
    this.availableVcores_normalized = 0;
    this.availableVcores = 0;
    this.isMBset = false;
    this.isNodesset = false;
    this.isVcoresset = false;

	}
	public long getInterSize() {
		return interSize;
	}

	public synchronized void setInterSize(long interSize) {
		this.interSize += interSize;
	}
	
	public void setInterSize_iter(){
	//	this.interSize = (long) (this.mapInputSize * this.mapFactor);
		//for now inter size = map size, because it will be normalized
		System.out.println("inter size approx : " + this.interSize +"="+this.mapInputSize+"*"+this.mapFactor);
		this.interSize = this.mapInputSize;
	}

	public String getCloudName() {
		return cloudName;
	}

	public void setCloudName(String cloudName) {
		this.cloudName = cloudName;
	}


	
	public void setRegionMapStartTime(int r) {
		this.regionMapStartTime = r;
		//System.out.println(this.cloudName+" time:"+this.regionMapTime+" size:"+this.inputSize);
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
	public void collectWanSpeed(Map<String, String> downSpeed){
		for (Map.Entry<String, Double> wan : wanSpeedMap
				.entrySet()) {
			System.out.println("CollectAllWAN:"+cloudName+">"+wan.getKey()+"="+wan.getValue());
			if(!cloudName.equals(wan.getKey())){
					String speed = downSpeed.get(wan.getKey());
					if(speed == null)
						downSpeed.put(wan.getKey(), this.cloudName+"="+Double.toString(wan.getValue())+"/");
					else
						downSpeed.put(wan.getKey(), speed + this.cloudName+"="+Double.toString(wan.getValue())+"/");
				//downSpeed.put(wan.getKey(), currentMinimum + wan.getValue());
			}
		}
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


	public double getInterSize_normalized() {
		return interSize_normalized;
	}

	public void setInterSize_normalized(double interSize_normalized) {
		this.interSize_normalized = interSize_normalized;
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

	public long getRegionStartTime() {
		return regionStartTime;
	}

	public void setRegionStartTime() {
		this.regionStartTime = System.currentTimeMillis();
	//	System.out.println(this.cloudName + "regionStartTime"+regionStartTime);
	}

	public long getInterStartTime() {
		return interStartTime;
	}

	public void setInterStartTime() {
		if(! interStart){
			this.interStartTime = System.currentTimeMillis();
		//	System.out.println(this.cloudName + "interStartTime"+interStartTime);
			interStart = true;
		}
	}

	public long getInterStopTime() {
		return interStopTime;
	}

	public void setInterStopTime() {
		interStart = false;
		this.interStopTime = System.currentTimeMillis();
		//System.out.println(this.cloudName + "interStopTime"+interStopTime);

	}

	public long getTopStartTime() {
		return topStartTime;
	}

	public void setTopStartTime() {
		this.topStartTime = System.currentTimeMillis();
		//System.out.println(this.cloudName + "topStartTime"+topStartTime);

	}

	public long getTopStopTime() {
		return topStopTime;
	}

	public void setTopStopTime() {
		this.topStopTime = System.currentTimeMillis();
		//System.out.println(this.cloudName + "topStopTime"+topStopTime);

	}
	
	public void printTime(){
		regionMap = this.regionMapStopTime - this.regionStartTime;
	    inter = this.interStopTime - this.interStartTime;
		top = this.topStopTime - this.topStartTime;
		
		System.out.println(this.cloudName + " Region Map Time = "+ regionMap/1000+"(s)");
		System.out.println(this.cloudName + " Inter Transfer Time = "+ inter/1000+"(s)");
		System.out.println(this.cloudName + " Top Time = "+ top/1000+"(s)");
		this.setMapSpeed();
		this.setTransferSpeed();
		//this.setReduceSpeed();
		System.out.println(this.cloudName + " Map Speed = "+ this.mapSpeed +"(byte/s)");
		System.out.println(this.cloudName + " Transfer Speed = "+ this.transferSpeed +"(byte/s)");
		//System.out.println(this.cloudName + " Reduce Speed = "+ this.reduceSpeed +"(byte/s)");

	}

	public long getRegionMapStopTime() {
		return regionMapStopTime;
	}

	public void setRegionMapStopTime() {
		this.regionMapStopTime = System.currentTimeMillis();;
	}

	public String getDownLinkSpeed() {
		return downLinkSpeed;
	}

	public void setDownLinkSpeed(String downLinkSpeed) {
		this.downLinkSpeed = downLinkSpeed;
	}

	public String getDownLinkSpeed_normalized() {
		return downLinkSpeed_normalized;
	}

	public void setDownLinkSpeed_normalized(String downLinkSpeed_normalized) {
		this.downLinkSpeed_normalized = downLinkSpeed_normalized;
	}

	public long getMapInputSize() {
		return mapInputSize;
	}

	public void setMapInputSize(long mapInputSize) {
    try { 
      this.mapFactor = (double) this.interSize /  (double) this.mapInputSize ; 
      System.out.println("this.interSize / this.mapInputSize = " +this.interSize +"/"+this.mapInputSize);
      this.mapInputSize = mapInputSize;
    } catch (Exception e ) {
      e.printStackTrace();
    }
	}

	public double getMapInputSize_normalized() {
		return mapInputSize_normalized;
	}

	public void setMapInputSize_normalized(double mapInputSize_normalized) {
		this.mapInputSize_normalized = mapInputSize_normalized;
	}

	public double getMapSpeed() {
		return mapSpeed;
	}

  public void setMapSpeed() {
    try {
      this.mapSpeed = this.mapInputSize / (this.regionMapStopTime - this.regionStartTime);
    } catch ( Exception e ) {
      e.printStackTrace();
      this.mapSpeed = 0;
    }
  }

	public double getReduceSpeed() {
		return reduceSpeed;
	}

	public void setReduceSpeed() {
    try {
      this.reduceSpeed = this.reduceInputSize / (this.topStopTime - this.topStartTime);
      System.out.println(this.cloudName + " Reduce Speed = "+ this.reduceInputSize +"/="+ this.reduceSpeed +"(byte/s)");
    } catch (Exception e) {
      e.printStackTrace();
      this.reduceSpeed =0;
    }
	}

	public double getTansferSpeed() {
		return transferSpeed;
	}

	public void setTransferSpeed() {
 		try { 
			this.transferSpeed =this.interSize /( this.interStopTime - this.interStartTime);
	  } catch (Exception e ) {
      e.printStackTrace();
		  this.transferSpeed = 0;
		}
	}

	public long getReduceInputSize() {
		return reduceInputSize;
	}

	public void setReduceInputSize(long reduceInputSize) {
		this.reduceInputSize = reduceInputSize;
	}

	public long getWanWaitingTime() {
		return wanWaitingTime;
	}

	public void setWanWaitingTime() {
		this.wanWaitingTime = this.lastWanTime - this.inter;
	}

	public long getReduceWaitingTime() {
		return reduceWaitingTime;
	}

	public void setReduceWaitingTime() {
		this.reduceWaitingTime = this.lastReduceTime - this.top;
	}

	public long getLastWanTime() {
		return lastWanTime;
	}

	public void setLastWanTime(long lastWanTime) {
		this.lastWanTime = lastWanTime;
	}

	public long getLastReduceTime() {
		return lastReduceTime;
	}

	public void setLastReduceTime(long lastReduceTime) {
		this.lastReduceTime = lastReduceTime;
	}

	public long getRegionMapTime() {
		return regionMap;
	}

	public void setRegionMap(long regionMap) {
		this.regionMap = regionMap;
	}

	public long getInterTime() {
		return inter;
	}

	public void setInter(long inter) {
		this.inter = inter;
	}

	public long getTopTime() {
		return top;
	}

	public void setTop(long top) {
		this.top = top;
	}

	public double getReduceSpeed_normalized() {
		return reduceSpeed_normalized;
	}

	public void setReduceSpeed_normalized(double reduceSpeed_normalized) {
		this.reduceSpeed_normalized = reduceSpeed_normalized;
	}
	public double getMapSpeed_normalized() {
		return mapSpeed_normalized;
	}
	public void setMapSpeed_normalized(double mapSpeed_normalized) {
		this.mapSpeed_normalized = mapSpeed_normalized;
	}

}
