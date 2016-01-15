package fbicloud.algorithm.classes;

import java.util.*;

public class BotnetGroup{
	HashSet<String> IPs = new HashSet<String>();
	double score;
	
	public BotnetGroup(String ip1, String ip2, double scoreValue){
		this.IPs.add(ip1);
		this.IPs.add(ip2);
		this.score = scoreValue;
	}
	public BotnetGroup(HashSet<String> tmpHashSet, double scoreValue){
		this.IPs.addAll(tmpHashSet);
		this.score = scoreValue;
	}
	public HashSet<String> getIPs(){
		return IPs;
	}
	public int getIPPairNo(){
		return IPs.size()-1;
	}
	public double getScore(){
		return score;
	}
	public void update(String ip1, String ip2, double scoreValue){
		//Add new IPs into this Botnet Group
		this.IPs.add(ip1);
		this.IPs.add(ip2);
		//Update score
		//if this.IPs.size() = 2	->	1 IP Pair contribute to score
		//if this.IPs.size() = 3	->	2 IP Pair contribute to score
		//if this.IPs.size() = 4	->	3 IP Pair contribute to score
		this.score = ( this.score*(this.IPs.size()-1) + scoreValue*1 ) / this.IPs.size();
	}
}
