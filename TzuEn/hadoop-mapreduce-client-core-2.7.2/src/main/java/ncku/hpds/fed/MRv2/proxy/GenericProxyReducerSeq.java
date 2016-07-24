/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed //System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2.proxy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ncku.hpds.fed.MRv2.FedCloudProtocol;
import ncku.hpds.fed.MRv2.FedJobServerClient;
import ncku.hpds.fed.MRv2.HdfsWriter;
import ncku.hpds.fed.MRv2.TopCloudHasher;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;

public class GenericProxyReducerSeq<T1, T2> extends Reducer<T1, T2, Text, Text> {
	public FedJobServerClient client;


	private StringBuffer sb = new StringBuffer();

	//private Text mCheckKey = new Text();

	private int count = 0;
	private int MAX_COUNT = 999;
	private String mSeperator = "||";
	private String namenode = "";
	private Partitioner<T1, T2> mPartitioner;
	private int topNumbers;
	private boolean firstInput = true;
	private static int PARTITION_GRAIN_NUMBERS = 1000;
	private static double NODES_FACTOR = 0d;
	private static double MB_FACTOR = 0.5d;
	private static double VCORES_FACTOR = 0.5d;
	private static double WAN_FACTOR = 0.33d;
	private static double MAP_INPUTSIZE_FACTOR = 0.33d;



	private String generateFileName(Text mKey2, int topNumbers) {
		return TopCloudHasher.generateFileName(mKey2, topNumbers);
	}
	
	
	private String generateFileName(T1 key, int topNumbers) throws NoSectionException{
		int hash = 0 ;	   
		//hash = (key.hashCode() & Integer.MAX_VALUE) % topNumbers;
		if(mWanOpt){
			hash = mPartitioner.getPartition(key, null, PARTITION_GRAIN_NUMBERS);
			int section = 0;
			for(double speed : mClusterWeight){
				//System.out.println("KEY:"+key.toString().substring(0, 75)+" HASH:" + hash);
				if(hash < speed){
				//	System.out.println("Out Section:"+section);
				    return Integer.toString(section);
				}
				    section++;
			}
			throw new NoSectionException();
		}
		else{
			hash = mPartitioner.getPartition(key, null, topNumbers);
		    return Integer.toString(hash);

		}
	}

	// private MultipleOutputs mos;
	// private TachyonFile//System tfs;

	private void __reset() {
		sb.setLength(0); // clean up the String buffer
		count = 0;
	}

	private Class<T1> mKeyClz;
	private Class<T2> mValueClz;
	private String mKeyClzName;
	private String mValueClzName;
	private List<HdfsWriter> mHdfsWriter = new ArrayList<HdfsWriter>();
	private T1 mLastKey;
	private String mOutBuffer;
	//private Map<String, Double> nDownSpeed = new HashMap<String, Double>();
	private List<Double> mClusterWeight = new ArrayList<Double>();
	private List<Double> mWanWeight = new ArrayList<Double>();
	private List<Double> mReduceWeight = new ArrayList<Double>();
	private List<Double> mMapWeight = new ArrayList<Double>();
	private boolean mWanOpt = false;



	// private String tachyonOutDir;

	public GenericProxyReducerSeq(Class<T1> keyClz, Class<T2> valueClz)
			throws Exception {
		mKeyClz = keyClz;
		mValueClz = valueClz;
		mKeyClzName = mKeyClz.getCanonicalName();
		mValueClzName = mValueClz.getCanonicalName();
	}

	/*
	 * public void setTachyonOutDir(String dir){ tachyonOutDir = dir; }
	 */
	// private List<FileOutStream> outList;
	// private List<String> outPath;
	// private Map<String, FileOutStream> outMap = new HashMap<String,
	// FileOutStream>();

	@Override
	public void setup(Context context) throws UnknownHostException {
		System.out.println("Start Proxy Reducer");

		Configuration conf = context.getConfiguration();
		namenode = conf.get("fs.default.name");
		String ip = conf.get("fedCloudHDFS").split(":")[1].split("/")[2];
		
		try {
			mPartitioner = (org.apache.hadoop.mapreduce.Partitioner<T1,T2>)
			          ReflectionUtils.newInstance(context.getPartitionerClass(), conf);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		mLastKey = (T1) ReflectionUtils.newInstance(mKeyClz,context.getConfiguration());

		InetAddress address = InetAddress.getByName(ip);
		
		

		
		topNumbers = Integer.parseInt(conf.get("topNumbers"));

		if (conf.get("topCounts") != null) {
			TopCloudHasher.topCounts = Integer.parseInt(conf.get("topCounts"));
		}

		System.out.println("topCOUNT:" + TopCloudHasher.topCounts);

		List<String> mTopCloudHDFSURLs = new ArrayList<String>();
		String topCloudHdfs[] = conf.get("topCloudHDFSs").split(",");
		for (int i = 0; i < topCloudHdfs.length; i++) {
			mTopCloudHDFSURLs.add(topCloudHdfs[i]);
		}
		TopCloudHasher.topURLs = mTopCloudHDFSURLs;

		for (String url : mTopCloudHDFSURLs) {
			HdfsWriter HW = new HdfsWriter(url, "hpds");
			HW.setFileName("/user/" + System.getProperty("user.name") + "/"
					+ conf.get("regionCloudOutput", "")
					+ TopCloudHasher.setFileNameOrder(url + "/"));

			mHdfsWriter.add(HW);
		}

		for (HdfsWriter HW : mHdfsWriter) {
			HW.creatwriter(mKeyClz, mValueClz);
			//HW.init();
		}

		// mos = new MultipleOutputs(context);

		if (conf.get("max_token") != null) {
			MAX_COUNT = 999;
			try {
				MAX_COUNT = Integer.parseInt(conf.get("max_token"));
				//System.out.println("set max count : " + MAX_COUNT);
			} catch (Exception e) {
			}
		}
		if (conf.get("proxy_seperator") != null) {
			try {
				mSeperator = conf.get("proxy_seperator");
			} catch (Exception e) {
				mSeperator = "||";
			}
		}
		client = new FedJobServerClient(address.getHostAddress(), 8713);
		client.start();
		client.sendRegionMapStopTime(namenode.split("/")[2]);
		if(conf.get("wanOpt").equals("true") && !conf.get("preciseIter").equals("true")){
			HashMap<String, Double> interSizeMap = new HashMap<String, Double>();
			mWanOpt = true;
			
			//c03:39100>>c02:39100=100||c04:39100=3||;1.0;1.0,
			System.out.println("FEDINFO	" + conf.get("fedInfos"));
			String[] nodes = conf.get("fedInfos").split(",");
			for(int i = 0; i<nodes.length-1; i++){
				mClusterWeight.add(-1d);
			}

			
		    boolean barrier = true;
			while(barrier){
				String res = client.sendWaitBarrier(namenode.split("/")[2]);
				if(res.contains(FedCloudProtocol.RES_TRUE_BARRIER))
					barrier = true;
				else if(res.contains(FedCloudProtocol.RES_FALSE_BARRIER))
					barrier = false;
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			String res = client.sendReqInterInfo(namenode.split("/")[2]);
			if(res.contains(FedCloudProtocol.RES_INTER_INFO)){
				String[] infonodes = res.substring(15).split(",");
				for(int i = 0; i< infonodes.length; i++){
					String cluster = infonodes[i].split("=")[0];
					String interSize = infonodes[i].split("=")[1];
					interSizeMap.put(cluster, Double.parseDouble(interSize));
				}
			}
			for(int i = 0; i<nodes.length-1; i++){
				String[] resources = nodes[i].split(">>")[1].split(";");
				String[] adaptiveFactor = nodes[nodes.length-1].split(";");
				String wan[] = resources[0].split("/");
			    //System.out.println("Fomula " + nodes[i].split(">>")[0] +"= " + minWAN + "+" + resources[1] + "+" + resources[2]);
				double multiInfoGain[];
				multiInfoGain = new double[wan.length];
				int a = 0;
				for(String Wan : wan){
					String cluster = Wan.split("=")[0];
					double w = Double.parseDouble(Wan.split("=")[1]);
					double inter = interSizeMap.get(cluster);
					double W = (WAN_FACTOR + Double.parseDouble(adaptiveFactor[0])- Double.parseDouble(adaptiveFactor[1])) * w;
					double I =  MAP_INPUTSIZE_FACTOR *inter;
					W=w;
					I=inter;
					multiInfoGain[a] = W/I +0.2d;
					//System.out.println("Formula "+ nodes[i].split(">>")[0] +"=" + cluster + " " + w +" /"+" "+inter +"="+ multiInfoGain[a]);
					a++;
				}
				double clusterGain = (MB_FACTOR+ Double.parseDouble(adaptiveFactor[2])/2- Double.parseDouble(adaptiveFactor[3])/2) * Double.parseDouble(resources[1])
		        		  + (VCORES_FACTOR+ Double.parseDouble(adaptiveFactor[2])/2- Double.parseDouble(adaptiveFactor[3])/2) * Double.parseDouble(resources[2]);
				double wanGain = Double.MAX_VALUE;
				for(double gain : multiInfoGain ){
					if(gain < wanGain)
						wanGain = gain;
				}
				double finalGain = (wanGain + clusterGain)/2;
				System.out.println("wanGain="+wanGain);
				System.out.println("clusterGain="+clusterGain);
				mClusterWeight.set(TopCloudHasher.setFileNameOrderInt("hdfs://"+nodes[i].split(">>")[0]+"/"), finalGain);
			}
			
			
		
			Double total = 0d;
			for(Double weight: mClusterWeight){
				total += weight;
			}
			for(int j = 0; j<mClusterWeight.size(); j++){
				mClusterWeight.set(j , (mClusterWeight.get(j) / total ) * PARTITION_GRAIN_NUMBERS);
			}
			int s = 0;
			for(Double clusterWeight: mClusterWeight){
				System.out.println(s+"="+clusterWeight);
				s++;
			}
			setUpSections();
		}
		else if(conf.get("wanOpt").equals("true") && conf.get("preciseIter").equals("true")){
			HashMap<String, Double> interSizeMap = new HashMap<String, Double>();
			mWanOpt = true;
			
			//c03:39100>>c02:39100=100||c04:39100=3||;1.0;1.0,
			System.out.println("FEDINFO	" + conf.get("fedInfos"));
			String[] nodes = conf.get("fedInfos").split(",");
			for(int i = 0; i<nodes.length; i++){
				mClusterWeight.add(-1d);
				mWanWeight.add(-1d);
				mMapWeight.add(-1d);
				mReduceWeight.add(-1d);

			}
		
			for(int i = 0; i<nodes.length; i++){
				String[] resources = nodes[i].split(">>")[1].split(";");
				String cluster = nodes[i].split(">>")[0];
				String interSize = resources[6];
				interSizeMap.put(cluster, Double.parseDouble(interSize));
			}
			long interTime = 0;
			long topTime = 0;
			long mapTime = 0;
			for(int i = 0; i<nodes.length; i++){
				String[] resources = nodes[i].split(">>")[1].split(";");
				interTime += Long.parseLong(resources[3]);
				topTime += Long.parseLong(resources[4]);
				mapTime += Long.parseLong(resources[5]);
			}
			for(int i = 0; i<nodes.length; i++){
					String[] resources = nodes[i].split(">>")[1].split(";");
					String wan[] = resources[0].split("/");
					double multiInfoGain[];
					multiInfoGain = new double[wan.length];
					int a = 0;
					for(String Wan : wan){
						String cluster = Wan.split("=")[0];
						//Mbps -> bps
					//	double W = Double.parseDouble(Wan.split("=")[1]) * 1024 *1024;
						//bytes/millisecond -> bps
					//	double C = Double.parseDouble(resources[1]) * 1000;
						double W = Double.parseDouble(Wan.split("=")[1]);
						double C = Double.parseDouble(resources[1]);
						double I = interSizeMap.get(cluster);
						multiInfoGain[a] = W/I + 0.2d;
						System.out.println("Formula "+ nodes[i].split(">>")[0] +"=" + cluster + " " + W +" ," + C + "," + I +"="+ multiInfoGain[a]);
						a++;
					}
					
					double reduceGain = Double.parseDouble(resources[1]);
					double mapGain = Double.parseDouble(resources[2]);
					double wanGain = Double.MAX_VALUE;
					for(double gain : multiInfoGain ){
						if(gain < wanGain)
							wanGain = gain;
					}
				
					double wanFactor = (double)interTime/(double)(interTime+topTime+mapTime);
					double reduceFactor = (double)topTime/(double)(interTime+topTime+mapTime);
					double mapFactor = (double)mapTime/(double)(interTime+topTime+mapTime);

					double finalGain = wanGain*wanFactor 
									  +reduceGain*reduceFactor
									  +mapGain*mapFactor;
					System.out.println("wanGain="+wanGain + " reduceGain="+reduceGain+ " mapGain="+mapGain + " finalGain="+finalGain +" interTime="+interTime+ " topTime="+topTime+ " mapTime="+mapTime);
					mWanWeight.set(TopCloudHasher.setFileNameOrderInt("hdfs://"+nodes[i].split(">>")[0]+"/"), wanGain);
					mMapWeight.set(TopCloudHasher.setFileNameOrderInt("hdfs://"+nodes[i].split(">>")[0]+"/"), mapGain);
					mReduceWeight.set(TopCloudHasher.setFileNameOrderInt("hdfs://"+nodes[i].split(">>")[0]+"/"), reduceGain);

					//mClusterWeight.set(TopCloudHasher.setFileNameOrderInt("hdfs://"+nodes[i].split(">>")[0]+"/"), finalGain);
													
			}
			double wanFactor = 0d;
			double reduceFactor = 0d;
			double mapFactor = 0d;
			if(!conf.get("lastIter").equals("true")){
				wanFactor = (double)interTime/(double)(interTime+topTime+mapTime);
				reduceFactor = (double)topTime/(double)(interTime+topTime+mapTime);
				mapFactor = (double)mapTime/(double)(interTime+topTime+mapTime);
			}
			else if(conf.get("lastIter").equals("true")){
				wanFactor = (double)interTime/(double)(interTime+topTime);
				reduceFactor = (double)topTime/(double)(interTime+topTime);
			}
			
			Double total = 0d;
			for(Double weight: mWanWeight){
				total += weight;
			}
			for(int j = 0; j<mWanWeight.size(); j++){
				mWanWeight.set(j , (mWanWeight.get(j) / total ) * PARTITION_GRAIN_NUMBERS);
			}
			int s = 0;
			for(Double mWanWeight: mWanWeight){
				mClusterWeight.set(s, mWanWeight*wanFactor);
				System.out.println("wanWeight"+s+"="+mWanWeight);
				s++;
			}
			if(!conf.get("lastIter").equals("true")){
				total = 0d;
				for(Double weight: mMapWeight){
					total += weight;
				}
				for(int j = 0; j<mMapWeight.size(); j++){
					mMapWeight.set(j , (mMapWeight.get(j) / total ) * PARTITION_GRAIN_NUMBERS);
				}
				s = 0;
				for(Double clusterWeight: mMapWeight){
					double curr = mClusterWeight.get(s);
					curr += clusterWeight*mapFactor;
					mClusterWeight.set(s, curr);
					System.out.println("mapWeight"+s+"="+clusterWeight);
					s++;
				}
			}
			total = 0d;
			for(Double weight: mReduceWeight){
				total += weight;
			}
			for(int j = 0; j<mReduceWeight.size(); j++){
				mReduceWeight.set(j , (mReduceWeight.get(j) / total ) * PARTITION_GRAIN_NUMBERS);
			}
			s = 0;
			for(Double clusterWeight: mReduceWeight){
				double curr = mClusterWeight.get(s);
				curr += clusterWeight*reduceFactor;
				mClusterWeight.set(s, curr);
				System.out.println("reduceWeight"+s+"="+clusterWeight);
				s++;
			}
		/*	Double total = 0d;
			for(Double weight: mClusterWeight){
				total += weight;
			}
			for(int j = 0; j<mClusterWeight.size(); j++){
				mClusterWeight.set(j , (mClusterWeight.get(j) / total ) * PARTITION_GRAIN_NUMBERS);
			}
			*/
			s = 0;
			for(Double clusterWeight: mClusterWeight){
				System.out.println(s+"="+clusterWeight);
				s++;
			}
			
			
			setUpSections();
		}
		
		client.sendRegionInterTransferStartTime(namenode.split("/")[2]);
		__reset();
	}
	private void setUpSections(){	
		double prev = 0;
		for(int j = 0 ; j < mClusterWeight.size(); j++){
			double curr = mClusterWeight.get(j)+ prev;
			mClusterWeight.set(j, curr);
			prev = mClusterWeight.get(j);
		}
		//for(double speed: mClusterWeight){
			//System.out.println("section edge: "+speed);
		//}
		return;
	}


	@Override
	public void reduce(T1 key, Iterable<T2> values, Context context)
			throws IOException, InterruptedException {
	/*	if(!mReduceStart){
			client.sendRegionMapFinished(namenode.split("/")[2]);
			mReduceStart = true;
		}*/
		Iterator<T2> it = values.iterator();
		__reset();
		
		while (it.hasNext()) {
			
			T2 value = it.next();
			HdfsWriter<T1, T2> HW;
			try {
				HW = mHdfsWriter.get(Integer.parseInt(generateFileName(key,topNumbers)));
				HW.swrite(key, value);
			}  catch (NoSectionException e) {
				//System.out.println("no section");
			} 
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		

		for (HdfsWriter<T1, T2> HW : mHdfsWriter) {
		//	HW.out.close();
			HW.sWriter.close();
		//	HW.client.close();
		}
	

	}
}
