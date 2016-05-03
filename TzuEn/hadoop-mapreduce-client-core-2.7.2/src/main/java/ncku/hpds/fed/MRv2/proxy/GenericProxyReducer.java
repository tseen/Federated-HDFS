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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;

public class GenericProxyReducer<T1, T2> extends Reducer<T1, T2, Text, Text> {
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
	private static double WAN_FACTOR = 1d;
	private static double MAP_INPUTSIZE_FACTOR = 0d;



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
				//System.out.println("KEY:"+key+" SPEED:"+speed +" HASH:" + hash+ " SECTION:"+section);
				if(hash < speed){
					//System.out.println("Out Section:"+section);
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
	private boolean mWanOpt = false;



	// private String tachyonOutDir;

	public GenericProxyReducer(Class<T1> keyClz, Class<T2> valueClz)
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
			HW.init();
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
		if(conf.get("wanOpt").equals("true")){
			
			mWanOpt = true;
			
			
			System.out.println(conf.get("fedInfos"));
			String[] nodes = conf.get("fedInfos").split(",");
			for(int i = 0; i<nodes.length; i++){
				mClusterWeight.add(-1d);
			}
			for(int i = 0; i<nodes.length; i++){
					double infoGain = 0d;
					String[] resources = nodes[i].split("=")[1].split(";");
					System.out.println("Fomula " + nodes[i].split("=")[0] +"= " + resources[0] + "+" + resources[1] + "+" + resources[2]);
					infoGain = WAN_FACTOR * Double.parseDouble(resources[0]) 
							        //+ NODES_FACTOR * Double.parseDouble(resources[0])
							        + MB_FACTOR * Double.parseDouble(resources[1])
							        + VCORES_FACTOR * Double.parseDouble(resources[2]);
							        //+ MAP_INPUTSIZE_FACTOR * Double.parseDouble(resources[3]); 
					mClusterWeight.set(TopCloudHasher.setFileNameOrderInt("hdfs://"+nodes[i].split("=")[0]+"/"), infoGain);
									
			}
		/*	
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
					String interSize = infonodes[i].split("=")[1];
					double currentGain = mClusterWeight.get(TopCloudHasher.setFileNameOrderInt("hdfs://"+infonodes[i].split("=")[0]+"/"));
					currentGain += MAP_INPUTSIZE_FACTOR * Double.parseDouble(interSize); 
					mClusterWeight.set(TopCloudHasher.setFileNameOrderInt("hdfs://"+infonodes[i].split("=")[0]+"/"), currentGain);
					System.out.println("Fomula " + nodes[i].split("=")[0] +" += " +interSize);

				}
			}
		*/
			Double total = 0d;
			for(Double speed: mClusterWeight){
				total += speed;
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
		int iterations = 0;
		while (it.hasNext()) {
			
			T2 value = it.next();
			T1 mCheckKey ;
			mCheckKey = (T1) ReflectionUtils.newInstance(mKeyClz,context.getConfiguration());
		    ReflectionUtils.copy(context.getConfiguration(), key, mCheckKey);
		    //System.out.println("key: "+ key.toString()+" CheckKey: "+mCheckKey.toString());
			iterations++;
			
			if(firstInput){
				firstInput = false;
				//System.out.println("mLastKey NULL");
				mOutBuffer = "";
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("DoubleWritable : " + tValue.get());

				} else if ( mValueClzName.contains("NullWritable") ) {
	                NullWritable tValue = (NullWritable) value;
	                mOutBuffer+="n"+mSeperator;
	                //System.out.println("NullWritable : " + tValue.get());
	            } else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					//System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					//System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					mOutBuffer+=tValue.toString()+mSeperator;
				}
			    ReflectionUtils.copy(context.getConfiguration(), mCheckKey, mLastKey);
				
			}
			
			else if(mCheckKey.equals(mLastKey)){
				////System.out.println("CheckKey: " + mCheckKey.toString());
				////System.out.println(" LastKey: "+mLastKey.toString());

				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("DoubleWritable : " + tValue.get());

				} else if ( mValueClzName.contains("NullWritable") ) {
	                NullWritable tValue = (NullWritable) value;
	                mOutBuffer+="n"+mSeperator;
	                //System.out.println("NullWritable : " + tValue.get());
	            } else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					//System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					//System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					mOutBuffer+=tValue.toString()+mSeperator;
				}
			    ReflectionUtils.copy(context.getConfiguration(), mCheckKey, mLastKey);
				//mLastKey = mCheckKey;
			}
			
			else{
				HdfsWriter<T1, String> HW;
				try {
					HW = mHdfsWriter.get(Integer.parseInt(generateFileName(mLastKey,topNumbers)));
					HW.write(mLastKey, mOutBuffer);
				}  catch (NoSectionException e) {
					//System.out.println("no section");
				} 
				//System.out.println("else out:" + mLastKey.toString() + "	"+mOutBuffer);
				mOutBuffer = "";
			    ReflectionUtils.copy(context.getConfiguration(), mCheckKey, mLastKey);
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("DoubleWritable : " + tValue.get());

				} else if ( mValueClzName.contains("NullWritable") ) {
	                NullWritable tValue = (NullWritable) value;
	                mOutBuffer+="n"+mSeperator;
	                //System.out.println("NullWritable : " + tValue.get());
	            } else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					//System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					//System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					//System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					mOutBuffer+=tValue.toString()+mSeperator;
				}
				
			}
			if( mOutBuffer.length()  > 10000){
				//System.out.println("buffer out");

				HdfsWriter<T1, String> HW;
				try {
					HW = mHdfsWriter.get(Integer.parseInt(generateFileName(mLastKey,topNumbers)));
					HW.write(mLastKey, mOutBuffer);
				}  catch (NoSectionException e) {
					//System.out.println("no section");
				} 
				mOutBuffer = "";
			}
			
		}
		//System.out.println("reduce value numbers:" + iterations);

		//System.out.println("keyMap size:" + keyMap.entrySet().size());

		
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		HdfsWriter<T1, String> hw;
		try {
			hw = mHdfsWriter.get(Integer.parseInt(generateFileName(mLastKey,topNumbers)));
			hw.write(mLastKey, mOutBuffer);
		} catch (NoSectionException e) {
			//System.out.println("no section");
		}
		//System.out.println("clean out:" + mLastKey.toString() + "	"+mOutBuffer);

		for (HdfsWriter HW : mHdfsWriter) {
			HW.out.close();
			HW.client.close();
		}
	

	}
}
