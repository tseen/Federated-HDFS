/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
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
	private Text mKey = new Text();
	private Text mValue = new Text();
	//private Text mCheckKey = new Text();

	private int count = 0;
	private int MAX_COUNT = 999;
	private String mSeperator = "||";
	private String namenode = "";
	private Partitioner<T1, T2> mPartitioner;
	private int topNumbers;
	private boolean firstInput = true;

	private Map<String, String> keyMap = new HashMap<String, String>();

	private String generateFileName(Text mKey2, int topNumbers) {
		return TopCloudHasher.generateFileName(mKey2, topNumbers);
	}
	private String generateFileName(T1 key, int topNumbers){
		int hash = 0 ;	   
		//hash = (key.hashCode() & Integer.MAX_VALUE) % topNumbers;
		hash = mPartitioner.getPartition(key, null, topNumbers);
	    return Integer.toString(hash);
	}

	// private MultipleOutputs mos;
	// private TachyonFileSystem tfs;

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
		
		client = new FedJobServerClient(address.getHostAddress(), 8713);
		client.start();

		
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
				System.out.println("set max count : " + MAX_COUNT);
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
		String res = client.sendReqWAN(namenode.split("/")[2]);
		if(res.contains(FedCloudProtocol.RES_WAN_SPEED)){
			System.out.println(res);
		}

		__reset();
	}


	@Override
	public void reduce(T1 key, Iterable<T2> values, Context context)
			throws IOException, InterruptedException {
		client.sendRegionMapFinished(namenode.split("/")[2]);
		Iterator<T2> it = values.iterator();
		__reset();
		int iterations = 0;
		while (it.hasNext()) {
			
			T2 value = it.next();
			T1 mCheckKey ;
			mCheckKey = (T1) ReflectionUtils.newInstance(mKeyClz,context.getConfiguration());
		    ReflectionUtils.copy(context.getConfiguration(), key, mCheckKey);
		    System.out.println("key: "+ key.toString()+" CheckKey: "+mCheckKey.toString());
			iterations++;
			/*if (mKeyClzName.contains("DoubleWritable")) {
				DoubleWritable tKey = (DoubleWritable) key;
				mCheckKey = String.valueOf(tKey.get());
				System.out.println("Check Key:" + mCheckKey);
			} 
			else if ( mKeyClzName.contains("NullWritable") ) {
	            NullWritable tKey = (NullWritable) key;
				mCheckKey = "n"+ Integer.toString(iterations);
				System.out.println("Check Key:" + mCheckKey);

			}
			else if (mKeyClzName.contains("FloatWritable")) {
				FloatWritable tKey = (FloatWritable) key;
				mCheckKey = String.valueOf(tKey.get());
				System.out.println("Check Key:" + mCheckKey);

			} else if (mKeyClzName.contains("IntWritable")) {
				IntWritable tKey = (IntWritable) key;
				mCheckKey = String.valueOf(tKey.get());
				System.out.println("Check Key:" + mCheckKey);

			} else if (mKeyClzName.contains("LongWritable")) {
				LongWritable tKey = (LongWritable) key;
				mCheckKey = String.valueOf(tKey.get());
				System.out.println("Check Key:" + mCheckKey);

			} else if (mKeyClzName.contains("Text")) {
				Text tKey = (Text) key;
				mCheckKey = tKey.toString();
				System.out.println("Check Key:" + mCheckKey);

			} else if (mKeyClzName.contains("UTF8")) {
				UTF8 tKey = (UTF8) key;
				mCheckKey = tKey.toString();
				System.out.println("Check Key:" + mCheckKey);

			} else if (mKeyClzName.contains("VIntWritable")) {
				VIntWritable tKey = (VIntWritable) key;
				mCheckKey = String.valueOf(tKey.get());
				System.out.println("Check Key:" + mCheckKey);

			} else if (mKeyClzName.contains("VLongWritable")) {
				VLongWritable tKey = (VLongWritable) key;
				mCheckKey = String.valueOf(tKey.get());
				System.out.println("Check Key:" + mCheckKey);
			} else {
				mCheckKey = key.toString();
				System.out.println("Check Key:" + mCheckKey);
			}*/
			if(firstInput){
				firstInput = false;
				System.out.println("mLastKey NULL");
				mOutBuffer = "";
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("DoubleWritable : " + tValue.get());

				} else if ( mValueClzName.contains("NullWritable") ) {
	                NullWritable tValue = (NullWritable) value;
	                mOutBuffer+="n"+mSeperator;
	                System.out.println("NullWritable : " + tValue.get());
	            } else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					mOutBuffer+=tValue.toString()+mSeperator;
				}
			    ReflectionUtils.copy(context.getConfiguration(), mCheckKey, mLastKey);
				
			}
			else if(mCheckKey.equals(mLastKey)){
				System.out.println("CheckKey: " + mCheckKey.toString());
				System.out.println(" LastKey: "+mLastKey.toString());

				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("DoubleWritable : " + tValue.get());

				} else if ( mValueClzName.contains("NullWritable") ) {
	                NullWritable tValue = (NullWritable) value;
	                mOutBuffer+="n"+mSeperator;
	                System.out.println("NullWritable : " + tValue.get());
	            } else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					mOutBuffer+=tValue.toString()+mSeperator;
				}
			    ReflectionUtils.copy(context.getConfiguration(), mCheckKey, mLastKey);
				//mLastKey = mCheckKey;
			}
			else{
				HdfsWriter<T1, String> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(mLastKey,topNumbers)));
				HW.write(mLastKey, mOutBuffer);
				System.out.println("else out:" + mLastKey.toString() + "	"+mOutBuffer);
				mOutBuffer = "";
			    ReflectionUtils.copy(context.getConfiguration(), mCheckKey, mLastKey);
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("DoubleWritable : " + tValue.get());

				} else if ( mValueClzName.contains("NullWritable") ) {
	                NullWritable tValue = (NullWritable) value;
	                mOutBuffer+="n"+mSeperator;
	                System.out.println("NullWritable : " + tValue.get());
	            } else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					mOutBuffer+=tValue.toString()+mSeperator;
					System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					mOutBuffer+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					mOutBuffer+=tValue.toString()+mSeperator;
				}
				
			}
			
		}
		System.out.println("reduce value numbers:" + iterations);

		System.out.println("keyMap size:" + keyMap.entrySet().size());

		
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		HdfsWriter<T1, String> hw = mHdfsWriter.get(Integer.parseInt(generateFileName(mLastKey,topNumbers)));
		hw.write(mLastKey, mOutBuffer);
		System.out.println("clean out:" + mLastKey.toString() + "	"+mOutBuffer);

		for (HdfsWriter HW : mHdfsWriter) {
			HW.out.close();
			HW.client.close();
		}
		client.stopClientProbe();

	}
}
