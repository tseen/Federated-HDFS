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

import ncku.hpds.fed.MRv2.FedJobServerClient;
import ncku.hpds.fed.MRv2.HdfsWriter;
import ncku.hpds.fed.MRv2.TopCloudHasher;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;

public class GenericProxyReduceBackup<T1, T2> extends Reducer<T1, T2, Text, Text> {

	public FedJobServerClient client;

	private StringBuffer sb = new StringBuffer();
	private Text mKey = new Text();
	private Text mValue = new Text();
	//private Text mCheckKey = new Text();

	private int count = 0;
	private int MAX_COUNT = 999;
	private String mSeperator = "||";
	private String namenode = "";

	private int topNumbers;

	private Map<String, String> keyMap = new HashMap<String, String>();

	private String generateFileName(Text mKey2, int topNumbers) {
		return TopCloudHasher.generateFileName(mKey2, topNumbers);
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

	// private String tachyonOutDir;

	public GenericProxyReduceBackup(Class<T1> keyClz, Class<T2> valueClz)
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
					+ TopCloudHasher.hashToTop(url + "/"));

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
			//System.out.println("Key:"+);
			if (keyMap.size() > 200) {
				System.out.println("SECTION 1");
				System.out.println("keyMap size:" + keyMap.size() );
				for (Iterator<Map.Entry<String, String>> iter = keyMap.entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String, String> entry = iter.next();
					
					//Text kkey = entry.getKey();
					String value = entry.getValue();
					String kkey = entry.getKey();
					
					
					System.out.println("PRINT 1:" + kkey+ "===" + value);

					HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(new Text(kkey),topNumbers)));
					HW.write(new Text(kkey), new Text(value));

						
					/*System.out.println("keyMap size:" + keyMap.size() );
					iter.remove();
					System.out.println("delete size:" + keyMap.size() );
					 */
				}
				keyMap.clear();
				System.out.println("delete size:" + keyMap.size() );
				

			}
			String mCheckKey = new String();
			iterations++;
			T2 value = it.next();
			if (mKeyClzName.contains("DoubleWritable")) {
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
			}
			// mCheckKey.set(key.toString());

			
			if (keyMap.containsKey(mCheckKey)) {
				System.out.println("Contains key:" + mCheckKey.toString());
				String ssb = keyMap.get(mCheckKey);
				// HdfsWriter<Text, Text> HW =
				// mHdfsWriter.get(Integer.parseInt(generateFileName(mKey)));
				// ----------------------------------------------------------
				// Partial Generic Mapper Value Part
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("DoubleWritable : " + tValue.get());

				} else if ( mValueClzName.contains("NullWritable") ) {
	                NullWritable tValue = (NullWritable) value;
	                ssb+="n"+mSeperator;
	                System.out.println("NullWritable : " + tValue.get());
	            } else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					ssb+=tValue.toString()+mSeperator;
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					ssb+=tValue.toString()+mSeperator;
					System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;
					System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					ssb+=tValue.toString()+mSeperator;

				}
				// ----------------------------------------------------------
				keyMap.put(mCheckKey, ssb);

			/*	if (ssb.length() > 20000) {

					System.out.println("PRINT 2");

					mValue.set(ssb.toString());
					HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer
							.parseInt(generateFileName(new Text(mCheckKey), topNumbers)));
					HW.write(new Text(mCheckKey), mValue);

					//ssb.setLength(0);
					keyMap.put(mCheckKey, ssb);
				}
				*/
			} else {
				System.out.println("Not Contains key:" + mCheckKey.toString());

				String ssb = new String();
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;

				} else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;

				} else if ( mValueClzName.contains("NullWritable") ) {
 	                NullWritable tValue = (NullWritable) value;
					ssb+="n"+mSeperator;
 	
 	            } else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					ssb+=tValue.toString()+mSeperator;

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					ssb+=tValue.toString()+mSeperator;

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					ssb+=String.valueOf(tValue.get())+mSeperator;
				} else {
					T1 tValue = (T1) value;
					ssb+=tValue.toString()+mSeperator;

				}
				keyMap.put(mCheckKey, ssb);

			}
		}
		System.out.println("reduce value numbers:" + iterations);

		System.out.println("keyMap size:" + keyMap.entrySet().size());

		/*
		 * for (Map.Entry<Text, StringBuffer> entry : keyMap.entrySet()) {
		 * if(entry.getValue().length() > 0){ StringBuffer ssb =
		 * entry.getValue(); mValue.set(ssb.toString()); HdfsWriter<Text, Text>
		 * HW =
		 * mHdfsWriter.get(Integer.parseInt(generateFileName(entry.getKey(),
		 * topNumbers))); HW.write(entry.getKey(), mValue); //ssb.setLength(0);
		 * 
		 * keyMap.remove(entry.getKey()); //keyMap.put(entry.getKey(), ssb); } }
		 */
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	//	Iterator iter = keyMap.entrySet().iterator();
		for (Iterator<Map.Entry<String, String>> pair = keyMap.entrySet().iterator(); pair.hasNext();)
		{
			Map.Entry<String, String> entry = pair.next();
			if (entry.getValue().length() > 0) {
				System.out.println("PRINT 3:" + entry.getKey().toString());
				String value = entry.getValue();
				String kkey = entry.getKey();
				//mValue.set(ssb.toString());
				HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(new Text(kkey),topNumbers)));
				HW.write(new Text(kkey), new Text(value));

				pair.remove();

			}
		}
	/*	while (iter.hasNext()) {
			System.out.println("PRINT 3");
			Map.Entry<Text, StringBuffer> pair = (Map.Entry<Text, StringBuffer>) iter
					.next();
			if (pair.getValue().length() > 0) {
				System.out.println("PRINT 3:" + pair.getKey().toString());
				StringBuffer ssb = pair.getValue();
				mValue.set(ssb.toString());
				HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer
						.parseInt(generateFileName(pair.getKey(), topNumbers)));
				HW.write(pair.getKey(), mValue);

				iter.remove();

			}
		}
		*/
		for (HdfsWriter HW : mHdfsWriter) {
			HW.out.close();
			HW.client.close();
		}
		client.stopClientProbe();

	}
}
