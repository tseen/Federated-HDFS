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

public class GenericProxyReducer<T1, T2> extends Reducer<T1, T2, Text, Text> {

	public FedJobServerClient client;

	private StringBuffer sb = new StringBuffer();
	private Text mKey = new Text();
	private Text mValue = new Text();
	private Text mCheckKey = new Text();

	private int count = 0;
	private int MAX_COUNT = 999;
	private String mSeperator = "||";
	private String namenode = "";

	private int topNumbers;

	private Map<Text, StringBuffer> keyMap = new HashMap<Text, StringBuffer>();

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

		InetAddress address = InetAddress.getByName(ip);
		client = new FedJobServerClient(address.getHostAddress(), 8769);
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
				for (Iterator<Map.Entry<Text, StringBuffer>> iter = keyMap.entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<Text, StringBuffer> entry = iter.next();
					
					StringBuffer ssb = entry.getValue();
					//Text kkey = entry.getKey();
					Text kkey = new Text();
					kkey.set(entry.getKey().toString());
					Text vvalue = new Text();
					vvalue.set(ssb.toString());
					
					System.out.println("PRINT 1:" + kkey.toString() + "===" + vvalue.toString());

					HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(kkey,topNumbers)));
					HW.write(kkey, vvalue);
						
					/*System.out.println("keyMap size:" + keyMap.size() );
					iter.remove();
					System.out.println("delete size:" + keyMap.size() );
					 */
				}
				keyMap.clear();
				System.out.println("delete size:" + keyMap.size() );
				

			}
			
			iterations++;
			T2 value = it.next();
			if (mKeyClzName.contains("DoubleWritable")) {
				DoubleWritable tKey = (DoubleWritable) key;
				mCheckKey.set(String.valueOf(tKey.get()));

			} else if (mKeyClzName.contains("FloatWritable")) {
				FloatWritable tKey = (FloatWritable) key;
				mCheckKey.set(String.valueOf(tKey.get()));

			} else if (mKeyClzName.contains("IntWritable")) {
				IntWritable tKey = (IntWritable) key;
				mCheckKey.set(String.valueOf(tKey.get()));

			} else if (mKeyClzName.contains("LongWritable")) {
				LongWritable tKey = (LongWritable) key;
				mCheckKey.set(String.valueOf(tKey.get()));

			} else if (mKeyClzName.contains("Text")) {
				Text tKey = (Text) key;
				mCheckKey.set(tKey.toString());

			} else if (mKeyClzName.contains("UTF8")) {
				UTF8 tKey = (UTF8) key;
				mCheckKey.set(tKey.toString());

			} else if (mKeyClzName.contains("VIntWritable")) {
				VIntWritable tKey = (VIntWritable) key;
				mCheckKey.set(String.valueOf(tKey.get()));

			} else if (mKeyClzName.contains("VLongWritable")) {
				VLongWritable tKey = (VLongWritable) key;
				mCheckKey.set(String.valueOf(tKey.get()));
			} else {
				mCheckKey.set(key.toString());
			}
			// mCheckKey.set(key.toString());

			
			if (keyMap.containsKey(mCheckKey)) {
				System.out.println("Contains key:" + mCheckKey.toString());
				StringBuffer ssb = keyMap.get(mCheckKey);
				// HdfsWriter<Text, Text> HW =
				// mHdfsWriter.get(Integer.parseInt(generateFileName(mKey)));
				// ----------------------------------------------------------
				// Partial Generic Mapper Value Part
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);
					System.out.println("DoubleWritable : " + tValue.get());

				} else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);
					System.out.println("FloatWritable : " + tValue.get());

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);
					System.out.println("IntWritable : " + tValue.get());

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);
					System.out.println("LongWritable : " + tValue.get());

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					ssb.append(tValue.toString()).append(mSeperator);
					// mValue.set(tValue.toString());
					// HW.write(mKey, mValue);
					System.out.println("Text : " + tValue.toString());

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					ssb.append(tValue.toString()).append(mSeperator);
					System.out.println("UTF8 : " + tValue.toString());

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);
					System.out.println("VIntWritable : " + tValue.get());

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);
					System.out.println("VLongWritable : " + tValue.get());
				} else {
					T1 tValue = (T1) value;
					ssb.append(tValue.toString()).append(mSeperator);

				}
				// ----------------------------------------------------------
				keyMap.put(mCheckKey, ssb);

				if (ssb.length() > 20000) {

					System.out.println("PRINT 2");

					mValue.set(ssb.toString());
					HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer
							.parseInt(generateFileName(mCheckKey, topNumbers)));
					HW.write(mCheckKey, mValue);

					ssb.setLength(0);
					keyMap.put(mCheckKey, ssb);
				}
			} else {
				System.out.println("Not Contains key:" + mCheckKey.toString());

				StringBuffer ssb = new StringBuffer();
				if (mValueClzName.contains("DoubleWritable")) {
					DoubleWritable tValue = (DoubleWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);

				} else if (mValueClzName.contains("FloatWritable")) {
					FloatWritable tValue = (FloatWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);

				} else if (mValueClzName.contains("IntWritable")) {
					IntWritable tValue = (IntWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);

				} else if (mValueClzName.contains("LongWritable")) {
					LongWritable tValue = (LongWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);

				} else if (mValueClzName.contains("Text")) {
					Text tValue = (Text) value;
					ssb.append(tValue.toString()).append(mSeperator);

				} else if (mValueClzName.contains("UTF8")) {
					UTF8 tValue = (UTF8) value;
					ssb.append(tValue.toString()).append(mSeperator);

				} else if (mValueClzName.contains("VIntWritable")) {
					VIntWritable tValue = (VIntWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);

				} else if (mValueClzName.contains("VLongWritable")) {
					VLongWritable tValue = (VLongWritable) value;
					ssb.append(String.valueOf(tValue.get())).append(mSeperator);
				} else {
					T1 tValue = (T1) value;
					ssb.append(tValue.toString()).append(mSeperator);

				}
				System.out.println("keyMap.put:"+ mCheckKey);
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
		for (Iterator<Map.Entry<Text, StringBuffer>> pair = keyMap.entrySet().iterator(); pair.hasNext();)
		{
			Map.Entry<Text, StringBuffer> entry = pair.next();
			if (entry.getValue().length() > 0) {
				System.out.println("PRINT 3:" + entry.getKey().toString());
				StringBuffer ssb = entry.getValue();
				mValue.set(ssb.toString());
				HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(entry.getKey(),topNumbers)));
				HW.write(entry.getKey(), mValue);

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
