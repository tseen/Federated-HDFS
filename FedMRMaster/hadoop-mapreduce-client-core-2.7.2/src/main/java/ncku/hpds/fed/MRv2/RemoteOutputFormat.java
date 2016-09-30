/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class RemoteOutputFormat<K, V> extends FileOutputFormat<K, V> {
	static int hash(int h) {
		h ^= (h >>> 20) ^ (h >>> 12);
		       return h ^ (h >>> 7) ^ (h >>> 4);
		     }
	public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
	private String mSeperator = "||";
	private String namenode = "";
	private int MAX_COUNT = 999;
	private int topNumbers;
	private List<HdfsWriter<K, String>> mHdfsWriterList = new ArrayList<HdfsWriter<K, String>>();
	
	protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}
		

		//private final byte[] keyValueSeparator;
		private List<HdfsWriter<K, String>> mHdfsWriter = new ArrayList<HdfsWriter<K, String>>();
		private int topNumbers;
		private String ip;
		public FedJobServerClient client;
		private Partitioner<K, V> mPartitioner;
		private Class<K> mClazz;
		private Configuration mConf;
		public LineRecordWriter(List<HdfsWriter<K, String>> hw, int top, String ip,  Partitioner<K, V> partitioner, Class<K> mclazz, Configuration conf){
			mHdfsWriter = hw;
			topNumbers = top;
			mPartitioner = partitioner;
			mClazz = mclazz;
			mConf = conf;
			InetAddress address;
			try {
				address = InetAddress.getByName(ip);
				client = new FedJobServerClient(address.getHostAddress(), 8713);
				client.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (HdfsWriter<K, String> HW : mHdfsWriter) {
				HW.init();
			}
			prev = (K) ReflectionUtils.newInstance(mClazz, mConf);

		}

    private String generateFileName(K key, int topNumbers){
      int hash = 0 ;	   
      //hash = (key.hashCode() & Integer.MAX_VALUE) % topNumbers;
      if ( topNumbers <= 0 ) {
        hash = 0;
      } else {
        hash = mPartitioner.getPartition(key, null, topNumbers);
      }
      return Integer.toString(hash);
    }
		
		private int iterations = 0;
		private Map<K, String> keyMap = new HashMap<K, String>();
		private String mSeperator = "||";

		K prev; 

    public synchronized void write(K key, V value) throws IOException {
      K keyout;
      V valueout;
      keyout = (K) ReflectionUtils.newInstance(mClazz, mConf);
      //			System.out.println("kout1: "+ keyout.toString());
      ReflectionUtils.copy(mConf, key, keyout);
      //			System.out.println("kout2: "+ keyout.toString());

      if (keyMap.size() > 200) {
        System.out.println("SECTION 1");
        System.out.println("keyMap size:" + keyMap.size() );
        for (Iterator<Map.Entry<K, String>> iter = keyMap.entrySet().iterator(); iter.hasNext();)
        {
          Map.Entry<K, String> entry = iter.next();
          String vvalue = entry.getValue();
          K kkey = entry.getKey();

          System.out.println("PRINT 1:" + kkey+ "===" + vvalue);
          HdfsWriter<K, String> hw = (HdfsWriter<K, String>) mHdfsWriter.get(Integer.parseInt(generateFileName(kkey,topNumbers)));
          hw.write(kkey, vvalue);

        }
        keyMap.clear();
        System.out.println("delete size:" + keyMap.size() );
      }
      //Gen<K> g = new Gen<K>(mClazz);
      // K mCheckKey = g.get();
      //System.out.println("prev3: "+ prev.toString());

      if (keyMap.containsKey(keyout)) {
        System.out.println("Contains key: " + keyout.toString());
        //	System.out.println("hash: " + hash(keyout.hashCode()));
        //	System.out.println("hash1: " + keyout.hashCode());
        String ssb = keyMap.get(keyout);
        ssb += value.toString()+mSeperator;
        keyMap.put(keyout, ssb);
      }
      else{
        //	System.out.println("prev: "+ prev.toString());
        //	System.out.println("kout: "+ keyout.toString());
        //	System.out.println("equals: "+ prev.equals(keyout));
        System.out.println("Not Contains key: " + keyout.toString());
        //	System.out.println("Hash : " + hash(keyout.hashCode())+ " Hash1 : " + keyout.hashCode());
        //	System.out.println("Hashp: " + hash(prev.hashCode())+ " Hash1p: " + prev.hashCode());

        String ssb = new String();
        ssb+=value.toString()+mSeperator;
        keyMap.put(keyout, ssb);
      }
      //System.out.print("prev: "+ prev.toString());
      //ReflectionUtils.copy(mConf, key, prev);
      //System.out.println(" --> "+ prev.toString());

      //HdfsWriter<K, String> hw = (HdfsWriter<K, String>) mHdfsWriter.get(Integer.parseInt(generateFileName(key,topNumbers)));
      //hw.write(key, value);

    }

		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			System.out.println("SECTION 2");
			System.out.println("keyMap size:" + keyMap.size() );
			for (Iterator<Map.Entry<K, String>> iter = keyMap.entrySet().iterator(); iter.hasNext();)
			{
				Map.Entry<K, String> entry = iter.next();
				String vvalue = entry.getValue();
				K kkey = entry.getKey();
				
				System.out.println("PRINT 1:" + kkey+ "===" + vvalue);
				HdfsWriter<K, String> hw = (HdfsWriter<K, String>) mHdfsWriter.get(Integer.parseInt(generateFileName(kkey,topNumbers)));
				hw.write(kkey, vvalue);

			}
			keyMap.clear();
			for (HdfsWriter HW : mHdfsWriter) {
				HW.close();
			}
			
			client.stopClientProbe();

		}
	}
	
	

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		Configuration conf = job.getConfiguration();
		namenode = conf.get("fs.default.name");
		String ip = conf.get("fedCloudHDFS").split(":")[1].split("/")[2];

		
		//TODO limit HdfsWriter based on topNumbers 
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

			mHdfsWriterList.add(HW);
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
		Partitioner<K, V> partitioner = null ;
		try {
			partitioner = (org.apache.hadoop.mapreduce.Partitioner<K,V>)
			          ReflectionUtils.newInstance(job.getPartitionerClass(), conf);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Class<?> mclazz = job.getMapOutputKeyClass();
		return new LineRecordWriter<K, V>(mHdfsWriterList, topNumbers, ip, partitioner, (Class<K>)mclazz, conf);
	}

}

