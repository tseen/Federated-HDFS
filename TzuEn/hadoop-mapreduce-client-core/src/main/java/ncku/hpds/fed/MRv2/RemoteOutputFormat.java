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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class RemoteOutputFormat<K, V> extends FileOutputFormat<K, V> {
	
	public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
	private String mSeperator = "||";
	private String namenode = "";
	private int MAX_COUNT = 999;
	private int topNumbers;
	private List<HdfsWriter<String,String>> mHdfsWriterList = new ArrayList<HdfsWriter<String,String>>();

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
		private List<HdfsWriter<String, String>> mHdfsWriter = new ArrayList<HdfsWriter<String, String>>();
		private int topNumbers;
		private String ip;
		public FedJobServerClient client;
		
		public LineRecordWriter(List<HdfsWriter<String, String>> hw, int top, String ip){
			mHdfsWriter = hw;
			topNumbers = top;
			
			InetAddress address;
			try {
				address = InetAddress.getByName(ip);
				client = new FedJobServerClient(address.getHostAddress(), 8769);
				client.start();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (HdfsWriter<String,String> HW : mHdfsWriter) {
				HW.init();
			}
		}

		private String generateFileName(String key, int topNumbers){
			int hash = 0 ;	   
			hash = (key.hashCode() & Integer.MAX_VALUE) % topNumbers;
		    return Integer.toString(hash);
		}
		private int iterations = 0;
		private Map<String, String> keyMap = new HashMap<String, String>();
		private String mSeperator = "||";


		public synchronized void write(K key, V value) throws IOException {
			if (keyMap.size() > 200) {
				System.out.println("SECTION 1");
				System.out.println("keyMap size:" + keyMap.size() );
				for (Iterator<Map.Entry<String, String>> iter = keyMap.entrySet().iterator(); iter.hasNext();)
				{
					Map.Entry<String, String> entry = iter.next();
					String vvalue = entry.getValue();
					String kkey = entry.getKey();
					
					System.out.println("PRINT 1:" + kkey+ "===" + vvalue);
					HdfsWriter<String, String> hw = (HdfsWriter<String, String>) mHdfsWriter.get(Integer.parseInt(generateFileName(kkey,topNumbers)));
					hw.write(kkey, vvalue);

				}
				keyMap.clear();
				System.out.println("delete size:" + keyMap.size() );
			}
			String mCheckKey = new String();
			mCheckKey = key.toString();
			if (keyMap.containsKey(mCheckKey)) {
				System.out.println("Contains key:" + mCheckKey.toString());
				String ssb = keyMap.get(mCheckKey);
				ssb += value.toString()+mSeperator;
				keyMap.put(mCheckKey, ssb);
			}
			else{
				System.out.println("Not Contains key:" + mCheckKey.toString());
				String ssb = new String();
				ssb+=value.toString()+mSeperator;
				keyMap.put(mCheckKey, ssb);
			}
			//HdfsWriter<String, String> hw = (HdfsWriter<String, String>) mHdfsWriter.get(Integer.parseInt(generateFileName(key,topNumbers)));
			//hw.write(key, value);
			
		}

		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			System.out.println("SECTION 2");
			System.out.println("keyMap size:" + keyMap.size() );
			for (Iterator<Map.Entry<String, String>> iter = keyMap.entrySet().iterator(); iter.hasNext();)
			{
				Map.Entry<String, String> entry = iter.next();
				String vvalue = entry.getValue();
				String kkey = entry.getKey();
				
				System.out.println("PRINT 1:" + kkey+ "===" + vvalue);
				HdfsWriter<String, String> hw = (HdfsWriter<String, String>) mHdfsWriter.get(Integer.parseInt(generateFileName(kkey,topNumbers)));
				hw.write(kkey, vvalue);

			}
			keyMap.clear();
			for (HdfsWriter HW : mHdfsWriter) {
				HW.out.close();
				HW.client.close();
			}
			
			client.stopClientProbe();

		}
	}
	
	

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		Configuration conf = job.getConfiguration();
		namenode = conf.get("fs.default.name");
		String ip = conf.get("fedCloudHDFS").split(":")[1].split("/")[2];

		
		 
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
	
		return new LineRecordWriter<K, V>(mHdfsWriterList, topNumbers, ip);
	}

}
