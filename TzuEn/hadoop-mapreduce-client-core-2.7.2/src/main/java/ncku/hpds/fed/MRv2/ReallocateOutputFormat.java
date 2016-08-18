/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReallocateOutputFormat<K, V> extends FileOutputFormat<K, V> {
	static int hash(int h) {
		h ^= (h >>> 20) ^ (h >>> 12);
		       return h ^ (h >>> 7) ^ (h >>> 4);
		     }
	public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
	private List<HdfsWriter<K, V>> mHdfsWriterList = new ArrayList<HdfsWriter<K, V>>();
	
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
		private List<HdfsWriter<K, V>> mHdfsWriter = new ArrayList<HdfsWriter<K, V>>();
		private int topNumbers;
		private Class<K> mClazz;
		private Configuration mConf;
		public LineRecordWriter(List<HdfsWriter<K, V>> hw,  Configuration conf){
			mHdfsWriter = hw;
			mConf = conf;
		
			for (HdfsWriter<K, V> HW : mHdfsWriter) {
				HW.init();
			}
			topNumbers = hw.size();
			System.out.println("topNumbers in reallocate = " + topNumbers);

		}

		private String generateFileName(K key, int topNumbers){
			int hash = 0 ;	   
			hash = (key.hashCode() & Integer.MAX_VALUE) % topNumbers;
		    return Integer.toString(hash);
		}
	
		public synchronized void write(K key, V value) throws IOException {
			
		    HdfsWriter<K, V> hw = (HdfsWriter<K, V>) mHdfsWriter.get(Integer.parseInt(generateFileName(key,topNumbers)));
			hw.write(key, value);
		}

		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			
			for (HdfsWriter HW : mHdfsWriter) {
				HW.out.close();
			}

		}
	}
	
	

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		Configuration conf = job.getConfiguration();

		if (conf.get("topCounts") != null) {
			TopCloudHasher.topCounts = Integer.parseInt(conf.get("topCounts"));
		}
		System.out.println(conf.get("topCloudHadoopHome"));
		System.out.println("topCOUNT:" + TopCloudHasher.topCounts);

		List<String> mTopCloudHDFSURLs = new ArrayList<String>();
		String topCloudHdfs[] = conf.get("topCloudHDFSs").split(",");
		for (int i = 0; i < topCloudHdfs.length; i++) {
			mTopCloudHDFSURLs.add(topCloudHdfs[i]);
		}
		TopCloudHasher.topURLs = mTopCloudHDFSURLs;
		int i = 0;
		for (String url : mTopCloudHDFSURLs) {
			HdfsWriter HW = new HdfsWriter(url, "hpds");
			HW.setFileName(conf.get("topCloudOutput", "") +"/"+ i );
			i++;
			mHdfsWriterList.add(HW);
		}

		return new LineRecordWriter<K, V>(mHdfsWriterList, conf);
	}

}


