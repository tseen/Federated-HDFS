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




public class HashMapGenericProxyReducer<T1,T2> extends Reducer<T1,T2,Text,Text> {
	
	public FedJobServerClient client;

	private StringBuffer sb = new StringBuffer();
    private Text mKey = new Text();
	private Text mValue = new Text();
    private Text mCheckKey = new Text();


	private int count =0;
	private int MAX_COUNT = 999;
    private String mSeperator = "||";
    private String namenode = "";
    
    private String generateFileName(Text mKey2) {
    	
    	return TopCloudHasher.generateFileName(mKey2);
    	}
    	  
    	
  //  private MultipleOutputs mos;
  //  private TachyonFileSystem tfs;
    
    private void __reset() {
	    sb.setLength(0);  // clean up the String buffer
        count = 0;
    }

    private Class<T1> mKeyClz ;
    private Class<T2> mValueClz;
    private String mKeyClzName ;
    private String mValueClzName ; 
    private List<HdfsWriter> mHdfsWriter = new ArrayList<HdfsWriter>();
 //   private String tachyonOutDir;
    
    public HashMapGenericProxyReducer(Class<T1> keyClz, Class<T2> valueClz) throws Exception {
        mKeyClz = keyClz;
        mValueClz = valueClz;
        mKeyClzName = mKeyClz.getCanonicalName();
        mValueClzName = mValueClz.getCanonicalName();
    }
    

  
    
 /*   public void setTachyonOutDir(String dir){
    	tachyonOutDir = dir;
    	}
   */ 
  //  private List<FileOutStream> outList;
   // private List<String> outPath;
  //  private Map<String, FileOutStream>  outMap = new HashMap<String, FileOutStream>();

    @Override
	public void setup(Context context) throws UnknownHostException{
        System.out.println("Start Proxy Reducer");
        
		Configuration conf = context.getConfiguration();
		namenode = conf.get("fs.default.name");
		String ip = conf.get("fedCloudHDFS").split(":")[1].split("/")[2];
		
		InetAddress address = InetAddress.getByName(ip);
		client = new FedJobServerClient(address.getHostAddress(),8769);
		client.start();
		
		if(conf.get("topCounts")!=null){
			TopCloudHasher.topCounts = Integer.parseInt(conf.get("topCounts"));
		}
		
		System.out.println("topCOUNT:"+TopCloudHasher.topCounts );
		
		List<String> mTopCloudHDFSURLs = new ArrayList<String>();
		String topCloudHdfs[] = conf.get("topCloudHDFSs").split(",");
		for (int i = 0; i < topCloudHdfs.length; i++) {
			mTopCloudHDFSURLs.add(topCloudHdfs[i]);
		}
		TopCloudHasher.topURLs = mTopCloudHDFSURLs;
		
		
		for(String url: mTopCloudHDFSURLs){
			HdfsWriter HW = new HdfsWriter(url, "hpds");
			HW.setFileName("/user/" + System.getProperty("user.name") +"/"+ conf.get("regionCloudOutput", "")+TopCloudHasher.hashToTop(url+"/"));
						
			mHdfsWriter.add(HW);
		}
		
		for(HdfsWriter HW : mHdfsWriter){
			HW.init();
		}
	
//		mos = new MultipleOutputs(context);
		
		if(conf.get("max_token")!=null) {
            MAX_COUNT = 999;
            try {
			    MAX_COUNT =Integer.parseInt(conf.get("max_token"));
                System.out.println("set max count : " + MAX_COUNT );
            } catch ( Exception e ) {
            }
		}
		if(conf.get("proxy_seperator")!=null) {
            try {
			    mSeperator = conf.get("proxy_seperator");
            } catch ( Exception e ) {
                mSeperator = "||";
            }
		}
        __reset();
	}
    private Map<Text, StringBuffer> keyMap = new HashMap<Text, StringBuffer>();

    @Override	
	public void reduce(T1 key, Iterable<T2> values, Context context) 
        throws IOException,InterruptedException{
    	client.sendRegionMapFinished(namenode.split("/")[2]);
        Iterator<T2> it = values.iterator();
        __reset();
       
        while(it.hasNext()) {	
            T2 value = it.next();
            if ( mKeyClzName.contains("DoubleWritable") ) {
                DoubleWritable tKey = (DoubleWritable) key;
                mCheckKey.set(String.valueOf(tKey.get()));

            } else if ( mKeyClzName.contains("FloatWritable") ) {
                FloatWritable tKey = (FloatWritable) key;
                mCheckKey.set(String.valueOf(tKey.get()));

            } else if ( mKeyClzName.contains("IntWritable") ) {
                IntWritable tKey = (IntWritable) key;
                mCheckKey.set(String.valueOf(tKey.get()));

            } else if ( mKeyClzName.contains("LongWritable") ) {
                LongWritable tKey = (LongWritable) key;
                mCheckKey.set(String.valueOf(tKey.get()));

            } else if ( mKeyClzName.contains("Text") ) {
                Text tKey = (Text) key;
                mCheckKey.set(tKey.toString());

            } else if ( mKeyClzName.contains("UTF8") ) {
                UTF8 tKey = (UTF8) key;
                mCheckKey.set(tKey.toString());

            } else if ( mKeyClzName.contains("VIntWritable") ) {
                VIntWritable tKey = (VIntWritable) key;
                mCheckKey.set(String.valueOf(tKey.get()));

            } else if ( mKeyClzName.contains("VLongWritable") ) {
                VLongWritable tKey = (VLongWritable) key;
                mCheckKey.set(String.valueOf(tKey.get()));
            } else{
            	mCheckKey.set(key.toString());
            }
        	//mCheckKey.set(key.toString());
            if(keyMap.size()>200){
            	 for (Map.Entry<Text, StringBuffer> entry : keyMap.entrySet())
                 {
                 	if(entry.getValue().length() > 0){
                 		StringBuffer ssb = entry.getValue();
                 		mValue.set(ssb.toString());
                        HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(entry.getKey())));
                      	HW.write(entry.getKey(), mValue);
                        ssb.setLength(0); 
                        keyMap.remove(entry.getKey());
                        }
                 }
            }
        	if(keyMap.get(mCheckKey)!=null){
        		StringBuffer ssb = keyMap.get(mCheckKey);
	          	//HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(mKey)));
	            //----------------------------------------------------------
	            // Partial Generic Mapper Value Part
	            if ( mValueClzName.contains("DoubleWritable") ) {
	                DoubleWritable tValue = (DoubleWritable) value;
	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
	                System.out.println("DoubleWritable : " + tValue.get());
	
	            } else if ( mValueClzName.contains("FloatWritable") ) {
	                FloatWritable tValue = (FloatWritable) value;
	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
	                System.out.println("FloatWritable : " + tValue.get());
	
	            } else if ( mValueClzName.contains("IntWritable") ) {
	                IntWritable tValue = (IntWritable) value;
	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
	                System.out.println("IntWritable : " + tValue.get());
	
	            } else if ( mValueClzName.contains("LongWritable") ) {
	                LongWritable tValue = (LongWritable) value;
	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
	                System.out.println("LongWritable : " + tValue.get());
	
	            } else if ( mValueClzName.contains("Text") ) {
	                Text tValue = (Text) value;
	                ssb.append(tValue.toString()).append(mSeperator);
	               // mValue.set(tValue.toString());
	              	//HW.write(mKey, mValue);
	                System.out.println("Text : " + tValue.toString());
	
	            } else if ( mValueClzName.contains("UTF8") ) {
	                UTF8 tValue = (UTF8) value;
	                ssb.append(tValue.toString()).append(mSeperator);
	                System.out.println("UTF8 : " + tValue.toString());
	
	            } else if ( mValueClzName.contains("VIntWritable") ) {
	                VIntWritable tValue = (VIntWritable) value;
	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
	                System.out.println("VIntWritable : " + tValue.get());
	
	            } else if ( mValueClzName.contains("VLongWritable") ) {
	                VLongWritable tValue = (VLongWritable) value;
	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
	                System.out.println("VLongWritable : " + tValue.get());
	            } else{
	            	T1 tValue = (T1) value;
	            	ssb.append(tValue.toString()).append(mSeperator);
	
	            }
	            //----------------------------------------------------------
	            keyMap.put(mCheckKey, ssb);
	            
	            
	            if ( ssb.length() > 20000 ) {
	 
	            	
	                mValue.set(ssb.toString());
	            	HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(mCheckKey)));
	            	HW.write(mCheckKey, mValue);
	          
	            	ssb.setLength(0);
	            	keyMap.put(mCheckKey, ssb);
	            	}
	        }
        	else{
        		StringBuffer ssb = new StringBuffer();
        		 if ( mValueClzName.contains("DoubleWritable") ) {
 	                DoubleWritable tValue = (DoubleWritable) value;
 	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
 	
 	            } else if ( mValueClzName.contains("FloatWritable") ) {
 	                FloatWritable tValue = (FloatWritable) value;
 	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
 	
 	            } else if ( mValueClzName.contains("IntWritable") ) {
 	                IntWritable tValue = (IntWritable) value;
 	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
 	
 	            } else if ( mValueClzName.contains("LongWritable") ) {
 	                LongWritable tValue = (LongWritable) value;
 	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
 	
 	            } else if ( mValueClzName.contains("Text") ) {
 	                Text tValue = (Text) value;
 	                ssb.append(tValue.toString()).append(mSeperator);
 	
 	            } else if ( mValueClzName.contains("UTF8") ) {
 	                UTF8 tValue = (UTF8) value;
 	                ssb.append(tValue.toString()).append(mSeperator);
 	
 	            } else if ( mValueClzName.contains("VIntWritable") ) {
 	                VIntWritable tValue = (VIntWritable) value;
 	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
 	
 	            } else if ( mValueClzName.contains("VLongWritable") ) {
 	                VLongWritable tValue = (VLongWritable) value;
 	                ssb.append(String.valueOf(tValue.get())).append(mSeperator);
 	            } else{
 	            	T1 tValue = (T1) value;
 	            	ssb.append(tValue.toString()).append(mSeperator);
 	
 	            }
         		keyMap.put(mCheckKey, ssb);
 	        
        	}
        }
        for (Map.Entry<Text, StringBuffer> entry : keyMap.entrySet())
        {
        	if(entry.getValue().length() > 0){
        		StringBuffer ssb = entry.getValue();
        		mValue.set(ssb.toString());
                HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(entry.getKey())));
             	HW.write(entry.getKey(), mValue);
                //ssb.setLength(0);
                keyMap.remove(entry.getKey());
            	//keyMap.put(entry.getKey(), ssb);
        	}
        }
  
	}
    protected void cleanup(Context context) throws IOException, InterruptedException {
  
    	for(HdfsWriter HW : mHdfsWriter){
			HW.out.close();
			HW.client.close();
    	}
    	client.stopClientProbe();
    
    	 }
}
