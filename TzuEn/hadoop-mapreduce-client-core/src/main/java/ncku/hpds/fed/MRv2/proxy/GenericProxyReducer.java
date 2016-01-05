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




public class GenericProxyReducer<T1,T2> extends Reducer<T1,T2,Text,Text> {
	
	public FedJobServerClient client;

	private StringBuffer sb = new StringBuffer();
    private Text mKey = new Text();
	private Text mValue = new Text();   

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
    
    public GenericProxyReducer(Class<T1> keyClz, Class<T2> valueClz) throws Exception {
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

    @Override	
	public void reduce(T1 key, Iterable<T2> values, Context context) 
        throws IOException,InterruptedException{
    	client.sendRegionMapFinished(namenode.split("/")[2]);
        Iterator<T2> it = values.iterator();
        __reset();
        //----------------------------------------------------------
        // Partial Generic Mapper Keypart 
        if ( mKeyClzName.contains("DoubleWritable") ) {
            DoubleWritable tKey = (DoubleWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("FloatWritable") ) {
            FloatWritable tKey = (FloatWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("IntWritable") ) {
            IntWritable tKey = (IntWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("LongWritable") ) {
            LongWritable tKey = (LongWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("Text") ) {
            Text tKey = (Text) key;
            mKey.set(tKey.toString());

        } else if ( mKeyClzName.contains("UTF8") ) {
            UTF8 tKey = (UTF8) key;
            mKey.set(tKey.toString());

        } else if ( mKeyClzName.contains("VIntWritable") ) {
            VIntWritable tKey = (VIntWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("VLongWritable") ) {
            VLongWritable tKey = (VLongWritable) key;
            mKey.set(String.valueOf(tKey.get()));
        } else{
        	mKey.set(key.toString());
        }
        System.out.println("mKey.toString : " + mKey.toString());
    	
 //       FileOutStream out = null;
  //  	TachyonURI path = null;
        while(it.hasNext()) {	
            T2 value = it.next();
            //sb.append(value).append(mSeperator);   
            //----------------------------------------------------------
            // Partial Generic Mapper Value Part
            if ( mValueClzName.contains("DoubleWritable") ) {
                DoubleWritable tValue = (DoubleWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("DoubleWritable : " + tValue.get());

            } else if ( mValueClzName.contains("FloatWritable") ) {
                FloatWritable tValue = (FloatWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("FloatWritable : " + tValue.get());

            } else if ( mValueClzName.contains("IntWritable") ) {
                IntWritable tValue = (IntWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("IntWritable : " + tValue.get());

            } else if ( mValueClzName.contains("LongWritable") ) {
                LongWritable tValue = (LongWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("LongWritable : " + tValue.get());

            } else if ( mValueClzName.contains("Text") ) {
                Text tValue = (Text) value;
                sb.append(tValue.toString()).append(mSeperator);
                System.out.println("Text : " + tValue.toString());

            } else if ( mValueClzName.contains("UTF8") ) {
                UTF8 tValue = (UTF8) value;
                sb.append(tValue.toString()).append(mSeperator);
                System.out.println("UTF8 : " + tValue.toString());

            } else if ( mValueClzName.contains("VIntWritable") ) {
                VIntWritable tValue = (VIntWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("VIntWritable : " + tValue.get());

            } else if ( mValueClzName.contains("VLongWritable") ) {
                VLongWritable tValue = (VLongWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("VLongWritable : " + tValue.get());
            } else{
            	T1 tValue = (T1) value;
            	sb.append(tValue.toString()).append(mSeperator);

            }
            //----------------------------------------------------------
            count++;
            
            if ( count == MAX_COUNT ) {
   /*         	path = new TachyonURI(tachyonOutDir+generateFileName(mKey));
            	if(!outMap.containsKey(path.toString())){
            		try {
						out = tfs.getOutStream(path);
					} catch (FileAlreadyExistsException e) {
						e.printStackTrace();
					} catch (InvalidPathException e) {
						e.printStackTrace();
					} catch (TachyonException e) {
						e.printStackTrace();
					}
            		outMap.put(path.toString(), out);
            	}
            	else{
            		out = outMap.get(path.toString());
            	}*/
            	
            	/*
            	try {
    				out = tfs.getOutStream(path);
    			} catch (FileAlreadyExistsException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			} catch (InvalidPathException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			} catch (TachyonException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    			*/
        //    	out.write("\n".getBytes());
    //		    	out.write(mKey.getBytes());
  //          	out.write("\t".getBytes());
//            	out.write(mValue.getBytes());
                mValue.set(sb.toString());
            	HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(mKey)));
            	HW.write(mKey, mValue);
            //	HW.writeByte(mKey.getBytes());
            //	HW.writeByte("\t".getBytes());
               // HW.writeByte(mValue.getBytes());
              //  HW.writeByte("\n".getBytes());


            	//mos.write(mKey, mValue, generateFileName(mKey));
               // context.write(mKey, mValue);
                __reset();
            }
        }
        if ( sb.length() > 0 ) {

            mValue.set(sb.toString());
            HdfsWriter<Text, Text> HW = mHdfsWriter.get(Integer.parseInt(generateFileName(mKey)));
        	HW.write(mKey, mValue);
          /*  HW.writeByte(mKey.getBytes());
            HW.writeByte("\t".getBytes());
            HW.writeByte(mValue.getBytes());
            HW.writeByte("\n".getBytes());
*/
        /*    
        	HW.writeUTF(mKey.toString());
        	HW.writeUTF("\t");
        	HW.writeUTF(mValue.toString());
        	HW.writeUTF("\n");
        	*/
        	//mos.write(mKey, mValue, generateFileName(mKey));
   /*     	
        	path = new TachyonURI(tachyonOutDir+generateFileName(mKey));
        	if(!outMap.containsKey(path.toString())){
        		try {
					out = tfs.getOutStream(path);
				} catch (FileAlreadyExistsException e) {
					e.printStackTrace();
				} catch (InvalidPathException e) {
					e.printStackTrace();
				} catch (TachyonException e) {
					e.printStackTrace();
				}
        		outMap.put(path.toString(), out);
        	}
        	else{
        		out = outMap.get(path.toString());
        		System.out.println("out.toString:"+ out.toString());
        	}*/
        /*	path = new TachyonURI(tachyonOutDir+generateFileName(mKey));
        	try {
				out = tfs.getOutStream(path);
			} catch (FileAlreadyExistsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidPathException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TachyonException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	*/
      //  	out.write("\n".getBytes());
        //	out.write(mKey.getBytes());
       // 	out.write("\t".getBytes());
       // 	out.write(mValue.getBytes());


          //  context.write(mKey, mValue);
            __reset();
        }
      //  out.close();
	}
    protected void cleanup(Context context) throws IOException, InterruptedException {
  /*  		System.out.println("Map Count:" + outMap.size());
    		
    		for(Map.Entry<String, FileOutStream> entry : outMap.entrySet()){
    			entry.getValue().close();
    		}*/
    	for(HdfsWriter HW : mHdfsWriter){
			HW.out.close();
			HW.client.close();
    	}
    	client.stopClientProbe();
    //	   mos.close();
    //	   out.close();
    	 }
}
