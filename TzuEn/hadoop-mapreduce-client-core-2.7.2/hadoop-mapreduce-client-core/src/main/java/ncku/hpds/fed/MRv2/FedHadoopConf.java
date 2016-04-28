/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// Region Cloud Configraution
public class FedHadoopConf{
    public enum ROLE {
        None,
        TopCloud,
        RegionCloud
    };
    public static String DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT="55679";
    public static int DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I=55679;
    private ROLE mRole = ROLE.None;
    private String mName="";
    private String mAddress="";
    private String mHadoopHome="";
    private String mJobName = "";
    private String mJarPath="";
    private String mMainClass="";
    private String mHDFSInputPath="";
    private String mHDFSOutputPath="";
    private String mOtherArgs="";
    private String mTopCloudHDFSURL ="";
    private List<String> mTopCloudHDFSURLs = new ArrayList<String>();
    private String mArg[] = new String[10];
    private String mRegionCloudServerListenPort= DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT; 
    private long mInputSize;
    private String multiMapper = "";
    private String multiFormat = "";
	private Map<String, String> userConfig = new HashMap<String,String>();
	private String mTopTaskNumbers = "";
	private String mProxyReduce = "";
	private String mWanOpt = "";


    

    public FedHadoopConf(ROLE role ) {
        mRole = role;
        for ( int i = 0 ; i < 10 ; i++ ) { 
            mArg[i] = "";
        }
    }
    //----------------------------------------------------------
    // setter
    public void setRole(ROLE role){ this.mRole = role; };
    public void setName(String s){ this.mName = s; }
    public void setAddress(String s){ this.mAddress = s; }
    public void setHadoopHome(String s){ this.mHadoopHome= s; }
    public void setJobName(String s){ this.mJobName = s; }
    public void setJarPath(String s){ this.mJarPath = s; }
    public void setMainClass(String s){ this.mMainClass = s; }
    public void setHDFSInputPath(String s){ this.mHDFSInputPath = s; }
    public void setHDFSOutputPath(String s){ this.mHDFSOutputPath = s; }
    public void setOtherArgs(String s){ this.mOtherArgs = s; }
    public void setInputSize(long s){ this.mInputSize = s; }
    public void setTopCloudHDFSURL(String s) { this.mTopCloudHDFSURL = s; }
    public void setTopCloudHDFSURLs(List<String> s) { this.mTopCloudHDFSURLs = s; }
    
    public void addTopCloudHDFSURL(String s){ this.mTopCloudHDFSURLs.add(s);}
    public void setRegionCloudServerListenPort(String s) { mRegionCloudServerListenPort =s ; }
    public void setArgs(int i, String s) { 
        if ( i >= 0 && i <= 9 ) {
            this.mArg[i] = new String(s);
        }
    }

    //----------------------------------------------------------
    // getter
    public String getName(){ return this.mName; }
    public String getAddress(){ return this.mAddress; }
    public String getHadoopHome(){ return this.mHadoopHome; }
    public String getJobName(){ return this.mJobName; }
    public String getJarPath(){ return this.mJarPath ;}
    public String getMainClass(){ return this.mMainClass; }
    public String getHDFSInputPath(){ return this.mHDFSInputPath ; }
    public String getHDFSOutputPath(){ return this.mHDFSOutputPath; }
    public String getOtherArgs(){ return this.mOtherArgs; }
    public String getTopCloudHDFSURL() { return this.mTopCloudHDFSURL; }
    public List<String> getTopCloudHDFSURLs() { return this.mTopCloudHDFSURLs; }
    public long getInputSize(){ return this.mInputSize; }
    public ROLE getRole() { return this.mRole; }  
    public String getRegionCloudServerListenPort() { return mRegionCloudServerListenPort; }
    public String getArgs(int i) {
        if ( i >= 0 && i <= 9 ) {
            return mArg[i];
        }
        return null;
    }
	public String getMultiMapper() {
		return multiMapper;
	}
	public void setMultiMapper(String multiMapper) {
		this.multiMapper = multiMapper;
	}
	public String getMultiFormat() {
		return multiFormat;
	}
	public void setMultiFormat(String multiFormat) {
		this.multiFormat = multiFormat;
	}
	public Map<String, String> getUserConfig() {
		return userConfig;
	}
	public void setUserConfig(Map<String, String> userConfig) {
		this.userConfig = userConfig;
	}
	public String getTopTaskNumbers() {
		return mTopTaskNumbers;
	}
	public void setTopTaskNumbers(String mTopTaskNumbers) {
		this.mTopTaskNumbers = mTopTaskNumbers;
	}
	public String getProxyReduce() {
		return mProxyReduce;
	}
	public void setProxyReduce(String mProxyReduce) {
		this.mProxyReduce = mProxyReduce;
	}
	public String getWanOpt() {
		return mWanOpt;
	}
	public void setWanOpt(String mWanOpt) {
		this.mWanOpt = mWanOpt;
	}
	
}
