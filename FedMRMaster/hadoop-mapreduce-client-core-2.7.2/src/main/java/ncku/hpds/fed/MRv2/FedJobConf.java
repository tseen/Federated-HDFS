/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import ncku.hpds.fed.MRv2.proxy.GenericProxyMapper;
import ncku.hpds.fed.MRv2.proxy.GenericProxyReducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.List;
import java.util.ArrayList;
import java.io.File;

public class FedJobConf extends AbstractFedJobConf {
	// ---------------------------------------------------------------------
	private static String DEFAULT_COWORKING_CONF = "/conf/coworking.xml";
	private static String FS_DEFAULT_NAME_KEY = "fs.default.name";
	private String mHadoopHome = "";
	private String mDefaultCoworkingConf = DEFAULT_COWORKING_CONF;

	private List<FedRegionCloudJob> mRegionJobList = null;
	private List<FedRegionCloudJobDistcp> mRegionJobDistcpList = null;
	private List<JarCopyJob> mJarCopyJobList = null;
	private List<FedCloudMonitorClient> mFedCloudMonitorClientList = new ArrayList<FedCloudMonitorClient>();

	private FedJobConfParser mParser;

	private boolean mFedFlag = false;
	private boolean mTopCloudFlag = false;
	private boolean mFedHdfsFlag = false;

	private boolean mRegionCloudFlag = false;
	private boolean mFedLoopFlag = false;
	private boolean mFedTestFlag = false;
	private boolean mRegionCloudDone = false;
	private boolean mFedTachyonFlag = false;
	private boolean mFedIterFlag = false;
	private int mFedIterNum = 1;
	private boolean mMapOnly = false;
  private boolean mWanOpt = false;
	private boolean mProxyReduceFlag = false;
	private AbstractProxySelector mSelector;
	private String mCoworkingConf = "";
	private String mTopCloudHDFSURL = "";
	private List<String> mTopCloudHDFSURLs = new ArrayList<String>();
	private Configuration mJobConf = null;
	private String[] topCloudHDFSString = null;
	private  Job mJob = null;
	private String mTopCloudInputPath = "";
	private String mTopCloudOutputPath = "";
	private String mTopCloudHadoopHome = "";
	private int mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I;
	private String mRegionCloudInputPath = "";
	private String mRegionCloudOutputPath = "";
	private String mRegionCloudHadoopHome = "";
	private Path[] mRegionCloudOutputPaths = null;

	// ---------------------------------------------------------------------
	public FedJobConf(Configuration jobConf, Job job) {
		// if command cotain "-D fed=on"
		mJob = job;
		mHadoopHome = System.getenv("HADOOP_HOME");
		System.out.println("Hadoop Home Path : " + mHadoopHome);
		if (mHadoopHome != null) {
			mDefaultCoworkingConf = mHadoopHome + mDefaultCoworkingConf;
		}

		mJobConf = jobConf;
		try {
			Class outputFormat = mJob.getOutputFormatClass();
			// mJob.setMapperClass ( mSelector.getProxyMapperClass( keyClz,
			// valueClz ));
			System.out.println("###### outputFormat.getCanonicalName() = "
					+ outputFormat.getCanonicalName());
		} catch (Exception e) {
			e.printStackTrace();
		}

		//MapOnly
		if (mJob.getNumReduceTasks() == 0) {
			mMapOnly = true;
		}
		//proxyReduce
		String proxyReduce = mJobConf.get("proxyReduce", "off");
		if (proxyReduce.toLowerCase().equals("on")
				|| proxyReduce.toLowerCase().equals("true")) {
			mProxyReduceFlag = true;
		}
		// isFedMR
		String fed = mJobConf.get("fed","off");
		if ( fed.toLowerCase().equals("on") || fed.toLowerCase().equals("true") ) 
		{ 
			mFedFlag = true; 
		}
		String fedHdfs = mJobConf.get("fedHdfs", "off");
		if (fedHdfs.toLowerCase().equals("on")
				|| fedHdfs.toLowerCase().equals("true")) {
			mFedHdfsFlag = true;
		}
		String fedIter = mJobConf.get("fedIteration", "1");
		try {
			mFedIterNum = Integer.parseInt(fedIter);
		} catch ( Exception e ) {
			mFedIterNum = 1;
		}
		if ( mFedIterNum > 1) {
			System.out.println("-------Fed Iteration------");
			System.out.println("Iterations:" + mFedIterNum );
			mFedIterFlag = true;
		}
		String fedTachyon = mJobConf.get("tachyon", "off");
		if (fedTachyon.toLowerCase().equals("on")
				|| fedTachyon.toLowerCase().equals("true")) {
			mFedTachyonFlag = true;
		}
		if(mJobConf.get("wanOpt","off").equals("true")){
			mWanOpt = true;
		}
		String regionCloud = mJobConf.get("regionCloud", "off");
		// if region cloud mode
		if (regionCloud.toLowerCase().equals("on")
				|| regionCloud.toLowerCase().equals("true")) {
			mFedFlag = true; 
			mRegionCloudFlag = true;
			mTopCloudHDFSURL = mJobConf.get("topCloudHDFSs", "");

			topCloudHDFSString = mTopCloudHDFSURL.split(",");
			for (int i = 0; i < topCloudHDFSString.length; i++) {
				mTopCloudHDFSURLs.add(topCloudHDFSString[i]);
			}

			TopCloudHasher.topURLs = mTopCloudHDFSURLs;
			TopCloudHasher.topCounts = topCloudHDFSString.length;
			System.out.println("TopCloudHDFSURLs:" + mTopCloudHDFSURLs);

			String port = mJobConf.get("regionCloudServerPort",
					FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT);
			try {
				mRegionCloudServerListenPort = Integer.valueOf(port);
			} catch (Exception e) {
				e.printStackTrace();
				mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I;
			}
			setRegionCloudInputPath(mJobConf.get("regionCloudInput", ""));
			mRegionCloudOutputPath = mJobConf.get("regionCloudOutput", "");
			mRegionCloudHadoopHome = mJobConf.get("regionCloudHadoopHome", "");
			//mJob.setPartitionerClass(ncku.hpds.fed.MRv2.Null.NullPartitioner.class);
			//mJob.setSortComparatorClass("");
			//mJob.setGroupingComparatorClass("");
			
		}
		String topCloud = mJobConf.get("topCloud", "off");
		// if top cloud mode
		if (topCloud.toLowerCase().equals("on")
				|| topCloud.toLowerCase().equals("true")) {
			mFedFlag = true; 
			mTopCloudFlag = true;
			// mTopCloudHDFSURL = mJobConf.get("topCloudHDFS","");

			mTopCloudOutputPath = mJobConf.get("topCloudOutput", "");
			mTopCloudInputPath = mJobConf.get("topCloudInput", "");
			mTopCloudHadoopHome = mJobConf.get("topCloudHadoopHome", "");
		}
		if(mJobConf.get("seqInter", "false").equals("true")){
			mSelector = new ProxySelectorSeq(mJobConf, mJob);
		}
		else{	
			mSelector = new ProxySelector(mJobConf, mJob);
		}

	}

	public boolean isFedMR() {
		return mFedFlag;
	}

	public boolean isTopCloud() {
		return mTopCloudFlag;
	}

	public boolean isRegionCloud() {
		return mRegionCloudFlag;
	}

	public boolean isFedLoop() {
		return mFedLoopFlag;
	}

	public boolean isFedTest() {
		return mFedTestFlag;
	}
	
	public boolean isFedHdfs() {
		return mFedHdfsFlag;
	}

	public boolean isFedTachyon() {
		return mFedTachyonFlag;
	}
	
  public boolean isFedIter() {
		return mFedIterFlag;
	}
	
	public int getFedIterNum() {
		return mFedIterNum;
	}

	public boolean isMapOnly() {
		return mMapOnly;
	}
	public boolean isWanOpt() {
		return mWanOpt;
	}
	public boolean isProxyReduce() {
		return mProxyReduceFlag;
	}

	// final Class kClz = mJob.getMapOutputKeyClass();
	// final Class vClz = mJob.getMapOutputValueClass();
	/*
	 * public class proxyReduce extends GenericProxyReducer {
	 * 
	 * @SuppressWarnings("unchecked") public proxyReduce() throws Exception {
	 * super(mJob.getMapOutputKeyClass(),mJob.getMapOutputValueClass()); } }
	 */
	private static boolean userDefine = false;


	public void selectProxyReduce(Class<?> keyClz, Class<?> valueClz, Class<? extends Reducer> reducer) {
		mJob.setReducerClass(reducer);
		mJob.setMapOutputKeyClass(keyClz);
		mJob.setMapOutputValueClass(valueClz);
		//mJob.setOutputKeyClass(Text.class);
		//mJob.setOutputValueClass(Text.class);
		try {
			Class outputFormat = mJob.getOutputFormatClass();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		mJob.setOutputFormatClass(TextOutputFormat.class);
	}
	public void selectProxyReduce() {
		if (!userDefine) {
			try {
				Class keyClz = mJob.getMapOutputKeyClass();
				Class valueClz = mJob.getMapOutputValueClass();
				// Class<? extends Reducer> testR = new
				// GenericProxyReducer(keyClz, valueClz);

				System.out.println("MOK:"
						+ mJob.getMapOutputKeyClass().getName());
				System.out.println("MOV:"
						+ mJob.getMapOutputValueClass().getName());
				try{
					
					mJob.setReducerClass(mSelector.getProxyReducerClass(keyClz,
						valueClz));
				}catch (NullPointerException e) {
					System.out.println("USER DEFINED REDUCER");
				}

				// System.out.println("CLASS:"+mSelector.getProxyReducerClass(keyClz,
				// valueClz).getName());
				mJob.setMapOutputKeyClass(keyClz);
				mJob.setMapOutputValueClass(valueClz);
				mJob.setOutputKeyClass(Text.class);
				mJob.setOutputValueClass(Text.class);
				Class outputFormat = mJob.getOutputFormatClass();
				mJob.setOutputFormatClass(TextOutputFormat.class);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	public void selectProxyMap(Class<?> keyClz, Class<?> valueClz, Class<? extends Mapper> mapper) {
		mJob.setMapperClass(mapper);
	}
	public void selectProxyMap() {
		try {
			Class keyClz = mJob.getMapOutputKeyClass();
			Class valueClz = mJob.getMapOutputValueClass();

		
			try{
				mJob.setMapperClass(mSelector.getProxyMapperClass(keyClz, valueClz));
			}catch (NullPointerException e) {
				System.out.println("USER DEFINED MAPPER");
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getCoworkingConf() {
		return mCoworkingConf;
	};

	public FedHadoopConf getTopCloudConf() {
		return mParser.getTopCloudConf();
	}

	public List<FedHadoopConf> getRegionCloudConfList() {
		return mParser.getRegionCloudConfList();
	}

	public List<FedRegionCloudJob> getRegionCloudJobList() {
		return this.mRegionJobList;
	}

	public List<FedCloudMonitorClient> getFedCloudMonitorClientList() {
		return this.mFedCloudMonitorClientList;
	}

	public List<JarCopyJob> getJarCopyJobList() {
		return this.mJarCopyJobList;
	}

	public Configuration getHadoopJobConf() {
		return mJobConf;
	}

	public String getTopCloudHDFSURL() {
		return mTopCloudHDFSURL;
	}

	public List<String> getTopCloudHDFSURLs() {
		return mTopCloudHDFSURLs;
	}

	public String getTopCloudInputPath() {
		return mTopCloudInputPath;
	}

	public String getTopCloudOutputPath() {
		return mTopCloudOutputPath;
	}

	public int getRegionCloudServerListenPort() {
		return mRegionCloudServerListenPort;
	}

	// getRegionCloudOutputPath is used in Region Cloud mode
	public String getRegionCloudOutputPath() {
		return mRegionCloudOutputPath;
	}

	public String getRegionCloudHadoopHome() {
		return mRegionCloudHadoopHome;
	}

	// getRegionCloudOutputPaths is used in Top Cloud mode instead of original
	// input path
	public Path[] getRegionCloudOutputPaths() {
		return mRegionCloudOutputPaths;
	}

	@Override
	public FedTopCloudJob getTopCloudJob() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getRegionCloudInputPath() {
		return mRegionCloudInputPath;
	}

	public void setRegionCloudInputPath(String mRegionCloudInputPath) {
		this.mRegionCloudInputPath = mRegionCloudInputPath;
	}

	@Override
	public List<FedTopCloudJob> getTopCloudJobList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void configIter(String name, int a) {
		// TODO Auto-generated method stub

	}
}
