/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

//import ncku.hpds.hadoop.fedhdfs.HdfsInfoCollector;
//import ncku.hpds.hadoop.fedhdfs.TopcloudSelector;
//import ncku.hpds.hadoop.fedhdfs.shell.GetRegionPath;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class FedJobConfHdfs extends AbstractFedJobConf {
	private static String DEFAULT_COWORKING_CONF = "/conf/coworking.xml";
	private static String FS_DEFAULT_NAME_KEY = "fs.default.name";
	private String mHadoopHome = "";
	private String mDefaultCoworkingConf = DEFAULT_COWORKING_CONF;

	// private FedTopCloudJob mTopCloudJob = null;
	private List<FedTopCloudJob> mTopJobList = new ArrayList<FedTopCloudJob>();
	private List<FedRegionCloudJob> mRegionJobList = null;
	private List<FedRegionCloudJobDistcp> mRegionJobDistcpList = null;
	private List<JarCopyJob> mJarCopyJobList = null;
	private List<FedCloudMonitorClient> mFedCloudMonitorClientList = null;
	private List<String> mTopCloudHDFSURLs = null;
	private Map<String, FedCloudInfo> mFedCloudInfos = new HashMap<String, FedCloudInfo>();

	private FedHdfsConfParser mParser;
	private boolean mTopCloudFlag = false;
	private boolean mFedFlag = false;
	private boolean mFedSubmitFlag = false;
	private boolean mRegionCloudFlag = false;
	private boolean mFedLoopFlag = false;
	private boolean mFedTestFlag = false;
	private boolean mRegionCloudDone = false;
	private ProxySelector mSelector;
	private String mCoworkingConf = "";
	private String mTopCloudHDFSURL = "";
	private Configuration mJobConf = null;
	private Job mJob = null;
	private int mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I;
	private String mRegionCloudOutputPath = "";
	private String mRegionCloudHadoopHome = "";
	private Path[] mRegionCloudOutputPaths = null;
	private String mTopHost = "";
	private Map<String, String> userConfig = new HashMap<String,String>();
	private String mTopTaskNumbers = "";

	public FedJobConfHdfs(Configuration jobConf, Job job, String inputName)
			throws Throwable {
		// if command cotain "-D fed=on -D fedHdfs=on"
		mJob = job;
		mHadoopHome = System.getenv("HADOOP_HOME");
		System.out.println("Hadoop Home Path : " + mHadoopHome);
		if (mHadoopHome != null) {
			mDefaultCoworkingConf = mHadoopHome + mDefaultCoworkingConf;
		}

		mJobConf = jobConf;
		Iterator<Entry<String, String>> confIter = mJobConf.iterator();
		while(confIter.hasNext()){
			Entry<String, String> e = confIter.next();
			if(e.getKey().contains("mapreduce.")||
					e.getKey().contains("yarn.")||
					e.getKey().contains("ipc.")||
					e.getKey().contains("dfs.")||
					e.getKey().contains("fs.")||
					e.getKey().contains("hadoop.")||
					e.getKey().contains("io.")||
					e.getKey().contains("net.")||
					e.getKey().contains("s3native.")||
					e.getKey().contains("tachyon")||
					e.getKey().contains("ha.")||
					e.getKey().contains("file.")||
					e.getKey().contains("ftp.")||
					e.getKey().contains("rpc.")||
					e.getKey().contains("Arg")||
					e.getKey().contains("s3.")||
					e.getKey().contains("mapred.")||
					e.getKey().contains("main")||
					e.getKey().contains("fed")||
					e.getKey().contains("map."))
			{}
			else{
				userConfig.put(e.getKey(), e.getValue());
			System.out.println("confIter:"+e.toString());
			}
		}
		try {
			Class outputFormat = mJob.getOutputFormatClass();
			// mJob.setMapperClass ( mSelector.getProxyMapperClass( keyClz,
			// valueClz ));
			System.out.println("###### outputFormat.getCanonicalName() = "
					+ outputFormat.getCanonicalName());
		} catch (Exception e) {
		}

		String fedHdfs = mJobConf.get("fedHdfs", "off");
		System.out.println("fedHdfs = " + fedHdfs);

		if (fedHdfs.toLowerCase().equals("on")
				|| fedHdfs.toLowerCase().equals("true")) {
			mFedFlag = true;

		}

		String globalfileInput = "";
		// Select top cloud
		Path[] Inputs = FileInputFormat.getInputPaths((JobConf) mJobConf);
		String multiMapper = mJobConf.get(MultipleInputs.DIR_MAPPERS);
		String multiFormat = mJobConf.get(MultipleInputs.DIR_FORMATS);
		boolean multiInput = true;
		if (multiMapper == null) {
			multiInput = false;
		}
		if(multiInput){
			multiMapper = multiMapper.replace(';', '=');
			multiMapper = multiMapper.replace('$', '+');
	
			multiFormat = multiFormat.replace(';', '=');
			System.out.println("MultipleFormats:" + multiFormat);
			System.out.println("MultipleInputs:" + multiMapper);
		}
		if (Inputs.length > 0) {
			String p[] = Inputs[0].toString().split("/");
			for(int i = 0; i<p.length;i++){
				if(i >2){
					globalfileInput += "/";
					globalfileInput += p[i];	
				}
			}
		//	globalfileInput = p[p.length - 1];
			//globalfileInput = Inputs[0].toString();
			System.out.println("Global File Name:" + globalfileInput);
		}

		//HdfsInfoCollector thisCluster = new HdfsInfoCollector();
		/*
		 * TopcloudSelector top; String realTop = ""; try { top = new
		 * TopcloudSelector(globalfileInput, false); top.show(); realTop =
		 * top.getTopCloud();
		 * 
		 * } catch (Throwable e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
		// System.out.println("Top Cloud of FedMR:" + realTop);

		// check coworking configuration existed or not,
		// if not existed, use the default coworking configuration
		System.out.println("mFedFlag = " + mFedFlag);
		if (mFedFlag) {
			// String mCoworkingConf =
			// mJobConf.get("fedconf",mDefaultCoworkingConf);
			String mCoworkingConf = "etc/hadoop/fedhadoop-clusters.xml";

			File conF = new File(mCoworkingConf);
			if (conF.exists()) {
				// if ( mCoworkingConf.equals(mDefaultCoworkingConf) == false )
				// {
				// java.io.File coworkingFile = new
				// java.io.File(mCoworkingConf);
				// if ( coworkingFile.exists() == false ) {
				// mCoworkingConf = mDefaultCoworkingConf;
				// }
				// }
				mParser = new FedHdfsConfParser(mCoworkingConf);
				// parse xml file

				mParser.parse();
				// add Region Cloud Job
				System.out.println("make FedRegionCloudJobs");
				// make RegionCloudJob
				mRegionJobList = new ArrayList<FedRegionCloudJob>();
				// get some top cloud configuration from region cloud

				/*
				 * for (FedHadoopConf conf : mParser.getRegionCloudConfList()) {
				 * if
				 * (conf.getName().equalsIgnoreCase(mJobConf.get(FS_DEFAULT_NAME_KEY
				 * ,""))) {
				 * 
				 * mTopCloudHDFSURL = "hdfs://" + conf.getTopCloudHDFSURL() +
				 * "/"; } }
				 */

				mTopCloudHDFSURL = mJobConf.get(FS_DEFAULT_NAME_KEY, "");
				// -----
				String submitJarPath = mJob.getJar();
				int lastSlash = submitJarPath.lastIndexOf("/"); // in unix/linux
																// environment
				String regJarFileName = submitJarPath.substring(lastSlash + 1);
				System.out.println("jar file name:" + regJarFileName);

				// -----
				String main = mJobConf.get("main", "");
				System.out.println("main class name:" + main);
				// -----
				mTopTaskNumbers = mJobConf.get("topNumbers", Integer.toString(mParser.getRegionCloudConfList().size()));
				// -----
				String[] arg = new String[10];
				for (int j = 0; j < 10; j++) {
					String argTag = "Arg" + String.valueOf(j);
					arg[j] = mJobConf.get(argTag, "none");
				}

			//	GetRegionPath fedHdfsInputGetter = new GetRegionPath();
				for (FedHadoopConf conf : mParser.getRegionCloudConfList()) {
					mTopCloudHDFSURLs = conf.getTopCloudHDFSURLs();
					FedCloudInfo fedinfo = new FedCloudInfo(
							conf.getHDFSUrl());
			//		long dataSize = thisCluster.getDataSize(globalfileInput,
			//				conf.getName());
					
					Configuration tmpconf = new Configuration();
					tmpconf.set("fs.defaultFS", "hdfs://" + conf.getHDFSUrl());
					recursivelySumOfLen(globalfileInput, tmpconf );
					long dataSize = getSumOfLen();
					System.out.println(fedinfo.getCloudName() +",file:"+ globalfileInput +",size:" + dataSize);
					fedinfo.setMapInputSize(dataSize);
					mFedCloudInfos.put(conf.getHDFSUrl(), fedinfo);
					if(multiInput){
						conf.setMultiFormat(multiFormat);
						conf.setMultiMapper(multiMapper);
					}
					conf.setRegionCloudServerListenPort(mParser
							.getRegionCloudServerListenPort());
					conf.setTopCloudHDFSURL(mTopCloudHDFSURL);
					// set jar path
					String remoteJarPath = conf.getHadoopHome();
					remoteJarPath = remoteJarPath + "/fed_task/"
							+ regJarFileName;
					conf.setJarPath(remoteJarPath);
					conf.setMainClass(main);
					conf.setTopTaskNumbers(mTopTaskNumbers);
					String wanOpt = mJobConf.get("wanOpt", "off");
					conf.setWanOpt(wanOpt);
					String proxyReduce = mJobConf.get("proxyReduce", "off");
					conf.setProxyReduce(proxyReduce);
					// TODO configure the input of region cloud ( fedHdfs)
					if (!multiInput) {
						String inputpath = "";
					/*	try{
							inputpath = fedHdfsInputGetter.getRegionPath(
								conf.getName(), "AirDrive/" + globalfileInput)
								.toString();
							System.out.println("set" + conf.getName()
									+ "Input Path:" + inputpath);
							}catch (NullPointerException e) {
							inputpath = globalfileInput;
							}
							*/
						inputpath = globalfileInput;

						conf.setHDFSInputPath(inputpath);
					}
					// --
					conf.setHDFSOutputPath(conf.getName() + "_"
							+ conf.getMainClass() + "_OUT_"
							+ mParser.getSubmittedTime());
					for (int j = 0; j < 10; j++) {
						conf.setArgs(j, arg[j]);
					}
					conf.setUserConfig(userConfig);
					FedRegionCloudJob regionJob = new FedRegionCloudJob(conf);
					mRegionJobList.add(regionJob);
				}

				if (mRegionJobList.size() > 0) {
					mRegionCloudOutputPaths = new Path[mRegionJobList.size()];
					int i = 0;
					for (FedHadoopConf conf : mParser.getRegionCloudConfList()) {
						String remote_path = conf.getHDFSOutputPath();
						System.out.println("add path [" + remote_path
								+ "] in InputPaths");
						mRegionCloudOutputPaths[i] = new Path(remote_path);
						i++;
					}
				}
				System.out.println("FedRegionCloudJobs size : "
						+ mRegionJobList.size());

				System.out.println("make FedTopCloudJob");
				// top cloud
				// FedHadoopConf topConf = mParser.getTopCloudConf();

				JobConf jobconf = new JobConf(mJobConf);
				for (FedHadoopConf topConf : mParser.getTopCloudConfList()) {

					topConf.setRole(FedHadoopConf.ROLE.TopCloud);
					for (int j = 0; j < 10; j++) {
						topConf.setArgs(j, arg[j]);
					}
					// topConf.setName(realTop);
					// topConf.setAddress(topAddress);
					// topConf.setHadoopHome(topHome);
					// String srcHDFS = mJobConf.get(FS_DEFAULT_NAME_KEY,"");
					String topInputPath = "";

					topInputPath = mRegionCloudOutputPaths[0].toString();

					for (int i = 1; i < mRegionCloudOutputPaths.length; i++) {
						topInputPath += ","
								+ mRegionCloudOutputPaths[i].toString();
					}
					String remoteJarPath = topConf.getHadoopHome();
					remoteJarPath = remoteJarPath + "/fed_task/"
							+ regJarFileName;
					topConf.setJarPath(remoteJarPath);
					topConf.setMainClass(main);
					topConf.setTopCloudHDFSURL(mTopCloudHDFSURL);
					topConf.setHDFSInputPath(topInputPath);
					topConf.setHDFSOutputPath(FileOutputFormat.getOutputPath(
							jobconf).toString());
					topConf.setJobName("ITER_" + mParser.getSubmittedTime());
					topConf.setUserConfig(userConfig);
					FedTopCloudJob topJob = new FedTopCloudJob(topConf);

					mTopJobList.add(topJob);
				}

				/*
				 * move to the region cloud // add Region Cloud Job Distcp
				 * mRegionJobDistcpList = new
				 * ArrayList<FedRegionCloudJobDistcp>(); for ( FedHadoopConf
				 * conf : mParser.getRegionCloudConfList() ) {
				 * FedRegionCloudJobDistcp regionJobDistcp = new
				 * FedRegionCloudJobDistcp(conf, mJobConf);
				 * mRegionJobDistcpList.add(regionJobDistcp) ; }
				 */
				System.out.println("make FedCloudMonitorClients");
				mFedCloudMonitorClientList = new ArrayList<FedCloudMonitorClient>();
				// make Region Cloud Monitor Job
				for (FedHadoopConf conf : mParser.getRegionCloudConfList()) {
					FedCloudMonitorClient client = new FedCloudMonitorClient(
							conf.getAddress(), Integer.valueOf(conf
									.getRegionCloudServerListenPort()));
					mFedCloudMonitorClientList.add(client);
				}
				// make Job Copy List

				mJarCopyJobList = new ArrayList<JarCopyJob>();
				// if ( topConf.getJarPath().equals(
				// FedJobConfParser.INVALID_VALUE ) == false ) {
				for (FedHadoopConf conf : mParser.getRegionCloudConfList()) {
					JarCopyJob jcj = new JarCopyJob(submitJarPath, conf);
					mJarCopyJobList.add(jcj);
				}
				// }
			}
		}
		// mSelector = new ProxySelector(mJobConf, mJob);
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
		return mJarCopyJobList;
	}

	public Configuration getHadoopJobConf() {
		return mJobConf;
	}

	public String getTopCloudHDFSURL() {
		return mTopCloudHDFSURL;
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

	public void selectProxyReduce() {
		// TODO Auto-generated method stub

	}

	public void selectProxyMap() {
		// TODO Auto-generated method stub

	}

	public List<FedTopCloudJob> getTopCloudJobList() {
		return mTopJobList;
	}

	public String getTopCloudInputPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTopCloudOutputPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRegionCloudInputPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FedTopCloudJob getTopCloudJob() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, FedCloudInfo> getFedCloudInfos() {
		// TODO Auto-generated method stub
		return mFedCloudInfos;
	}

	@Override
	public List<String> getTopCloudHDFSURLs() {
		// TODO Auto-generated method stub
		return mTopCloudHDFSURLs;
	}

	@Override
	public void configIter(String filename, int currentIter) {
		mRegionJobList.clear();
		mTopJobList.clear();
		mFedCloudMonitorClientList.clear();
		for (FedHadoopConf conf : mParser.getRegionCloudConfList()) {
			if(currentIter>1){
				
				FedCloudInfo fedInfo = mFedCloudInfos.get(conf.getHDFSUrl());
				Configuration tmpconf = new Configuration();
				tmpconf.set("fs.defaultFS", "hdfs://" + conf.getHDFSUrl());
				try {
					recursivelySumOfLen(filename, tmpconf );
					//recursivelySumOfLen(filename+ "_" + Integer.toString(currentIter - 1 ), tmpconf );
				} catch (IOException e) {
					e.printStackTrace();
				}
				long dataSize = getSumOfLen();
				//System.out.println(fedInfo.getCloudName() +",file:"+ filename+ "_"
				//		+ Integer.toString(currentIter - 1 ) +",size:" + dataSize);
				for (int i =0; i < mRegionCloudOutputPaths.length; i++) {
					try {
						recursivelySumOfLen(mRegionCloudOutputPaths[i].toString() + "_" + Integer.toString(currentIter-1), tmpconf );
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				long prevReduceInputSize = getSumOfLen();
				
				fedInfo.setReduceInputSize(prevReduceInputSize);
				fedInfo.setReduceSpeed();
				fedInfo.setMapInputSize(dataSize);
				fedInfo.setInterSize_iter();
				conf.setHDFSInputPath(filename);
				//conf.setHDFSInputPath(filename+ "_" + Integer.toString(currentIter - 1 ));
				conf.setIteration("true");
			}

			conf.setHDFSOutputPath(conf.getName() + "_" + conf.getMainClass()
					+ "_OUT_" + mParser.getSubmittedTime() + "_"
					+ Integer.toString(currentIter));
			FedRegionCloudJob regionJob = new FedRegionCloudJob(conf);
			mRegionJobList.add(regionJob);
		}
		for (FedHadoopConf topConf : mParser.getTopCloudConfList()) {

			String topInputPath = mRegionCloudOutputPaths[0].toString() + "_"
					+ Integer.toString(currentIter);

			for (int i = 1; i < mRegionCloudOutputPaths.length; i++) {
				topInputPath += "," + mRegionCloudOutputPaths[i].toString()
						+ "_" + Integer.toString(currentIter);
			}

			topConf.setHDFSInputPath(topInputPath);
			topConf.setHDFSOutputPath(filename + "_"
					+ Integer.toString(currentIter));
			FedTopCloudJob topJob = new FedTopCloudJob(topConf);
			mTopJobList.add(topJob);
		}

		for (FedHadoopConf conf : mParser.getRegionCloudConfList()) {
			FedCloudMonitorClient client = new FedCloudMonitorClient(
					conf.getAddress(), Integer.valueOf(conf
							.getRegionCloudServerListenPort()));
			mFedCloudMonitorClientList.add(client);
		}
		
	}

	@Override
	public void selectProxyMap(Class<?> keyClz, Class<?> valueClz,
			Class<? extends Mapper> mapper) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void selectProxyReduce(Class<?> keyClz, Class<?> valueClz,
			Class<? extends Reducer> mapper) {
		// TODO Auto-generated method stub
		
	}
	private ArrayList<Long> tmpFsLenElement = new ArrayList<Long>();

	public void recursivelySumOfLen(String Uri, Configuration conf) throws IOException {
		
		try {

			FileSystem FS = FileSystem.get(URI.create(Uri), conf);
			FileStatus[] status = FS.listStatus(new Path(Uri));

			for (int i = 0; i < status.length; i++) {
				if (status[i].isDirectory()) {
					recursivelySumOfLen(status[i].getPath().toString(), conf);

				} else {
					try {
						FileStatus fileStatus = FS.getFileStatus(new Path(status[i].getPath().toString()));
						tmpFsLenElement.add(fileStatus.getLen());
					} catch (Exception e) {
						System.err.println(e.toString());
					}
				}
			}
		} catch (IOException e) {
			
		}
	}
	public long getSumOfLen(){
		
		long sumOfLen = 0;
		for (int i = 0; i < tmpFsLenElement.size(); i++) {
			sumOfLen +=  tmpFsLenElement.get(i);
		}
		tmpFsLenElement = new  ArrayList<Long>();
		return sumOfLen;
	}

}
