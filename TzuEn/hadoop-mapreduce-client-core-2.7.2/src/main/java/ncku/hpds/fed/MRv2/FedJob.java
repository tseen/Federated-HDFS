/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.UnknownHostException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class FedJob {
	/*
	 * FedJob FedJobConf FedJobConfParser
	 */
	private Configuration mJobConf;
	private String mFileName;
	private Job mJob;
	private FedJobStatistics mFedStat = new FedJobStatistics();
	private FedCloudMonitorServer mServer = null;
	private boolean bIsFed = false;
	private boolean bIsFedHdfs = false;
	private boolean bIsFedTachyon = false;
	private boolean bIsFedIteration = false;
	private boolean bIsProxyReduce = false;
	private AbstractFedJobConf mFedJobConf;
	private int iterations = 1;

	public FedJob(Job job) {
		mJob = job;
		mJobConf = mJob.getConfiguration();
		String fed = mJobConf.get("fed", "off");
		if (fed.toLowerCase().equals("on") || fed.toLowerCase().equals("true")) {
			bIsFed = true;
		}
		String regionCloud = mJobConf.get("regionCloud", "off");
		if (regionCloud.toLowerCase().equals("on")
				|| regionCloud.toLowerCase().equals("true")) {
			bIsFed = true;
		}
		String topCloud = mJobConf.get("topCloud", "off");
		if (topCloud.toLowerCase().equals("on")
				|| topCloud.toLowerCase().equals("true")) {
			bIsFed = true;
		}
		String fedHdfs = mJobConf.get("fedHdfs", "off");
		if (fedHdfs.toLowerCase().equals("on")
				|| fedHdfs.toLowerCase().equals("true")) {
			bIsFedHdfs = true;
		}
		String fedIter = mJobConf.get("fedIteration", "1");
		if (Integer.parseInt(fedIter) > 1) {
			System.out.println("-------Fed Iteration------");
			System.out.println("Iterations:" + Integer.parseInt(fedIter));
			bIsFedIteration = true;
			iterations = Integer.parseInt(fedIter);
		}
		String fedTachyon = mJobConf.get("tachyon", "off");
		if (fedTachyon.toLowerCase().equals("on")
				|| fedTachyon.toLowerCase().equals("true")) {
			bIsFedTachyon = true;
		}
	
	}

	public boolean isFedHdfsJob() {
		return bIsFedHdfs;
	}

	public boolean isFedJob() {
		return bIsFed;
	}

	public boolean isFedIteration() {
		return bIsFedIteration;
	}
    
	public void scheduleAndStartFedJob() {
		int currentIter = 1;
		boolean mapOnly = false;
		System.out.println("REDUCE TASK:" + mJob.getNumReduceTasks());
		if (mJob.getNumReduceTasks() == 0) {
			mapOnly = true;
		}
		//else if(mJob.getNumReduceTasks() == 1){
		//	mJobConf.set("topNumbers", "1");
		//}
		try {
			//Start servers
			//FedWANServer wanServer = new FedWANServer();
			FedJobServer jobServer = new FedJobServer(8713);
			jobServer.start();
			//Get input path
			Path[] mInputPaths = FileInputFormat.getInputPaths(mJob);
			if (mInputPaths.length > 0) {
				mFileName = mInputPaths[0].toString();
			}

			System.out.println("Start FedJobConfHdfs");
			mFedJobConf = new FedJobConfHdfs(mJobConf, mJob, mFileName);
			System.out.println("End FedJobConfHdfs");
			//System.out.println("VCORES:"+mJobConf.get("yarn.nodemanager.resource.cpu-vcores", "78"));
			//System.out.println("MEMORY:"+mJobConf.get("yarn.nodemanager.resource.memory-mb", "78"));

			List<JarCopyJob> jcjList = mFedJobConf.getJarCopyJobList();
			HashMap<String, FedCloudInfo> fedCloudInfos = (HashMap<String, FedCloudInfo>) mFedJobConf
					.getFedCloudInfos();
			//wanServer.setFedCloudInfos(fedCloudInfos);
			//wanServer.start();
			jobServer.setFedCloudInfos(fedCloudInfos);
			if (jcjList.size() > 0) {
				System.out.println("Start Jar copy jobs");
				for (JarCopyJob job : jcjList) {
					job.start();
				}
				System.out.println("Wait For Jar copy finishing");
				for (JarCopyJob job : jcjList) {
					job.join();
				}
				System.out.println("Jar copy Jobs finished");
			}

			for (int i = 0; i < iterations; i++) {
				
				if (bIsFedIteration) {
					Path mOutpuPath = FileOutputFormat.getOutputPath(mJob);
					String input[] = mOutpuPath.toString().split("/");
					// mFedJobConf.configIter("/"+input[3]+"/"+input[4]+"/"+input[5],currentIter);
					/*
					 * User-defined the iterative input file, if not defined,
					 * using the output file of the last iteration.
					 */
					String iterInput = mJobConf.get("iterInput", "/" + input[3]
							+ "/" + input[4] + "/" + input[5]);
					System.out.println("iterInput:"+ iterInput);
					if(currentIter < iterations)
						mFedJobConf.configIter(iterInput, currentIter, "false");
					else if(currentIter == iterations)
						mFedJobConf.configIter(iterInput, currentIter, "true");

				}
				
				// get top job
				List<FedTopCloudJob> tList = mFedJobConf.getTopCloudJobList();
				// get region job
				List<FedRegionCloudJob> rList = mFedJobConf
						.getRegionCloudJobList();
				// get region monitor
				List<FedCloudMonitorClient> cList = mFedJobConf
						.getFedCloudMonitorClientList();

				TopCloudHasher.topCounts = rList.size();
				System.out
						.println("TopCloud Report : RegionCloud Start Time = "
								+ mFedStat.getRegionCloudsStart() + "(ms)");
				System.out.println("Start FedRegionCloudJobs");

				for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos
						.entrySet()) {
					
					info.getValue().setRegionMapStartTime(
							(int) System.currentTimeMillis());
					jobServer.resetGetInfo();
					info.getValue().clearInfo();
				}
				mFedStat.setRegionCloudsStart();

				// int regionMapStartTime = (int) System.currentTimeMillis();
				for (FedRegionCloudJob job : rList) {
					System.out.println("REGION START");
					job.start();
				}
				System.out.println("Wait For FedRegionCloudJob Join");

				if(mJobConf.get("wanOpt","off").equals("true")){
					Map<String, String> downSpeed = getDownSpeed(fedCloudInfos);
				//	if(currentIter == 1 )
						normalizeInfo(fedCloudInfos);
					jobServer.setDownSpeed(downSpeed);
				}

				//for (FedCloudMonitorClient job : cList) {
				//	job.start();
				//}
				System.out.println("DownSpeed Ready");

				for (FedRegionCloudJob job : rList) {
					job.join();
					System.out.println("REGION JOIN");
				}
				jobServer.restoreName(fedCloudInfos);
				System.out.println("FedRegionCloudJob All Joined");
				mFedStat.setRegionCloudsEnd();
				System.out.println("TopCloud Report : RegionCloud End Time = "
						+ mFedStat.getRegionCloudsEnd() + "(ms)");
				System.out
						.println("TopCloud Report : RegionCloud Total Time = "
								+ mFedStat.getRegionCloudsTime() + "(ms)");
				if (!mapOnly) {
					System.out.println("----------------------------------");
					System.out.println("Map-ProxyReduce Phrase Finished");
					System.out.println("----------------------------------\n");
					/*
					 * startFedJob() --> stopFedJob() job finished Region Cloud
					 * Notify Top Cloud Top Cloud notify Region Cloud start to
					 * upload result to Top Cloud Record Aggregation Time When
					 * all data is collected then do something Proxy-Map Reducer
					 * Phrase
					 */

					System.out.println("----------------------------------");
					System.out.println("Global Aggregation Start ...");
					System.out.println("----------------------------------");

				// distcp copy from region cloud hdfs to top cloud hdfs
				//	System.out.println("Wait For FedCouldMonitorClient Join");
				//	for (FedCloudMonitorClient job : cList) {
				//		job.join();
				//	}
					System.out.println("FedCouldMonitorClient All Joined");

					System.out
							.println("Top Cloud Report : Global Aggregation Consuming Time :");
					for (FedCloudMonitorClient job : cList) {
						job.printAggregationTime();
					}
					System.out.println("----------------------------------");
					System.out.println("Global Aggregation End ...");
					System.out.println("----------------------------------");
//				 FileSystem hdfs =FileSystem.get(new Configuration());
//					 hdfs.delete(new Path("/user/hpds/zzz"), false);
					System.out.println("----------------------------------");
					System.out.println("TOP START ...");
					System.out.println("----------------------------------");
					mFedStat.setTopCloudStart();

					for (FedTopCloudJob job : tList) {
						if (bIsFedIteration) {
							job.setIterFlag(currentIter);
						}
						job.start();
					}
					for (FedTopCloudJob job : tList) {
						job.join();
					}
					System.out.println("----------------------------------");
					mFedStat.setTopCloudEnd();
					System.out.println("TOP END ...");
					System.out.println("----------------------------------");

	//				HdfsFileSender sender = new HdfsFileSender();
	//				List<String> topCloudHDFSs = mFedJobConf
	//						.getTopCloudHDFSURLs();
	//				sender.send((ArrayList) topCloudHDFSs, "/user/hpds/zzz");
					// String topCloudHDFS = "hdfs://"+topCloudHDFSs.get(0);

					currentIter++;
				}
				long lastWanTime = 0;
				long lastReduceTime = 0;
				for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos
						.entrySet()) {
					FedCloudInfo fedcloudinfo = info.getValue();
					fedcloudinfo.printTime();
					if(fedcloudinfo.getInterTime() > lastWanTime){
						lastWanTime = fedcloudinfo.getInterTime();
					}
					if(fedcloudinfo.getTopTime() > lastReduceTime){
						lastReduceTime = fedcloudinfo.getTopTime();
					}
				}
				for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos
						.entrySet()) {
					FedCloudInfo fedcloudinfo = info.getValue();
					fedcloudinfo.setLastWanTime(lastWanTime);
					fedcloudinfo.setWanWaitingTime();
					System.out.println("WanWaitingTime: " + fedcloudinfo.getCloudName()+" = "+ fedcloudinfo.getWanWaitingTime());
					fedcloudinfo.setLastReduceTime(lastReduceTime);
					fedcloudinfo.setReduceWaitingTime();
					System.out.println("TopWaitingTime: " + fedcloudinfo.getCloudName()+" = "+ fedcloudinfo.getReduceWaitingTime());

				}
				
			}
			jobServer.stopServer();
			jobServer.join();

			
			//wanServer.stopServer();
			//wanServer.join();
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}
	private Map<String, String> getDownSpeed(Map<String, FedCloudInfo>  fedCloudInfos){
		boolean collectAll = false;
		while(!collectAll){
			collectAll = true;
			for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos
					.entrySet()) {
				if(info.getValue().getWanSpeedCount() == TopCloudHasher.topCounts
						&& info.getValue().isMBset()
						&& info.getValue().isNodesset()
						&& info.getValue().isVcoresset())
				{
					collectAll &= true;
				}else
				{
					collectAll = false;
				}		
			}
		}
		Map<String, String> downSpeed = new HashMap<String, String>();
		for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos
				.entrySet()) {
			System.out.println("fedCloudInfos Key:"+info.getKey());
			info.getValue().collectWanSpeed(downSpeed);
		}
		
		for (Map.Entry<String, String> info : downSpeed
				.entrySet()) {
			fedCloudInfos.get(info.getKey()).setDownLinkSpeed(info.getValue());
			System.out.println("DOWNSPEED:"+info.getKey()+"="+info.getValue());
		}
		return downSpeed;

	}
	private void normalizeInfo(Map<String, FedCloudInfo>  fedCloudInfos){
		long minIntersize = Long.MAX_VALUE;
		double minWan = Double.MAX_VALUE;
		double minReduceSpeed = Double.MAX_VALUE;
		double minMapSpeed = Double.MAX_VALUE;
		int minCore = Integer.MAX_VALUE;
		int minMB = Integer.MAX_VALUE;
		for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos
				.entrySet()) {
			FedCloudInfo fedInfo = info.getValue();
			if( fedInfo.getAvailableMB() < minMB){
				minMB = fedInfo.getAvailableMB();
			}
			if( fedInfo.getAvailableVcores() < minCore){
				minCore = fedInfo.getAvailableVcores();
			}
			if( fedInfo.getReduceSpeed() < minReduceSpeed){
				minReduceSpeed = fedInfo.getReduceSpeed();
			}
			if( fedInfo.getMapSpeed() < minMapSpeed){
				minMapSpeed = fedInfo.getMapSpeed();
			}
			if( fedInfo.getInterSize() < minIntersize){
				minIntersize = fedInfo.getInterSize();
			}
			//if( fedInfo.getInputSize() < minInputsize){
			//	minInputsize = fedInfo.getInputSize();
			//}
			System.out.println("DownLinkSpeed " + fedInfo.getDownLinkSpeed());
			String downLinkSpeedInfo[] = fedInfo.getDownLinkSpeed().split("/");
			for(String dLinkSpeedInfo : downLinkSpeedInfo){
				System.out.println("DLinkSpeed " + dLinkSpeedInfo);
				double speed = Double.parseDouble(dLinkSpeedInfo.split("=")[1]);
				//for Text Text only
				speed = (60d * speed)/(60d + speed);
				if(speed < minWan)
					minWan = speed;
			}
			//if( fedInfo.getMinWanSpeed() < minWan){
			//	minWan = fedInfo.getMinWanSpeed();
			//}
		}
		for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos
				.entrySet()) {
			FedCloudInfo fedInfo = info.getValue();
			fedInfo.setAvailableMB_normalized((double)fedInfo.getAvailableMB() /(double) minMB);
			fedInfo.setAvailableVcores_normalized((double)fedInfo.getAvailableVcores() /(double) minCore);
			fedInfo.setReduceSpeed_normalized((double)fedInfo.getReduceSpeed() /(double) minReduceSpeed);
			fedInfo.setMapSpeed_normalized((double)fedInfo.getMapSpeed() /(double) minMapSpeed);
			fedInfo.setInterSize_normalized((double)fedInfo.getInterSize() /(double) minIntersize);
			//fedInfo.setMinWanSpeed_normalized( fedInfo.getMinWanSpeed() /  minWan);
			String downLinkSpeedInfo[] = fedInfo.getDownLinkSpeed().split("/");
			String n_downLinkSpeedInfo = "";
			for(String dLinkSpeedInfo : downLinkSpeedInfo){
				double speed = Double.parseDouble(dLinkSpeedInfo.split("=")[1]);
				double n_speed = speed/minWan;
				n_downLinkSpeedInfo += dLinkSpeedInfo.split("=")[0]+"="+ Double.toString(n_speed)+"/";
			}
			System.out.println("n_DownLinkSpeed " + n_downLinkSpeedInfo);
			fedInfo.setDownLinkSpeed_normalized(n_downLinkSpeedInfo);
		}
	}
	

	private Class<?> mKeyClz;
	private Class<?> mValueClz;
	private Class<? extends Reducer> mReducer;
	private Class<? extends Mapper> mMapper;

	public void setUserDefine(Class<?> keyClz, Class<?> valueClz,
			Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {
		mKeyClz = keyClz;
		mValueClz = valueClz;
		mReducer = reducer;
		mMapper = mapper;
	}

	private Class<? extends RawComparator> gcls;
	private Class<? extends RawComparator> scls;
	private Class<? extends Partitioner> pcls;

	public void setSortGroupPartitionClass(Class<? extends RawComparator> Scls,
			Class<? extends RawComparator> Gcls,
			Class<? extends Partitioner> Pcls) {
		scls = Scls;
		gcls = Gcls;
		pcls = Pcls;
	}
	FedWANServer wanServer = new FedWANServer();
	public void startFedJob() {
		try {
			
		//	FedWANClient wanClient = new FedWANClient();
		//	wanClient.setHost(mJob.getConfiguration().get("fs.default.name").split("/")[2]);
	    //	wanClient.start();
	    //	wanClient.join();
			boolean mapOnly = false;
			if (mJob.getNumReduceTasks() == 0) {
				mapOnly = true;
			}
					
			mFedJobConf = new FedJobConf(mJobConf, mJob);
			System.out.println("TopCloudURLs: " + TopCloudHasher.topURLs);

			/*
			 * if ( mFedJobConf.isFedTest() ) {
			 * FedRegionCloudJobDistcp.test(mJobConf); }
			 */
			// Fed-MR , Top Cloud Mode
			if (mFedJobConf.isTopCloud()) {
				System.out.println("Run AS Top Cloud");
				if(mJobConf.get("seqInter", "false").equals("true"))
					mJob.setInputFormatClass(SequenceFileInputFormat.class);
				else
					mJob.setInputFormatClass(TextInputFormat.class);
				if (mMapper != null) {
					System.out.println("PROMAP:" + mMapper.getName());
					mFedJobConf.selectProxyMap(mKeyClz, mValueClz, mMapper);

				} else {
					mFedJobConf.selectProxyMap();
				}
				// set input path
				String inputPathsString = mFedJobConf.getTopCloudInputPath();
				System.out.println("TOP CLOUD IN =" + inputPathsString);
				String[] pathString = inputPathsString.split(",");
				Path[] inputPaths = new Path[pathString.length];
				for (int i = 0; i < pathString.length; i++) {
					inputPaths[i] = new Path(pathString[i]);
				}

				for (Path inputPath : inputPaths) {
					System.out.println("INPUT PATH:" + inputPath.toString());
				}
				// mJob.setPartitionerClass(pcls) ;
				// mJob.setSortComparatorClass(scls) ;
				// mJob.setGroupingComparatorClass(gcls) ;
				FileInputFormat.setInputPaths(mJob, inputPaths);
				FileInputFormat.setInputPathFilter(mJob, InterPathFilter.class);

				Path outputPath = new Path(mFedJobConf.getTopCloudOutputPath());
				System.out.println("TOP CLOUD OUT ="
						+ mFedJobConf.getTopCloudOutputPath());
				FileOutputFormat.setOutputPath(mJob, outputPath);
				FedJobServerClient jclient = null;
				String ip = mJobConf.get("fedCloudHDFS").split(":")[1].split("/")[2];
				InetAddress address;
				try {
					address = InetAddress.getByName(ip);
					jclient = new FedJobServerClient(address.getHostAddress(), 8713);
					jclient.start();
				} catch (UnknownHostException e1) {
					e1.printStackTrace();
				}
				String namenode = mJobConf.get("fs.default.name");
				jclient.sendTopStartTime(namenode.split("/")[2]);
				mFedStat.setTopCloudStart();
				System.out.println("----------------------------------");
				System.out.println("TopCloud Start Time = "
						+ mFedStat.getTopCloudStart());
				System.out.println("----------------------------------");
				if(mJobConf.get("topNumbers").equals("1")){
					mJob.setOutputFormatClass(ReallocateOutputFormat.class);
					mJob.setNumReduceTasks(1);
				}

			} else if (mFedJobConf.isRegionCloud()) {
				int tmp_port = mJobConf.get("regionCloudOutput").split("_")[0].hashCode()%10000;
				tmp_port = Math.abs(tmp_port);
                                System.out.println("tmp_port = " + tmp_port);
				//wanServer.setPort(mJobConf.get("regionCloudOutput").split("_")[0].hashCode()%10000);
				wanServer.setPort(tmp_port);
				wanServer.setJobConf(mJobConf);
				wanServer.start();
				String[] HDFSs = mJobConf.get("topCloudHDFSs").split(",");
				List<FedWANClient> wlist = new ArrayList<FedWANClient>();
				for(String HDFS: HDFSs){
					String ip = HDFS.split("//")[1].split(":")[0];
					FedWANClient wanClient = new FedWANClient();
					wanClient.setIp(ip);
					wanClient.setHost(mJob.getConfiguration().get("fs.default.name").split("/")[2]);
					wlist.add(wanClient);
				}
				for(FedWANClient c :wlist){
					c.start();
					c.join();
				}
		
				FedJobServerClient jclient = null;
				String ip = mJobConf.get("fedCloudHDFS").split(":")[1].split("/")[2];
				InetAddress address;
				try {
					address = InetAddress.getByName(ip);
					jclient = new FedJobServerClient(address.getHostAddress(), 8713);
					jclient.start();
				} catch (UnknownHostException e1) {
					e1.printStackTrace();
				}
				String namenode = mJobConf.get("fs.default.name");
				jclient.sendRegionMapStartTime(namenode.split("/")[2]);

				System.out.println(mJobConf.get("wanSpeed", "noSpeed"));
				if (mJobConf.get("wanOpt").equals("true")) {
					

					String webapp = mJobConf
							.get("yarn.resourcemanager.webapp.address");
					URLConnection connection = new URL("http://" + webapp
							+ "/ws/v1/cluster/metrics").openConnection();
					InputStream response = connection.getInputStream();
					String res = IOUtils.toString(response, "UTF-8");

					res = res.replaceAll("}", "");
					System.out.println("RAPI:" + res);
					String[] metrics = res.split(",");
					String availableMB = "";
					String availableVcores = "";
					String activeNodes = "";
					for (int i = 0; i < metrics.length; i++) {
						if (metrics[i].contains("activeNodes")) {
							activeNodes = metrics[i].substring(14);
						} else if (metrics[i].contains("availableVirtualCores")) {
							availableVcores = metrics[i].substring(24);
						} else if (metrics[i].contains("availableMB")) {
							availableMB = metrics[i].substring(14);
						}
					}
					
					jclient.sendRegionResource(namenode.split("/")[2] + ","
							+ activeNodes + "," + availableMB + ","
							+ availableVcores);
					// jclient.stopClientProbe();
					// mWanOpt = true;
					String resWAN = "";
					if(mJobConf.get("preciseIter", "false").equals("true"))
						resWAN = jclient.sendReqInfo(namenode.split("/")[2]);
					else
						resWAN = jclient.sendReqInfo_normalized(namenode.split("/")[2]);
					
					if (resWAN.contains(FedCloudProtocol.RES_INFO)) {
						String info = resWAN.substring(14);
						System.out.println("INFO in FEDJob:"+ info);
						mJobConf.set("fedInfos", info);
					}
				}

				// do region cloud things
				System.out.println("Run AS Region Cloud");
				System.out.println("----------------------------------");
				System.out.println("|        RegionCloud Mode        |");
				System.out.println("----------------------------------");
				/*
				 * FedJobServerClient client = new
				 * FedJobServerClient("10.3.1.2",8769); client.start();
				 * Thread.sleep(10000); client.sendRegionMapFinished();
				 * client.stopClientProbe();
				 */
			
			//	mServer = new FedCloudMonitorServer(
			//			mFedJobConf.getRegionCloudServerListenPort());
			//	mServer.start();
				mFedStat.setRegionCloudsStart();

				String multiMapper = mJobConf.get("multiMapper", "false");
				String multiFormat = mJobConf.get("multiFormat", "false");
				if (!multiMapper.equals("false")) {
					System.out.println(multiMapper);
					String[] mapper = multiMapper.split(",");
					String[] format = multiFormat.split(",");

					for (int i = 0; i < mapper.length; i++) {
						System.out.println(mapper[i].split("=")[0]);
						System.out.println(mapper[i].split("=")[1]);
						File root = new File(mJobConf.get("classpath"));
						URLClassLoader classLoader = URLClassLoader
								.newInstance(new URL[] { root.toURI().toURL() });

						Class<? extends InputFormat> fclazz = Class.forName(
								format[i].split("=")[1]).asSubclass(
								InputFormat.class);
						Class<? extends Mapper> mclazz = Class.forName(
								mapper[i].split("=")[1].replace('+', '$'),
								true, classLoader).asSubclass(Mapper.class);
						MultipleInputs.addInputPath(mJob,
								new Path(mapper[i].split("=")[0]), fclazz,
								mclazz);
						System.out.println("add Mapper:"
								+ mapper[i].split("=")[0] + "||"
								+ fclazz.getName() + "||" + mclazz.getName());
					}
				} else {
					Path[] inputPath = new Path[1];
					inputPath[0] = new Path(
							mFedJobConf.getRegionCloudInputPath());
					FileInputFormat.setInputPaths(mJob, inputPath);
				}
				if (!mapOnly) {
					LazyOutputFormat.setOutputFormatClass(mJob,
							TextOutputFormat.class);
					Path outputPath = new Path(
							mFedJobConf.getRegionCloudOutputPath());
					FileOutputFormat.setOutputPath(mJob, outputPath);
				}
				if (!mapOnly) {
					if (mReducer != null) {
						System.out.println("PRORED:" + mReducer.getName());
						mFedJobConf.selectProxyReduce(mKeyClz, mValueClz,
								mReducer);
					} else {
						mFedJobConf.selectProxyReduce();
					}
				}
				//mJobConf.set(JobContext.KEY_COMPARATOR, "");
				//directly sent to top cloud
				String proxyReduce = mJobConf.get("proxyReduce", "off");
				if (proxyReduce.toLowerCase().equals("on")
						|| proxyReduce.toLowerCase().equals("true")) {
					bIsProxyReduce = true;
				}
				if(!bIsProxyReduce){
					mJob.setOutputFormatClass(RemoteOutputFormat.class);
					mJob.setNumReduceTasks(0);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	public void stopFedJob() throws UnknownHostException {
		// TODO print statistic values
		if (mFedJobConf.isFedMR()) {
			// print global aggregation time from its client, to get correct
			// answers.
			List<FedCloudMonitorClient> cList = mFedJobConf
					.getFedCloudMonitorClientList();
			long total_aggregation_time = 0;
			for (FedCloudMonitorClient job : cList) {
				job.printAggregationTime();
				total_aggregation_time += job.getAggregationTime();
			}
			System.out
					.println("Hdfs Cloud Report : Total Global Aggregation Time = "
							+ total_aggregation_time + "(ms)");

			long early_aggregation_start_time = 0;
			long latest_aggregation_end_time = 0;
			for (FedCloudMonitorClient job : cList) {
				if (early_aggregation_start_time == 0) {
					early_aggregation_start_time = job.getAggregationStart();
				} else if (early_aggregation_start_time > job
						.getAggregationStart()) {
					early_aggregation_start_time = job.getAggregationStart();

				}
			}
			for (FedCloudMonitorClient job : cList) {
				if (latest_aggregation_end_time < job.getAggregationEnd()) {
					latest_aggregation_end_time = job.getAggregationEnd();

				}
			}
			long actualAggregationTimeDuration = latest_aggregation_end_time
					- early_aggregation_start_time;
			System.out.println("actual Aggregation Time = "
					+ actualAggregationTimeDuration + "(ms)");
			double total_time = mFedStat.getRegionCloudsTime()
					+ mFedStat.getTopCloudTime()
					+ actualAggregationTimeDuration;
			double total_time_in_s = total_time / 1000.0;
			System.out.println("total time = " + total_time + " (ms), "
					+ total_time_in_s + "(s)");

		}
		if (mFedJobConf.isTopCloud()) {
			FedJobServerClient jclient = null;
			String ip = mJobConf.get("fedCloudHDFS").split(":")[1].split("/")[2];
			InetAddress address;
			try {
				address = InetAddress.getByName(ip);
				jclient = new FedJobServerClient(address.getHostAddress(), 8713);
				jclient.start();
			} catch (UnknownHostException e1) {
				e1.printStackTrace();
			}
			String namenode = mJobConf.get("fs.default.name");
			jclient.sendTopStopTime(namenode.split("/")[2]);
			//jclient.stopClientProbe();
			mFedStat.setTopCloudEnd();
			System.out.println("----------------------------------");
			System.out.println("TopCloudEnd() = " + mFedStat.getTopCloudEnd());
			System.out.println("----------------------------------");
			System.out.println("Top Cloud Report : RegionCloud Work Time = "
					+ mFedStat.getRegionCloudsTime() + "(ms)");
			System.out.println("Top Cloud Report : Top Cloud Work Time = "
					+ mFedStat.getTopCloudTime() + "(ms)");
			System.out
					.println("Top Cloud Report : Global Aggregation Time Details : ");

		}
		if (mFedJobConf.isRegionCloud()) {
			System.out.println("stopFED");
			FedJobServerClient jclient = null;
			String ip = mJobConf.get("fedCloudHDFS").split(":")[1].split("/")[2];
			InetAddress address;
			try {
				address = InetAddress.getByName(ip);
				jclient = new FedJobServerClient(address.getHostAddress(), 8713);
				jclient.start();
			} catch (UnknownHostException e1) {
				e1.printStackTrace();
			}
			String namenode = mJobConf.get("fs.default.name");
			jclient.sendRegionInterTransferStopTime(namenode.split("/")[2]);
			//wanServer.stopServer();
		/*	try {
				wanServer.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			*/
			/*
			 * String[] preIP = mFedJobConf.getTopCloudHDFSURL().split("/");
			 * String[] IP = preIP[2].split(":");
			 * System.out.println("1:::::"+IP[0]);
			 * System.out.println("2:::::"+Inet4Address
			 * .getLocalHost().getHostAddress());
			 */
			// TODO if same machine but different hdfs

			// execute Distcp from Region Cloud
			mFedStat.setRegionCloudsEnd();
			// mFedStat.setGlobalAggregationStart();
		//	mServer.sendMapPRFinished();
			// mServer.sendMigrateData("");
			/*
			 * List<FedRegionCloudJobDistcp> distCpList = new
			 * ArrayList<FedRegionCloudJobDistcp>(); try { List<String>
			 * TopCloudHDFSURLs = mFedJobConf.getTopCloudHDFSURLs(); for(String
			 * topHDFSURL: TopCloudHDFSURLs){
			 * if(!mJobConf.get("FS_DEFAULT_NAME_KEY", "").equals(topHDFSURL)){
			 * distCpList.add(new FedRegionCloudJobDistcp( mFedJobConf,
			 * mJobConf, topHDFSURL)); } } for(FedRegionCloudJobDistcp dJob :
			 * distCpList){ if(bIsFedTachyon){ dJob.setTachyonFlag(); }
			 * if(!dJob.isLocal()){ dJob.start(); } }
			 * for(FedRegionCloudJobDistcp dJob : distCpList){
			 * if(!dJob.isLocal()){ dJob.join(); } } // distcp copy from region
			 * cloud hdfs to top cloud hdfs
			 * System.out.println("Server To Join"); } catch ( Exception e ) {
			 * e.printStackTrace(); }
			 */
			// mServer.sendMigrateDataFinished("");
			System.out.println("Stop Server");
		//	mServer.stopServer();
			// mFedStat.setGlobalAggregationEnd();
			System.out.println("Region Cloud Report : RegionCloudsTime = "
					+ mFedStat.getRegionCloudsTime() + "(ms)");
			// System.out.println("Region Cloud Report : GlobalAggregationTime = "
			// + mFedStat.getGlobalAggregationTime() + "(ms)");

		}
	}
	 private static final PathFilter FileFilter = new PathFilter(){
	      public boolean accept(Path p){
	        String name = p.getName(); 
	        return !name.startsWith("_") && !name.startsWith(".") && !name.startsWith("p"); 
	     }
	  }; 
}
