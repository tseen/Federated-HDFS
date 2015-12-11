package ncku.hpds.fed.MRv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.io.File;
import java.net.Inet4Address;
import java.net.UnknownHostException;

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
			System.out.println("Iterations:"+Integer.parseInt(fedIter));
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

	public void scheduleAndStartFedJob() {
		int currentIter = 1;
		try {
			FedWANServer wanServer = new FedWANServer();
			wanServer.start();
			FedJobServer jobServer = new FedJobServer(8769);
			jobServer.start();
			Path[] mInputPaths = FileInputFormat.getInputPaths(mJob);
			if (mInputPaths.length > 0) {
				mFileName = mInputPaths[0].getName();
			}
			
			System.out.println("Start FedJobConfHdfs");
			mFedJobConf = new FedJobConfHdfs(mJobConf, mJob, mFileName);
			System.out.println("End FedJobConfHdfs");
			List<JarCopyJob> jcjList = mFedJobConf.getJarCopyJobList();
			HashMap<String, FedCloudInfo> fedCloudInfos = (HashMap<String, FedCloudInfo>) mFedJobConf.getFedCloudInfos();
			
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
				if (bIsFedIteration && currentIter > 1
						&& currentIter <= iterations) {
					Path mOutpuPath = FileOutputFormat.getOutputPath(mJob);
					String input[] = mOutpuPath.toString().split("/");
					//mFedJobConf.configIter("/"+input[3]+"/"+input[4]+"/"+input[5],currentIter);
					/* 
					 * User-defined the iterative input file, if not defined, using the output file of
					 * the last iteration.
					 */
					String iterInput = mJobConf.get("iterInput", "/"+input[3]+"/"+input[4]+"/"+input[5]);
					mFedJobConf.configIter(iterInput,currentIter);

				}
				// get top job
				List<FedTopCloudJob> tList = mFedJobConf.getTopCloudJobList();
				// get region job
				List<FedRegionCloudJob> rList = mFedJobConf
						.getRegionCloudJobList();
				// get region monitor
				List<FedCloudMonitorClient> cList = mFedJobConf
						.getFedCloudMonitorClientList();

				mFedStat.setRegionCloudsStart();
				TopCloudHasher.topCounts = rList.size();
				System.out.println("TopCloud Report : RegionCloud Start Time = "
								+ mFedStat.getRegionCloudsStart() + "(ms)");
				System.out.println("Start FedRegionCloudJobs");
				
				
				for (Map.Entry<String, FedCloudInfo> info : fedCloudInfos.entrySet())
				{
					info.getValue().setRegionMapStartTime((int) System.currentTimeMillis());
				}
				
				//int regionMapStartTime = (int) System.currentTimeMillis();
				for (FedRegionCloudJob job : rList) {
					if (bIsFedTachyon) {
						job.setTachyonFlag();
					}
					job.start();
				}
				
				for ( FedCloudMonitorClient job : cList ) { job.start(); } 

				System.out.println("Wait For FedRegionCloudJob Join");
				for (FedRegionCloudJob job : rList) {
					job.join();
				}
				System.out.println("FedRegionCloudJob All Joined");
				mFedStat.setRegionCloudsEnd();
				System.out.println("TopCloud Report : RegionCloud End Time = "
						+ mFedStat.getRegionCloudsEnd() + "(ms)");
				System.out
						.println("TopCloud Report : RegionCloud Total Time = "
								+ mFedStat.getRegionCloudsTime() + "(ms)");
				System.out.println("----------------------------------");
				System.out.println("Map-ProxyReduce Phrase Finished");
				System.out.println("----------------------------------\n");
				/*
				 * startFedJob() --> stopFedJob() job finished Region Cloud
				 * Notify Top Cloud Top Cloud notify Region Cloud start to
				 * upload result to Top Cloud Record Aggregation Time When all
				 * data is collected then do something Proxy-Map Reducer Phrase
				 */

				System.out.println("----------------------------------");
				System.out.println("Global Aggregation Start ...");
				System.out.println("----------------------------------");

				// distcp copy from region cloud hdfs to top cloud hdfs
				System.out.println("Wait For FedCouldMonitorClient Join");
				for (FedCloudMonitorClient job : cList) {
					job.join();
				}
				System.out.println("FedCouldMonitorClient All Joined");

				System.out
						.println("Top Cloud Report : Global Aggregation Consuming Time :");
				for (FedCloudMonitorClient job : cList) {
					job.printAggregationTime();
				}
				System.out.println("----------------------------------");
				System.out.println("Global Aggregation End ...");
				System.out.println("----------------------------------");

				System.out.println("----------------------------------");
				System.out.println("TOP START ...");
				System.out.println("----------------------------------");
				mFedStat.setTopCloudStart();
				// topJob.start();
				// topJob.join();
				for (FedTopCloudJob job : tList) {
					if (bIsFedIteration) {
						job.setIterFlag(currentIter);
					}
					job.start();
				}
				for (FedTopCloudJob job : tList) {
					job.join();
				}
				mFedStat.setTopCloudEnd();
				System.out.println("----------------------------------");
				System.out.println("TOP END ...");
				System.out.println("----------------------------------");
				
				HdfsFileSender sender = new HdfsFileSender();
				List<String> topCloudHDFSs = mFedJobConf.getTopCloudHDFSURLs();
				for(String t:topCloudHDFSs){
					System.out.println("*****"+t+"*****");
				}
		        sender.send((ArrayList) topCloudHDFSs, "/user/hpds/zzz");
		       // String topCloudHDFS = "hdfs://"+topCloudHDFSs.get(0);
		        
				
				currentIter ++;

			}
			jobServer.stopServer();
			wanServer.stopServer();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

	public void startFedJob() {
		try {

			mFedJobConf = new FedJobConf(mJobConf, mJob);
			System.out.println("TopCloudURLs: " + TopCloudHasher.topURLs);

			/*
			 * if ( mFedJobConf.isFedTest() ) {
			 * FedRegionCloudJobDistcp.test(mJobConf); }
			 */
			// Fed-MR , Top Cloud Mode
			if (mFedJobConf.isTopCloud()) {
				System.out.println("Run AS Top Cloud");
				mFedJobConf.selectProxyMap();

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

				FileInputFormat.setInputPaths(mJob, inputPaths);

				Path outputPath = new Path(mFedJobConf.getTopCloudOutputPath());
				System.out.println("TOP CLOUD OUT ="
						+ mFedJobConf.getTopCloudOutputPath());
				FileOutputFormat.setOutputPath(mJob, outputPath);

				mFedStat.setTopCloudStart();
				System.out.println("----------------------------------");
				System.out.println("TopCloud Start Time = "
						+ mFedStat.getTopCloudStart());
				System.out.println("----------------------------------");

			} else if (mFedJobConf.isRegionCloud()) {
				// do region cloud things
				System.out.println("Run AS Region Cloud");
				System.out.println("----------------------------------");
				System.out.println("|        RegionCloud Mode        |");
				System.out.println("----------------------------------");
			/*	FedJobServerClient client = new FedJobServerClient("10.3.1.2",8769);
				client.start();
				Thread.sleep(10000);
				client.sendRegionMapFinished();
				client.stopClientProbe();*/
				mFedJobConf.selectProxyReduce();
				mServer = new FedCloudMonitorServer(
						mFedJobConf.getRegionCloudServerListenPort());
				mServer.start();
				mFedStat.setRegionCloudsStart();

				Path[] inputPath = new Path[1];
				inputPath[0] = new Path(mFedJobConf.getRegionCloudInputPath());
				FileInputFormat.setInputPaths(mJob, inputPath);

				LazyOutputFormat.setOutputFormatClass(mJob,
						TextOutputFormat.class);
				Path outputPath = new Path(
						mFedJobConf.getRegionCloudOutputPath());
				FileOutputFormat.setOutputPath(mJob, outputPath);
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
			//mFedStat.setGlobalAggregationStart();
			mServer.sendMapPRFinished();
			//mServer.sendMigrateData("");
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
			//mServer.sendMigrateDataFinished("");
			System.out.println("Stop Server");
			mServer.stopServer();
			//mFedStat.setGlobalAggregationEnd();
			System.out.println("Region Cloud Report : RegionCloudsTime = "
					+ mFedStat.getRegionCloudsTime() + "(ms)");
			//System.out.println("Region Cloud Report : GlobalAggregationTime = "
			//		+ mFedStat.getGlobalAggregationTime() + "(ms)");

		}
	}
}
