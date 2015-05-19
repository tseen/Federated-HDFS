package ncku.hpds.fed.MRv2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.List;
import java.util.ArrayList;
import java.io.File;
public class FedJob{
    /*
     * FedJob 
     *   FedJobConf
     *     FedJobConfParser
     * */
    private Configuration mJobConf; 
    private Job mJob;
    private FedJobConf mFedJobConf;
    private FedJobStatistics mFedStat = new FedJobStatistics(); 
    private FedCloudMonitorServer mServer = null;
    private boolean bIsFed = false;
    public FedJob( Job job ) {
        mJob = job;
        mJobConf = mJob.getConfiguration();
        String fed = mJobConf.get("fed","off");
        if ( fed.toLowerCase().equals("on") || 
             fed.toLowerCase().equals("true") ) 
        {
            bIsFed = true;
        } 
        String regionCloud = mJobConf.get("regionCloud","off") ;
        if ( regionCloud.toLowerCase().equals("on") ||
             regionCloud.toLowerCase().equals("true") ) 
        {
            bIsFed = true;
        }
    }
    public boolean isFedJob() {
        return bIsFed;
    }
    public void startFedJob() throws Throwable {
        try {
            mFedJobConf= new FedJobConf(mJobConf, mJob);
            if ( mFedJobConf.isFedTest() ) {
                FedRegionCloudJobDistcp.test(mJobConf);
            }
            //Fed-MR , Top Cloud Mode
            if ( mFedJobConf.isFedMR() ) {
                System.out.println("Run AS Top Cloud");
                List<FedRegionCloudJob> rList = mFedJobConf.getRegionCloudJobList();
                List<FedCloudMonitorClient> cList = mFedJobConf.getFedCloudMonitorClientList();
                List<JarCopyJob> jcjList = mFedJobConf.getJarCopyJobList();

                if ( jcjList.size() > 0 ) {
                    System.out.println("Start Jar copy jobs");
                    for ( JarCopyJob job : jcjList) { job.start(); } 
                    System.out.println("Wait For Jar copy finishing");
                    for ( JarCopyJob job : jcjList) { job.join(); } 
                    System.out.println("Jar copy Jobs finished");
                }

                mFedStat.setRegionCloudsStart();
                System.out.println("TopCloud Report : RegionCloud Start Time = " + mFedStat.getRegionCloudsStart() + "(ms)");	
                System.out.println("Start FedRegionCloudJobs");
                for ( FedRegionCloudJob job : rList ) { job.start(); } 
                System.out.println("Start FedCloudMonitorClient");
                for ( FedCloudMonitorClient job : cList ) { job.start(); } 

                System.out.println("Wait For FedRegionCloudJob Join");
                for ( FedRegionCloudJob job : rList ) { job.join(); }
                System.out.println("FedRegionCloudJob All Joined");
                mFedStat.setRegionCloudsEnd();
                System.out.println("TopCloud Report : RegionCloud End Time = " + mFedStat.getRegionCloudsEnd()+ "(ms)");	
                System.out.println("TopCloud Report : RegionCloud Total Time = " + mFedStat.getRegionCloudsTime()+ "(ms)");	
                System.out.println("----------------------------------");
                System.out.println("Map-ProxyReduce Phrase Finished");
                System.out.println("----------------------------------\n");
                /*
                 *  startFedJob() --> stopFedJob()
                 *  job finished 
                 *  Region Cloud Notify Top Cloud
                 *  Top Cloud notify Region Cloud start to upload result to Top Cloud
                 *  Record Aggregation Time 
                 *  When all data is collected then do something Proxy-Map Reducer Phrase
                 *
                 * */

                System.out.println("----------------------------------");
                System.out.println("Global Aggregation Start ...");
                System.out.println("----------------------------------");

                // distcp copy from region cloud hdfs to top cloud hdfs
                System.out.println("Wait For FedCouldMonitorClient Join");
                for ( FedCloudMonitorClient job : cList ) { job.join(); } 
                System.out.println("FedCouldMonitorClient All Joined");

                System.out.println("Top Cloud Report : Global Aggregation Consuming Time :");
                for ( FedCloudMonitorClient job : cList ) { 
                    job.printAggregationTime(); 
                } 
                System.out.println("----------------------------------");
                System.out.println("Global Aggregation End ...");
                System.out.println("----------------------------------");

                // set inputpath
                Path[] inputPaths = mFedJobConf.getRegionCloudOutputPaths();
                FileInputFormat.setInputPaths( mJob, inputPaths );
                mFedJobConf.selectProxyMap();

                mFedStat.setTopCloudStart();
                System.out.println("----------------------------------");
                System.out.println("TopCloud Start Time = " + mFedStat.getTopCloudStart());	
                System.out.println("----------------------------------");

            } else if ( mFedJobConf.isRegionCloud() ) {
                //TODO do region cloud things
                System.out.println("Run AS Region Cloud");
                System.out.println("----------------------------------");
                System.out.println("|        RegionCloud Mode        |");
                System.out.println("----------------------------------");
                mFedJobConf.selectProxyReduce();
                mServer = new FedCloudMonitorServer( mFedJobConf.getRegionCloudServerListenPort() ); 
                mServer.start();
                mFedStat.setRegionCloudsStart();
                Path outputPath = new Path( mFedJobConf.getRegionCloudOutputPath() );
                FileOutputFormat.setOutputPath( mJob, outputPath );
            }
        } catch ( Exception e ) {
        } 
    }
    public void stopFedJob() {
        //TODO print statistic values
        if ( mFedJobConf.isFedMR() ) { 
            mFedStat.setTopCloudEnd();
            System.out.println("----------------------------------");
            System.out.println("TopCloudEnd() = " + mFedStat.getTopCloudEnd());	
            System.out.println("----------------------------------");
            System.out.println("Top Cloud Report : RegionCloud Work Time = " + mFedStat.getRegionCloudsTime() + "(ms)");	
            System.out.println("Top Cloud Report : Top Cloud Work Time = " + mFedStat.getTopCloudTime()+ "(ms)");	
            System.out.println("Top Cloud Report : Global Aggregation Time Details : " );	
            
            // print global aggregation time from its client, to get correct answers.
            List<FedCloudMonitorClient> cList = mFedJobConf.getFedCloudMonitorClientList();
            long total_aggregation_time = 0;
            for ( FedCloudMonitorClient job : cList ) { 
                job.printAggregationTime(); 
                total_aggregation_time += job.getAggregationTime();
            } 
            System.out.println("Top Cloud Report : Total Global Aggregation Time = " + total_aggregation_time + "(ms)");	

      	    long early_aggregation_start_time = 0;
	    long latest_aggregation_end_time = 0;
            for ( FedCloudMonitorClient job : cList ) { 
		if ( early_aggregation_start_time == 0 ) {
			early_aggregation_start_time = 
				job.getAggregationStart();
		} else if ( early_aggregation_start_time > 
			   job.getAggregationStart() ) {
			early_aggregation_start_time = 
				job.getAggregationStart();

		}
            } 
            for ( FedCloudMonitorClient job : cList ) { 
		if ( latest_aggregation_end_time < 
			   job.getAggregationEnd() ) {
			latest_aggregation_end_time = 
				job.getAggregationEnd();

		}
            } 
	    long actualAggregationTimeDuration = latest_aggregation_end_time -  early_aggregation_start_time;
            System.out.println("actual Aggregation Time = " + 
		actualAggregationTimeDuration + "(ms)");
	    double total_time = mFedStat.getRegionCloudsTime() + mFedStat.getTopCloudTime() + actualAggregationTimeDuration;
	    double total_time_in_s = total_time / 1000.0;
	    System.out.println("total time = " + total_time + " (ms), " + total_time_in_s + "(s)");
		
            
        }
        if ( mFedJobConf.isRegionCloud() ) {
            //execute Distcp from Region Cloud
            mFedStat.setRegionCloudsEnd();
            mFedStat.setGlobalAggregationStart();
            mServer.sendMapPRFinished();
            mServer.sendMigrateData("");
            try {
                FedRegionCloudJobDistcp distcp = new 
                    FedRegionCloudJobDistcp( mFedJobConf, mJobConf ); 
                distcp.start(); 
                distcp.join();
                // distcp copy from region cloud hdfs to top cloud hdfs
                System.out.println("Server To Join");
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            mServer.sendMigrateDataFinished("");
	    System.out.println("Stop Server");
            mServer.stopServer();
            mFedStat.setGlobalAggregationEnd();
            System.out.println("Region Cloud Report : RegionCloudsTime = " + mFedStat.getRegionCloudsTime() +"(ms)");	
            System.out.println("Region Cloud Report : GlobalAggregationTime = " + mFedStat.getGlobalAggregationTime() + "(ms)");
        }
    }
}

