package ncku.hpds.fed.MRv1;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
public class FedJob{
    /*
     * FedJob 
     *   FedJobConf
     *     FedJobConfParser
     * */
    private JobConf mJobConf; 
    private FedJobConf mFedJobConf;
    private FedJobStatistics mFedStat = new FedJobStatistics(); 
    private FedCloudMonitorServer mServer = null;
    private boolean bIsFed = false;
    public FedJob( JobConf jobConf ) {
        mJobConf = jobConf;
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
    public void startFedJob() {
        try {
            mFedJobConf= new FedJobConf(mJobConf);
            if ( mFedJobConf.isFedTest() ) {
                FedRegionCloudJobDistcp.test(mJobConf);
            }
            //Fed-MR , Top Cloud Mode
            if ( mFedJobConf.isFedMR() ) {
                mFedStat.setRegionCloudsStart();
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

                System.out.println("Start FedRegionCloudJobs");
                for ( FedRegionCloudJob job : rList ) { job.start(); } 
                System.out.println("Start FedCloudMonitorClient");
                for ( FedCloudMonitorClient job : cList ) { job.start(); } 
                
                System.out.println("Wait For FedRegionCloudJob Join");
                for ( FedRegionCloudJob job : rList ) { job.join(); }
                System.out.println("FedRegionCloudJob All Joined");
                /*
                 *  startFedJob() --> stopFedJob()
                 *  job finished 
                 *  Region Cloud Notify Top Cloud
                 *  Top Cloud notify Region Cloud start to upload result to Top Cloud
                 *  Record Aggregation Time 
                 *  When all data is collected then do something Proxy-Map Reducer Phrase
                 *
                 * */
                mFedStat.setRegionCloudsEnd();
                System.out.println("----------------------------------");
                System.out.println("Map-ProxyReduce Phrase Finished");
                System.out.println("----------------------------------\n");
                System.out.println("----------------------------------");
                System.out.println("Global Aggregation Processing ...");
                System.out.println("----------------------------------");
                mFedStat.setGlobalAggregationStart();
                // distcp copy from region cloud hdfs to top cloud hdfs
                System.out.println("Wait For FedCouldMonitorClient Join");
                for ( FedCloudMonitorClient job : cList ) { job.join(); } 
                System.out.println("FedCouldMonitorClient All Joined");
                mFedStat.setGlobalAggregationEnd();
                // for test
                //System.exit(1);
                // set inputpath
                Path[] inputPaths = mFedJobConf.getRegionCloudOutputPaths();
                FileInputFormat.setInputPaths( mJobConf, inputPaths );
                mFedJobConf.selectProxyMap();
            } else if ( mFedJobConf.isRegionCloud() ) {
                //TODO do region cloud things
                System.out.println("RegionCloud Mode");
                mFedJobConf.selectProxyReduce();
                mServer = new FedCloudMonitorServer( mFedJobConf.getRegionCloudServerListenPort() ); 
                mServer.start();
                mFedStat.setRegionCloudsStart();
                Path outputPath = new Path( mFedJobConf.getRegionCloudOutputPath() );
                FileOutputFormat.setOutputPath( mJobConf, outputPath );
            }
        } catch ( Exception e ) {
        } 
    }
    public void stopFedJob() {
        //TODO print statistic values
        if ( mFedJobConf.isFedMR() ) { 
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
                mServer.stopServer();
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            mServer.sendMigrateDataFinished("");
            mFedStat.setGlobalAggregationEnd();
        }
    }
}
