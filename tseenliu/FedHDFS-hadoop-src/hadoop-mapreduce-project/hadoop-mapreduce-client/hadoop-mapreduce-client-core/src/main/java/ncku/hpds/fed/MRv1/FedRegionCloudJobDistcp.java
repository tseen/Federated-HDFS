package ncku.hpds.fed.MRv1 ;

import org.apache.hadoop.mapred.JobConf;
import java.net.*;
import java.io.*;

public class FedRegionCloudJobDistcp extends Thread {
    private FedJobConf mConf;
    private ShellMonitor mOutputMonitor;
    private ShellMonitor mErrorMonitor;
    private static final String FS_DEFAULT_NAME_KEY = "fs.default.name";

    private boolean mRunFlag = false;
    private JobConf mJobConf = null;
    //--------------------------------------------
    public static void test(JobConf jobConf) {
        String url = jobConf.get(FS_DEFAULT_NAME_KEY,"file:///");
        System.out.println("HDFS URL is " + url );
    }
    //--------------------------------------------
    public FedRegionCloudJobDistcp(FedJobConf conf, JobConf jobConf )  {
        mConf = conf;
        mJobConf = jobConf;
    }
    public void run() {
        try { 
            String cmd = makeRegionCloudCmd();
            if ( mRunFlag ) {
                Runtime rt = Runtime.getRuntime();
                //copy configuration into Region Cloud first
                Process proc = rt.exec(cmd);
                mOutputMonitor = new ShellMonitor( proc.getInputStream(), "Distcp_" );
                mErrorMonitor = new ShellMonitor( proc.getErrorStream(), "Distcp_" );
                mOutputMonitor.start();
                mErrorMonitor.start();
                mOutputMonitor.join();
                mErrorMonitor.join();
                proc.waitFor();
            } 
        } catch ( Exception e ) {
        }
    }
    //--------------------------------------------
    private String makeRegionCloudCmd() {
        if ( mConf == null ) {
            System.err.println("Null FedHadoopConf");
            return "";
        }
        if ( mConf.getTopCloudHDFSURL().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.err.println("Invalid Top Cloud HDFS URL ");
            return "";
        } 
        String userName = System.getProperty("user.name");  
        String hadoop_home = mConf.getRegionCloudHadoopHome();
        String outputPath = mConf.getRegionCloudOutputPath();
        // get source HDFS URL, specified in coworking.xml
        String srcHDFS = mJobConf.get(FS_DEFAULT_NAME_KEY,""); 
        System.out.println(" userName = " + userName );
        System.out.println(" hadoop_home = " + hadoop_home );
        System.out.println(" outputPath = " + outputPath );
        System.out.println(" srcHDFS = " + srcHDFS );
        if ( srcHDFS.length() <= 0 ) {
            System.err.println("Invalid HDFS");
            return "";
        }
        String dstHDFS = mConf.getTopCloudHDFSURL();
        if ( srcHDFS.equals(dstHDFS) ) {
            mRunFlag = false;
            return "";
        }
        mRunFlag = true;
        //ssh hpds@140.116.164.101 ls
        //list HOME Directory of hpds
        // nn1 to nn2
        // bash$ hadoop distcp hdfs://nn1:8020/foo/bar hdfs://nn2:8020/bar/foo
        String cmd = hadoop_home + "/bin/hadoop distcp "; 
        cmd = cmd + srcHDFS + "user/"+ userName + "/" + outputPath + " "  + dstHDFS +"user/" + userName + "/" + outputPath ;
        System.out.println("RegionCloud Distcp [" + cmd + "]" ); 
        return cmd;
    }
    //--------------------------------------------
}


