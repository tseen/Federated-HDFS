package ncku.hpds.fed.MRv2 ;

import org.apache.hadoop.conf.Configuration;

import java.net.*;
import java.io.*;

public class FedRegionCloudJobDistcp extends Thread {
    private AbstractFedJobConf mConf;
    private ShellMonitor mOutputMonitor;
    private ShellMonitor mErrorMonitor;
    private static final String FS_DEFAULT_NAME_KEY = "fs.default.name";
    private boolean mTachyon = false;
    private String mDistUrl ="";


    private boolean mRunFlag = false;
    private Configuration mJobConf = null;
    //--------------------------------------------
    public static void test(Configuration jobConf) {
        String url = jobConf.get(FS_DEFAULT_NAME_KEY,"file:///");
        System.out.println("HDFS URL is " + url );
    }
    //--------------------------------------------
    public FedRegionCloudJobDistcp(AbstractFedJobConf mFedJobConf, Configuration jobConf, String distUrl )  {
        mConf =  mFedJobConf;
        mJobConf = jobConf;
        mDistUrl = distUrl + "/";
    }
    public boolean isLocal(){
    	try{
	    	String[] preIP = mDistUrl.split("/");
	    	String[] IP = preIP[2].split(":");
	    	System.out.println("1:::::"+IP[0]);
	    	System.out.println("2:::::"+Inet4Address.getLocalHost().getHostAddress());
	    
    	
			return(IP[0].equals(Inet4Address.getLocalHost().getHostAddress()));
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    	return false;
    	
    }
	public void run() {
        try { 
        	String cmd;
        	if(!mTachyon)
            	cmd = makeRegionCloudCmd();
        	else
        		cmd = makeRegionCloudCmdTachyon();
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
	public void setTachyonFlag(){
    	mTachyon = true;
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
    //    String dstHDFS = mConf.getTopCloudHDFSURL();
        if ( srcHDFS.equals(mDistUrl) ) {
            mRunFlag = false;
            return "";
        }
        String[] Host = mDistUrl.split(":");
        String host = Host[1].substring(2);
        mRunFlag = true;
        //ssh hpds@140.116.164.101 ls
        //list HOME Directory of hpds
        // nn1 to nn2
        // bash$ hadoop distcp hdfs://nn1:8020/foo/bar hdfs://nn2:8020/bar/foo
        String cmd = hadoop_home + "/bin/hadoop distcp "; 
        //cmd = cmd + " -libjars "+ hadoop_home + "/tachyon-client-0.9.0-SNAPSHOT-jar-with-dependencies.jar ";
       // cmd = cmd + " tachyon://"+host +":19998"+ "/" + outputPath + " "  + dstHDFS +"user/" + userName + "/" ;
        //cmd = cmd  + outputPath + " "  + dstHDFS +"user/" + userName + "/" ;
        TopCloudHasher tch = new TopCloudHasher();
        String fileName = tch.hashToTop(mDistUrl);
        cmd = cmd + srcHDFS + "user/"+ userName + "/" + outputPath + fileName + " "  + mDistUrl +"user/" + userName + "/" + outputPath+"/"+fileName ;
        System.out.println("RegionCloud Distcp [" + cmd + "]" ); 
        return cmd;
    }
    //--------------------------------------------
    private String makeRegionCloudCmdTachyon() {
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
        String[] Host = srcHDFS.split(":");
        String host = Host[1].substring(2);
        mRunFlag = true;
        //ssh hpds@140.116.164.101 ls
        //list HOME Directory of hpds
        // nn1 to nn2
        // bash$ hadoop distcp hdfs://nn1:8020/foo/bar hdfs://nn2:8020/bar/foo
        String cmd = hadoop_home + "/bin/hadoop distcp "; 
        cmd = cmd + " -libjars "+ hadoop_home + "/tachyon-client-0.9.0-SNAPSHOT-jar-with-dependencies.jar ";
       // cmd = cmd + " tachyon://"+host +":19998"+ "/" + outputPath + " "  + dstHDFS +"user/" + userName + "/" ;
        cmd = cmd  + outputPath + " "  + dstHDFS +"user/" + userName + "/" ;

       // cmd = cmd + srcHDFS + "user/"+ userName + "/" + outputPath + " "  + dstHDFS +"user/" + userName + "/" + outputPath ;
        System.out.println("RegionCloud Distcp [" + cmd + "]" ); 
        return cmd;
    }
    //--------------------------------------------
}


