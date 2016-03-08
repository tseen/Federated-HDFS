/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2 ;

import java.net.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.*;

public class FedRegionCloudJob extends Thread {
    private FedHadoopConf mConf;
    private ShellMonitor mOutputMonitor;
    private ShellMonitor mErrorMonitor;
    private boolean mRunFlag = false;
    private boolean mTachyon = false;
    public FedRegionCloudJob(FedHadoopConf conf)  {
        System.out.println("init FedRegionCloudJob");
        mConf = conf;
    }
    public void run() {
        System.out.println("run FedRegionCloudJob");
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
                mOutputMonitor = new ShellMonitor( proc.getInputStream(), mConf.getJobName() + "-" + mConf.getName() + "-" + mConf.getAddress() );
                mErrorMonitor = new ShellMonitor( proc.getErrorStream(), mConf.getJobName()  + "-" + mConf.getName() + "-" + mConf.getAddress() );
                mOutputMonitor.start();
                mErrorMonitor.start();
                mOutputMonitor.join();
                mErrorMonitor.join();
                proc.waitFor();
            } 
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    public void setTachyonFlag(){
    	mTachyon = true;
    }
    public boolean getRunFlag() {
        return mRunFlag;
    }
    //--------------------------------------------
    
    private String makeRegionCloudCmd() {
        System.out.println("make region cloud command");
        if ( mConf == null ) {
            System.out.println("Null FedHadoopConf");
            return "";
        }
        if ( mConf.getAddress().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid host address of FedHadoopConf");
            return "";
        } 
        if ( mConf.getHadoopHome().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid Hadoop Home of FedHadoopConf");
            return "";
        } 
        if ( mConf.getJarPath().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid MapReduce JAR Path of FedHadoopConf");
            return "";
        } 
        /*
        if ( mConf.getMainClass().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid Main Class of FedHadoopConf");
            return "";
        } 
        */
        List<String> topCloudHDFSs = mConf.getTopCloudHDFSURLs();
        mRunFlag = true;
        String topCloudHDFS = "hdfs://"+topCloudHDFSs.get(0);
        for(int i =1; i< topCloudHDFSs.size(); i++){
        	topCloudHDFS += ","+"hdfs://"+topCloudHDFSs.get(i);
        }
        String userName = System.getProperty("user.name");  

        //ssh hpds@140.116.164.101 ls
        //list HOME Directory of hpds
        String cmd = "ssh " + mConf.getAddress() + " ";
        cmd = cmd + mConf.getHadoopHome() + "/bin/hadoop jar "; 
        cmd = cmd + mConf.getJarPath() + " ";
        if ( mConf.getMainClass().length() > 0 &&
             mConf.getMainClass().equals(FedJobConfParser.INVALID_VALUE) == false
           ) {
            cmd = cmd + " " + mConf.getMainClass();
        }
        cmd = cmd + " -Dclasspath="+ mConf.getJarPath(); 
        cmd = cmd + " -DregionCloud=on ";
        //cmd = cmd + " -D topCloudHDFS=\"" + mConf.getTopCloudHDFSURL() +"\" ";
        cmd = cmd + " -DtopCloudHDFSs=" + topCloudHDFS + " "; 
        cmd = cmd + " -DregionCloudServerPort=" + mConf.getRegionCloudServerListenPort() + " ";
        if(mConf.getMultiMapper() != ""){
	        cmd = cmd + " -DmultiMapper=" + mConf.getMultiMapper();
			cmd = cmd + " -DmultiFormat=" + mConf.getMultiFormat();
        }
        cmd = cmd + " -DregionCloudOutput="+mConf.getHDFSOutputPath();
        cmd = cmd + " -DregionCloudHadoopHome=" + mConf.getHadoopHome() + " ";
		cmd = cmd + " -DfedCloudHDFS="+ mConf.getTopCloudHDFSURL();
        cmd = cmd + " -DregionCloudInput="+ mConf.getHDFSInputPath() + " ";
        cmd = cmd + " -DtopCounts="+ Integer.toString(TopCloudHasher.topCounts);
        cmd = cmd + " -DtopNumbers=" + mConf.getTopTaskNumbers() + " ";
        cmd = cmd + " -DproxyReduce=" + mConf.getProxyReduce() + " ";

        
		for(Map.Entry<String, String> e : mConf.getUserConfig().entrySet()){
        	//Entry<String, String> e = confIter.next();
        	cmd = cmd +" -D"+e.getKey()+"="+e.getValue()+" ";
        }
        cmd = cmd + mConf.getOtherArgs() + " ";
        for ( int i = 0 ; i < 10 ; i++ ) {
            String arg = mConf.getArgs(i);
            System.out.println("Arg " + i + " = " + arg );
            if ( arg.equals( FedJobConfParser.INVALID_VALUE ) == false ) {
                cmd = cmd + arg + " ";
            }
        }
        //cmd = cmd + mConf.getHDFSInputPath() + " ";
        //cmd = cmd + mConf.getHDFSOutputPath() + " ";
        System.out.println("RegionCloud cmd " + cmd ); 
        return cmd;
    }
    //--------------------------------------------
    
    
    private String makeRegionCloudCmdTachyon() {
        System.out.println("make region cloud command");
        if ( mConf == null ) {
            System.out.println("Null FedHadoopConf");
            return "";
        }
        if ( mConf.getAddress().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid host address of FedHadoopConf");
            return "";
        } 
        if ( mConf.getHadoopHome().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid Hadoop Home of FedHadoopConf");
            return "";
        } 
        if ( mConf.getJarPath().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid MapReduce JAR Path of FedHadoopConf");
            return "";
        } 
        /*
        if ( mConf.getMainClass().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid Main Class of FedHadoopConf");
            return "";
        } 
        */
        mRunFlag = true;
        String userName = System.getProperty("user.name");  

        //ssh hpds@140.116.164.101 ls
        //list HOME Directory of hpds
        String cmd = "ssh " + mConf.getAddress() + " ";
        cmd = cmd + mConf.getHadoopHome() + "/bin/hadoop jar "; 
        cmd = cmd + mConf.getJarPath() + " ";
        if ( mConf.getMainClass().length() > 0 &&
             mConf.getMainClass().equals(FedJobConfParser.INVALID_VALUE) == false
           ) {
            cmd = cmd + " " + mConf.getMainClass();
        }
        //TODO add tachyon configuration in XML
        cmd = cmd + " -libjars "+mConf.getHadoopHome()+ "/tachyon-client-0.9.0-SNAPSHOT-jar-with-dependencies.jar";
        cmd = cmd + " -Dtachyon.user.file.understoragetype.default=SYNC_PERSIST";
        cmd = cmd + " -Dtachyon=on ";
        
        cmd = cmd + " -DregionCloud=on ";
        //cmd = cmd + " -D topCloudHDFS=\"" + mConf.getTopCloudHDFSURL() +"\" ";
        cmd = cmd + " -DtopCloudHDFS=" + mConf.getTopCloudHDFSURL() + " "; 
		cmd = cmd + " -DfedCloudHDFS="+ mConf.getTopCloudHDFSURL();
		cmd = cmd + " -DmultiMapper=" + mConf.getMultiMapper();
		cmd = cmd + " -DmultiFormat=" + mConf.getMultiFormat();
        cmd = cmd + " -DregionCloudServerPort=" + mConf.getRegionCloudServerListenPort() + " "; 
        cmd = cmd + " -DregionCloudOutput=tachyon://"+mConf.getAddress()+":19998/user/"+userName+"/" + mConf.getHDFSOutputPath() + " ";
        cmd = cmd + " -DregionCloudHadoopHome=" + mConf.getHadoopHome() + " ";
        cmd = cmd + " -DregionCloudInput="+ mConf.getHDFSInputPath() + " ";
        cmd = cmd + " -DtopNumbers=" + mConf.getTopTaskNumbers() + " ";
        cmd = cmd + mConf.getOtherArgs() + " ";
        for ( int i = 0 ; i < 10 ; i++ ) {
            String arg = mConf.getArgs(i);
            System.out.println("Arg " + i + " = " + arg );
            if ( arg.equals( FedJobConfParser.INVALID_VALUE ) == false ) {
                cmd = cmd + arg + " ";
            }
        }
        //cmd = cmd + mConf.getHDFSInputPath() + " ";
        //cmd = cmd + mConf.getHDFSOutputPath() + " ";
        System.out.println("RegionCloud cmd " + cmd ); 
        return cmd;
    }
}


