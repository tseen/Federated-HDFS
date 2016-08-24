/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2 ;

import java.net.*;
import java.io.*;

public class JarCopyJob extends Thread {
    private FedHadoopConf mTopConf;
    private FedHadoopConf mRegConf;
    private ShellMonitor mOutputMonitor;
    private ShellMonitor mErrorMonitor;
    private boolean mRunFlag = false;
    private String jarPath ="";
    public JarCopyJob (FedHadoopConf topConf, FedHadoopConf regConf)  {
        System.out.println("init JarCopyJob ");
        mTopConf = topConf;
        mRegConf = regConf;
        jarPath = mTopConf.getJarPath();
    }
    public JarCopyJob (String path, FedHadoopConf regConf)  {
        System.out.println("init JarCopyJob ");
        jarPath = path;
        mRegConf = regConf;
    }
    public void run() {
        System.out.println("run JarCopyJob ");
        try { 
            String cmd = makeRegionCloudCmd();
            if ( mRunFlag ) {
                Runtime rt = Runtime.getRuntime();
                //copy configuration into Region Cloud first
                Process proc = rt.exec(cmd);
                mOutputMonitor = new ShellMonitor( proc.getInputStream(), "JarCopyJob-" + mRegConf.getName() + "-" + mRegConf.getAddress() );
                mErrorMonitor = new ShellMonitor( proc.getErrorStream(), "JarCopyJob-" + mRegConf.getName() + "-" + mRegConf.getAddress() );
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
    public boolean getRunFlag() {
        return mRunFlag;
    }
    //--------------------------------------------
    private String makeRegionCloudCmd() {
        System.out.println("make jcj cloud command");
       /* if ( mTopConf == null || mRegConf == null ) {
            System.out.println("Null FedHadoopConf");
            return "";
        }
        if ( mRegConf.getAddress().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid host address of FedHadoopConf");
            return "";
        } 
        if ( mRegConf.getHadoopHome().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid Hadoop Home of FedHadoopConf");
            return "";
        } 
        if ( mTopConf.getJarPath().equals( FedJobConfParser.INVALID_VALUE ) ) {
            System.out.println("Invalid MapReduce JAR Path of FedHadoopConf");
            return "";
        }*/
        mRunFlag = true;
        //ssh hpds@140.116.164.101 ls
        //list HOME Directory of hpds
        String cmd = "scp " + jarPath + " ";
        cmd = cmd + mRegConf.getAddress() + ":" + mRegConf.getHadoopHome() + "/fed_task/";
        System.out.println("JarCopyJob cmd " + cmd );
        return cmd;
    }
    //--------------------------------------------
}


