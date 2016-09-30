/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FedTopCloudJob extends Thread {
	private FedHadoopConf mConf;
	private ShellMonitor mOutputMonitor;
	private ShellMonitor mErrorMonitor;
	private boolean mRunFlag = false;
	private boolean mIterFlag = false;
	private int currentIter = 0;

	public FedTopCloudJob(FedHadoopConf conf) {
		System.out.println("init FedTopCloudJob");
		mConf = conf;
	}
	public void setIterFlag(int i ){
		currentIter = i;
		mIterFlag = true;
	}
	
	public void run() {
		System.out.println("run FedTopCloudJob");
		try {
			String cmd = "";
			if(mIterFlag){
				 cmd = makeTopCloudCmdIter();
			}else{	
				 cmd = makeTopCloudCmd();
			}
			System.out.println("top cmd:" + cmd);
			if (mRunFlag) {
				Runtime rt = Runtime.getRuntime();
				// copy configuration into Region Cloud first
				Process proc = rt.exec(cmd);
				mOutputMonitor = new ShellMonitor(proc.getInputStream(),
						mConf.getJobName() + "-" + mConf.getName() + "-"
								+ mConf.getAddress());
				mErrorMonitor = new ShellMonitor(proc.getErrorStream(),
						mConf.getJobName() + "-" + mConf.getName() + "-"
								+ mConf.getAddress());
				mOutputMonitor.start();
				mErrorMonitor.start();
				mOutputMonitor.join();
				mErrorMonitor.join();
				proc.waitFor();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean getRunFlag() {
		return mRunFlag;
	}

	// --------------------------------------------
	private String makeTopCloudCmd() {
		System.out.println("make top cloud command");
		if (mConf == null) {
			System.out.println("Null FedHadoopConf");
			return "";
		}
		if (mConf.getAddress().equals(FedJobConfParser.INVALID_VALUE)) {
			System.out.println("Invalid host address of FedHadoopConf");
			return "";
		}
		if (mConf.getHadoopHome().equals(FedJobConfParser.INVALID_VALUE)) {
			System.out.println("Invalid Hadoop Home of FedHadoopConf");
			return "";
		}
		if (mConf.getJarPath().equals(FedJobConfParser.INVALID_VALUE)) {
			System.out.println("Invalid MapReduce JAR Path of FedHadoopConf");
			return "";
		}
		/*
		 * if ( mConf.getMainClass().equals( FedJobConfParser.INVALID_VALUE ) )
		 * { System.out.println("Invalid Main Class of FedHadoopConf"); return
		 * ""; }
		 */
		mRunFlag = true;
		// ssh hpds@140.116.164.101 ls
		// list HOME Directory of hpds
		String cmd = "ssh " + mConf.getAddress() + " ";
		cmd = cmd + mConf.getHadoopHome() + "/bin/hadoop jar ";
		cmd = cmd + mConf.getJarPath() + " ";
		if (mConf.getMainClass().length() > 0
				&& mConf.getMainClass().equals(FedJobConfParser.INVALID_VALUE) == false) {
			cmd = cmd + " " + mConf.getMainClass();
		}
		cmd = cmd + " -DtopCloud=on ";
		// cmd = cmd + " -D topCloudHDFS=\"" + mConf.getTopCloudHDFSURL()
		// +"\" ";
		// cmd = cmd + " -DtopCloudHDFS=" + mConf.getTopCloudHDFSURL() + " ";
		// cmd = cmd + " -DregionCloudServerPort=" +
		// mConf.getRegionCloudServerListenPort() + " ";
		cmd = cmd + " -DtopCloudInput=" + mConf.getHDFSInputPath() + " ";
		String outs[] = mConf.getHDFSOutputPath().split("/");
		String out = "";
		for(int i = 0; i<outs.length;i++){
			if(i >2){
				out += "/";
				out += outs[i];	
			}
		}
		cmd = cmd + " -DtopCloudOutput=" + out + " ";
		cmd = cmd + " -DfedCloudHDFS="+ mConf.getTopCloudHDFSURL();
		cmd = cmd + " -DtopCloudHadoopHome=" + mConf.getHadoopHome() + " ";
		
	       //----for reallocation----
			List<String> topCloudHDFSs = mConf.getTopCloudHDFSURLs();
		    String topCloudHDFS = "hdfs://"+topCloudHDFSs.get(0);
		        for(int i =1; i< topCloudHDFSs.size(); i++){
		        	topCloudHDFS += ","+"hdfs://"+topCloudHDFSs.get(i);
		        }
		    cmd = cmd + " -DtopCloudHDFSs=" + topCloudHDFS + " "; 
		    //------------------------
	
		for(Map.Entry<String, String> e : mConf.getUserConfig().entrySet()){
        	//Entry<String, String> e = confIter.next();
        	cmd = cmd +" -D"+e.getKey()+"="+e.getValue()+" ";
        }
		cmd = cmd + mConf.getOtherArgs() + " ";
		for (int i = 0; i < 10; i++) {
			String arg = mConf.getArgs(i);
			System.out.println("Arg " + i + " = " + arg);
			if (arg.equals(FedJobConfParser.INVALID_VALUE) == false) {
				cmd = cmd + arg + " ";
			}
		}
		
 
		// cmd = cmd + mConf.getHDFSInputPath() + " ";
		// cmd = cmd + mConf.getHDFSOutputPath() + " ";
		System.out.println("TopCloud cmd " + cmd);
		return cmd;
	}
	private String makeTopCloudCmdIter() {
		System.out.println("make top cloud command");
		if (mConf == null) {
			System.out.println("Null FedHadoopConf");
			return "";
		}
		if (mConf.getAddress().equals(FedJobConfParser.INVALID_VALUE)) {
			System.out.println("Invalid host address of FedHadoopConf");
			return "";
		}
		if (mConf.getHadoopHome().equals(FedJobConfParser.INVALID_VALUE)) {
			System.out.println("Invalid Hadoop Home of FedHadoopConf");
			return "";
		}
		if (mConf.getJarPath().equals(FedJobConfParser.INVALID_VALUE)) {
			System.out.println("Invalid MapReduce JAR Path of FedHadoopConf");
			return "";
		}
		
		mRunFlag = true;
		
		String cmd = "ssh " + mConf.getAddress() + " ";
		cmd = cmd + mConf.getHadoopHome() + "/bin/hadoop jar ";
		cmd = cmd + mConf.getJarPath() + " ";
		if (mConf.getMainClass().length() > 0
				&& mConf.getMainClass().equals(FedJobConfParser.INVALID_VALUE) == false) {
			cmd = cmd + " " + mConf.getMainClass();
		}
		cmd = cmd + " -DtopCloud=on ";
		cmd = cmd + " -DtopFedIter=true ";
		cmd = cmd + " -DfedCloudHDFS="+ mConf.getTopCloudHDFSURL();
		cmd = cmd + " -DiterName=ITERR";
		cmd = cmd + " -DtopCloudInput=" + mConf.getHDFSInputPath() + " ";
		String outs[] = mConf.getHDFSOutputPath().split("/");
		String out = outs[outs.length - 1];
		cmd = cmd + " -DtopCloudOutput=" + out + " ";
		cmd = cmd + " -DtopCloudHadoopHome=" + mConf.getHadoopHome() + " ";
		for(Map.Entry<String, String> e : mConf.getUserConfig().entrySet()){
        	//Entry<String, String> e = confIter.next();
        	cmd = cmd +" -D"+e.getKey()+"="+e.getValue()+" ";
        }
		//TODO fix
	
		cmd = cmd + mConf.getOtherArgs() + " ";
		for (int i = 0; i < 10; i++) {
			String arg = mConf.getArgs(i);
			System.out.println("Arg " + i + " = " + arg);
			if (arg.equals(FedJobConfParser.INVALID_VALUE) == false) {
				cmd = cmd + arg + " ";
			}
		}
		
		System.out.println("TopCloud cmd " + cmd);
		return cmd;
	}
}
