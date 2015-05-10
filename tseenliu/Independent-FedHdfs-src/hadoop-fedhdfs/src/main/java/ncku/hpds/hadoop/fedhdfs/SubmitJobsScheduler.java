package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import ncku.hpds.hadoop.fedhdfs.shell.Ls;
import ncku.hpds.hadoop.fedhdfs.shell.Mkdir;
import ncku.hpds.hadoop.fedhdfs.shell.Union;

import org.apache.hadoop.conf.Configuration;

public class SubmitJobsScheduler {
	
	static ArrayList<String> requestGlobalFile;
	
	public static void main(String[] args) throws Throwable {
		
		String SNaddress = "127.0.0.1";
		int SNport = 8763;
		
		if (args.length < 5) {
			System.err.println("Usage: submit jar [jarFile] [program] [globalfileInput] [globalfileOutput]");
			System.exit(2);
		}
		
		String parseArg[] = args;
		String jarPath = parseArg[1];
		String jarFile = jarPath.substring(jarPath.lastIndexOf("/")+1, jarPath.length());
		String mainClass = parseArg[2];
		String globalfileInput = parseArg[3];
		String globalfileOutput = parseArg[4];
		
		Socket client = new Socket(SNaddress, SNport);
		
		try {
			
			OutputStream stringOut = client.getOutputStream();
			stringOut.write(globalfileInput.getBytes());
			System.out.println("send globalFile to GN query server : " + globalfileInput);
			
			ObjectInputStream objectIn = new ObjectInputStream(client.getInputStream());
			Object object = objectIn.readObject();
			
			requestGlobalFile = (ArrayList<String>) object;
			
            stringOut.flush();
            stringOut.close();
            stringOut = null;
			objectIn.close();
			client.close();
			client = null;

		} catch (java.io.IOException e) {
			System.out.println("Socket connect error");
			System.out.println("IOException :" + e.toString());
		}
		
		for ( String GNlink : requestGlobalFile ) { System.out.println(GNlink); }
		System.out.println("JAR : " + jarFile);
		
		List<multipleMR> listClusters = new ArrayList<multipleMR>();
		List<copyJar> listCpJar = new ArrayList<copyJar>();
		
		for (int i = 0; i < requestGlobalFile.size(); i++) {
			String tmpHostPath[] = requestGlobalFile.get(i).split(":");
			listCpJar.add(new copyJar(jarPath, tmpHostPath[0]));
		}
		
		for ( copyJar job : listCpJar ) { job.start(); }
		for ( copyJar job : listCpJar) { job.join(); } 
		
		for (int i = 0; i < requestGlobalFile.size(); i++) {
			String tmpHostPath[] = requestGlobalFile.get(i).split(":");
			listClusters.add(new multipleMR(jarFile, mainClass,tmpHostPath[0], tmpHostPath[1], globalfileOutput));
		}
		
		System.out.println("Start running RegionCloud Jobs");
		for ( multipleMR job : listClusters ) { job.start(); }
		System.out.println("Wait For RegionCloud Jobs");
        for ( multipleMR job : listClusters) { job.join(); } 
        System.out.println("RegionCloud Jobs all finished");
        
		Mkdir mkdirGN = new Mkdir();
		mkdirGN.constructGlobalFile("-mkdir", globalfileOutput);
		
        for (int i = 0; i < requestGlobalFile.size(); i++) {
        	String tmpHostPath[] = requestGlobalFile.get(i).split(":");
        	Union unionGlobalFile = new Union();
    		unionGlobalFile.union("-union", globalfileOutput, tmpHostPath[0] + ":/user/hpds/" + globalfileOutput);
        }
	}
}

class multipleMR extends Thread {
	
	private String JAR;
	private String mainClass;
	private String hostName;
	private String input;
	private String output;

	public multipleMR(String JAR, String mainClass,String hostName, String input, String output) {
		
		this.JAR = JAR;
		this.mainClass = mainClass;
		this.hostName = hostName;
		this.input = input;
		this.output = output;
	}
	
	File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
	
	@Override
	public void run() {
		
		Runtime rt = Runtime.getRuntime();
		String HdfsUri = FedHdfsConParser.getHdfsUri(FedConfpath, hostName);
		String split[] = HdfsUri.split(":");
		String HostAddress = split[0];
		String cmd = "ssh hpds@" + HostAddress + " ";
		cmd = cmd + FedHdfsConParser.getHadoopHOME(FedConfpath, hostName) + "/bin/hadoop jar" + " ";
		cmd = cmd + FedHdfsConParser.getHadoopHOME(FedConfpath, hostName) + "/" + JAR + " ";
		cmd = cmd + mainClass + " ";
		cmd = cmd + input + " " + output;
	
		System.out.println(cmd);
		
		Process proc;
		try {
			proc = rt.exec(cmd);
			InputStream stderr = proc.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stderr);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			System.out.println("<ERROR>");
			while ((line = br.readLine()) != null)
				System.out.println(line);
			System.out.println("</ERROR>");

			int exitVal = proc.waitFor();
			System.out.println("Process exitValue: " + exitVal);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class copyJar extends Thread {
	
	private String JAR;
	private String hostName;

	public copyJar(String JAR, String hostName) {
		this.hostName = hostName;
		this.JAR = JAR;
	}
	
	File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
	
	@Override
	public void run() {
		
		Runtime rt = Runtime.getRuntime();
		String HdfsUri = FedHdfsConParser.getHdfsUri(FedConfpath, hostName);
		String split[] = HdfsUri.split(":");
		String HostAddress = split[0];
		String cmd = "scp" + " " + JAR + " ";
		cmd = cmd + "hpds@" + HostAddress + ":" + FedHdfsConParser.getHadoopHOME(FedConfpath, hostName);
		System.out.println(cmd);
		
		Process proc;
		try {
			proc = rt.exec(cmd);
			InputStream stderr = proc.getErrorStream();
			InputStreamReader isr = new InputStreamReader(stderr);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			System.out.println("<ERROR>");
			while ((line = br.readLine()) != null)
				System.out.println(line);
			System.out.println("</ERROR>");

			int exitVal = proc.waitFor();
			System.out.println("Process exitValue: " + exitVal);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
