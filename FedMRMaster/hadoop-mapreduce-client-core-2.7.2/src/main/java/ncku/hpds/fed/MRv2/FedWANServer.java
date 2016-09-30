/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class FedWANServer extends Thread {
	private Configuration mJobConf;
	
	boolean listeningSocket = true;
	private Map<String, FedCloudInfo> mFedCloudInfos;
	private int port;
	public FedJobServerClient client;

	public void run() {
		String ip = mJobConf.get("fedCloudHDFS").split(":")[1].split("/")[2];
		InetAddress address;
		try {
			address = InetAddress.getByName(ip);
			client = new FedJobServerClient(address.getHostAddress(), 8713);
			client.start();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		ServerSocket serverSocket = null;

		try {
			//serverSocket = new ServerSocket(port);
            System.out.println("FedWANServer bind port " + port + " to listen ");
            serverSocket = new ServerSocket();
			serverSocket.setSoTimeout(60000); //wait 60 second
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));
		} catch (Exception e) {
			//System.out.println("Can't setup server on this port number. ");
			System.out.println("Can't setup server on this port number. " );
		        e.printStackTrace();
		}

		Socket socket = null;
		
		while (listeningSocket) {

			try {
				socket = serverSocket.accept();
				MiniWANServer mini = new MiniWANServer(socket, mJobConf, client);
				//mini.setFedCloudInfos(mFedCloudInfos);
				mini.start();
			} catch (IOException ex) {
				System.out.println("Can't accept client connection. ");
			}
		}
		try {
			socket.close();
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	public void stopServer() {
		try { 
			Thread.sleep(5000);
			client.stopClientProbe();
			listeningSocket = false;
			this.join();
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}
	public void setFedCloudInfos(Map<String,FedCloudInfo> mFedCloudInfos) {
		this.mFedCloudInfos = mFedCloudInfos;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
    System.out.println("wanServer port = " + port );
		this.port = port;
	}
	public Configuration getJobConf() {
		return mJobConf;
	}
	public void setJobConf(Configuration mJobConf) {
		this.mJobConf = mJobConf;
	}

}
