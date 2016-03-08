/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

public class MiniWANServer extends Thread {

	private Socket socket = null;
	private Map<String, FedCloudInfo> mFedCloudInfos;
	private boolean mRunFlag = true;
	private Object mLock = new Object();

	public MiniWANServer(Socket socket) {

		super("MiniServer");
		this.socket = socket;

	}

	public void run() {
		BufferedReader bufferReader = null;
		InputStream in = null;
		OutputStream out = null;
		try {
			in = socket.getInputStream();
			bufferReader = new BufferedReader(new InputStreamReader(in));

		} catch (IOException ex) {
			System.out.println("Can't get socket input stream. ");
		}

		/*
		 * try { out = new FileOutputStream("test.xml"); } catch
		 * (FileNotFoundException ex) { System.out.println("File not found. ");
		 * }
		 */

		byte[] bytes = new byte[16 * 1024];

		int count;
		try {
			String line = null;
			String namenode = null;
			while (mRunFlag) {
				synchronized (mLock) {
					try {
						line = bufferReader.readLine();
						if (line.contains("STOP")) {
							namenode = line.substring(5);
							System.out.println("STOP namenode:"+namenode);
						mRunFlag = false;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			System.out.println("START");
			long startTime = System.currentTimeMillis();
			while ((count = in.read(bytes)) > 0) {
				// out.write(bytes, 0, count);
			}
			long endTime = System.currentTimeMillis();
			System.out.println("END");
			mFedCloudInfos
			.get(namenode)
			.setWanSpeed(
					(float) ((float) 20.0000f / (((float) endTime - (float) startTime) / 1000)));
	

			
			// out.close();
			bufferReader.close();
			in.close();
			socket.close();
			this.join();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// implement your methods here
	public void setFedCloudInfos(Map<String, FedCloudInfo> mFedCloudInfos) {
		this.mFedCloudInfos = mFedCloudInfos;
	}

}
