/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class MiniWANServer extends Thread {
	private Configuration mJobConf;

	private Socket socket = null;
	private Map<String, FedCloudInfo> mFedCloudInfos;
	private boolean mRunFlag = true;
	private Object mLock = new Object();
	public FedJobServerClient mClient;

	public MiniWANServer(Socket socket, Configuration jobConf, FedJobServerClient client) {

		super("MiniServer");
		this.mClient = client;
		this.mJobConf = jobConf;
		this.socket = socket;

	}

	public void run() {
		BufferedReader bufferReader = null;
		InputStream in = null;
		InputStream inBytes = null;

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

		byte[] bytes = new byte[5 * 1024 * 1024];

		//int count;
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
							System.out.println("START");
							
							inBytes = new ByteArrayInputStream(bytes);
							try {
								
								out = socket.getOutputStream();

								int count;
								while ((count = inBytes.read(bytes)) > 0) {
									out.write(bytes, 0, count);
								}
								System.out.println(namenode+"finish");
								//mRunFlag = true;
							}catch (IOException e) {
									// TODO Auto-generated catch block
								e.printStackTrace();
							}
							//mRunFlag = false;
						}
						else if (line.contains("SPEED")) {
							float speed =Float.parseFloat( line.substring(6));
							//mFedCloudInfos
							//.get(namenode)
							//.setWanSpeed(speed);
							String s = mJobConf.get("wanSpeed") == null ? " ": mJobConf.get("wanSpeed");
							s += namenode +"|"+Float.toString(speed)+",";
							mJobConf.set("wanSpeed", s);
							String res = mClient.sendRegionWAN(mJobConf.get("fs.default.name").split("/")[2], namenode, speed);
							System.out.println("-->"+namenode+"WAN SPEED: "+speed+"MB");
							System.out.println("-->"+namenode+"WAN SPEED: "+speed+"MB RESP:"+res);
							//mClient.sendBye();
							mRunFlag = false;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		/*	System.out.println("START");
			
			inBytes = new ByteArrayInputStream(bytes);
			try {
				
				out = socket.getOutputStream();

				int count;
				while ((count = inBytes.read(bytes)) > 0) {
					out.write(bytes, 0, count);
				}
				System.out.println(namenode+"finish");
				mRunFlag = true;
			}catch (IOException e) {
					// TODO Auto-generated catch block
				e.printStackTrace();
			}
			float speed = 0;
			while (mRunFlag) {

				synchronized (mLock) {
					try {
						System.out.println("LOCK");

						line = bufferReader.readLine();
						if (line.contains("SPEED")) {
							speed =Float.parseFloat( line.substring(6));
							System.out.println("WAN SPEED:"+namenode+" "+speed);
							mRunFlag = false;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
				
	     */
				
			
		/*	long total = 0;
			long startTime = System.currentTimeMillis();
			float speed = 0;
			
			while ((count = in.read(bytes)) > 0) {
				total += count;
			}
			long endTime = System.currentTimeMillis();
			System.out.printf(namenode +": read %,d bytes, speed: %,d MB/s%n", total, total/(endTime-startTime)/1000);
			System.out.println("END");
			*/
			
		//	mFedCloudInfos
		//	.get(namenode)
		//	.setWanSpeed(speed);
	

	

			
			// out.close();
			bufferReader.close();
			in.close();
			socket.close();
			out.close();
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
