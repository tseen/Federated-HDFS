/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.util.Time;

import ncku.hpds.fed.MRv2.FedCloudMonitorServer.FedClientSocket;


public class FedJobServer extends Thread{
	private ServerSocket mServer;
	private int mListenPort = 0;
	private FedClientSocket mAcceptFedSocket = null;
	private boolean mRunFlag = false;
	private Object mLock = new Object();
	private FedCloudProtocol.FedSocketState mState = 
			FedCloudProtocol.FedSocketState.NONE;
	private Map<String,FedCloudInfo> mFedCloudInfos = new HashMap<String, FedCloudInfo>();
	private Map<String,String> mDownSpeed = new HashMap<String, String>();
	private AtomicBoolean isSet = new AtomicBoolean(false);
	private ArrayList<String> mFedCloudName;

	class FedClientSocket {
		public Socket mClientSocket = null;
		public InputStream mIS = null;
		public OutputStream mOUT = null;
		public BufferedReader mInputReader = null;
		public PrintWriter mOutputWriter = null;
		public FedClientSocket ( Socket s ) throws Exception {
			mClientSocket = s;
			mClientSocket.setSoTimeout(500); // 500ms, 0.5s
			mClientSocket.setTcpNoDelay(true);
			mIS = mClientSocket.getInputStream();
			mOUT = mClientSocket.getOutputStream();
			mInputReader = new BufferedReader(new InputStreamReader(mIS));
			mOutputWriter = new PrintWriter(mOUT);
		}
		public BufferedReader getInputReader() { return mInputReader; }
		public PrintWriter getOutputWriter() { return mOutputWriter;  }
		public void close() { 
			try { 
				mClientSocket.close(); 
			} catch ( Exception e ) {
				e.printStackTrace();
			}
		}
	} 
	
	public FedJobServer(int port) {
		mListenPort = port;
	} 

	public void run() { 
		try {
			System.out.println("Start fedjobServer");
			mServer = new ServerSocket(mListenPort);
			mServer.setSoTimeout(60000); //wait 60 second
			mServer.setReuseAddress(true);
			//mServer.setTcpNoDelay(true);
			mRunFlag = true;
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		int i = 0;
		while ( mRunFlag ) {
			
				
				synchronized ( mLock ) { 
					mState = FedCloudProtocol.FedSocketState.ACCEPTING;
				}
				if ( i > 12 ) {
					System.out.println("FedJobManager no incoming connection, break out");
					break;
				}
				Socket s;
				try {
					s = mServer.accept();
				//	System.out.println("ACCEPTED");
					jobServer js = new jobServer(s, mFedCloudInfos, mDownSpeed, isSet);
					
					js.start();
					if ( s == null ) {
						i ++;
						continue;
					}
					//mAcceptFedSocket = new FedClientSocket(s); 
					//something incoming
				} catch (IOException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
		}
		try {
			if ( mServer != null ) {
					mServer.close();
					System.out.println("Fed Cloud sub Server Closed");
			} else {
					System.out.println("mServer == null");
			}
		} catch ( Exception e ) {
				//e.printStackTrace();
		}
			
	}
	public void stopServer() {
		try { 
			Thread.sleep(5000);
			mRunFlag = false;
			this.join();
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}

	public Map<String,FedCloudInfo> getFedCloudInfos() {
		return mFedCloudInfos;
	}
	
	synchronized boolean clusterMapFinish(String c){
		if(mFedCloudName.remove(c)){
			mFedCloudName.trimToSize();
		}
		if(mFedCloudName.size() == 0)
			return true;
		else
			return false;
	}
	
	public void restoreName(Map<String,FedCloudInfo> mFedCloudInfos) {
		mFedCloudName = new ArrayList<String>(mFedCloudInfos.size());
		for(Entry<String, FedCloudInfo> info : mFedCloudInfos.entrySet()){
			mFedCloudName.add(info.getKey());
		}
	}
	public void setFedCloudInfos(Map<String,FedCloudInfo> mFedCloudInfos) {
		mFedCloudName = new ArrayList<String>(mFedCloudInfos.size());
		for(Entry<String, FedCloudInfo> info : mFedCloudInfos.entrySet()){
			mFedCloudName.add(info.getKey());
		}
		this.mFedCloudInfos = mFedCloudInfos;
	}
	public Map<String,String> getDownSpeed() {
		return mDownSpeed;
	}

	/*public void setDownSpeed(Map<String,Double> mDownSpeed) {
		this.isSet.set(true);
		this.mDownSpeed = mDownSpeed;
	}*/
	public void resetGetInfo(){
		this.isSet.set(false);
	}
	public void setDownSpeed(Map<String,String> mDownSpeed) {
		this.isSet.set(true);
		this.mDownSpeed = mDownSpeed;
	}
	class jobServer extends Thread{
		//private ServerSocket mServer;
		//private int mListenPort = 0;
		private FedClientSocket nAcceptFedSocket = null;
		private boolean nRunFlag = false;
		private Object nLock = new Object();
		private FedCloudProtocol.FedSocketState nState = 
				FedCloudProtocol.FedSocketState.NONE;
		private Map<String,FedCloudInfo> nFedCloudInfos = new HashMap<String, FedCloudInfo>();
		private Socket nSocket = null;
		private Map<String,String> nDownSpeed = new HashMap<String, String>();
		private final  AtomicBoolean isSet;

		
		public jobServer(Socket s, Map<String, FedCloudInfo> nFedCloudInfos2, 
				Map<String,String> nDownSpeed2, AtomicBoolean isset){
			super("jobServer");
			this.nSocket = s;
			this.nFedCloudInfos = nFedCloudInfos2;
			this.nDownSpeed = nDownSpeed2;
			this.isSet = isset;
		}

		public void run() { 
			nRunFlag  = true;
			//int i = 0;
			while ( nRunFlag ) {
				try {
					int getNumber = 0;
					BufferedReader clientInput = null;
					PrintWriter clientOutput = null;
					String line = null;
					
					
					nAcceptFedSocket = new FedClientSocket(nSocket); 
					//something incoming
					clientInput = nAcceptFedSocket.getInputReader();
					clientOutput = nAcceptFedSocket.getOutputWriter();
					synchronized ( nLock ) { 
						nState = FedCloudProtocol.FedSocketState.ACCEPTED;
					}
					while ( nRunFlag ) {
						synchronized ( nLock ) { 
							try {
								line = clientInput.readLine(); 
								//System.out.println("Recv : " + line );
								if ( line.contains( FedCloudProtocol.REQ_PING ) ) {
									//System.out.println("PINGGGG");
									//clientOutput.println( FedCloudProtocol.RES_PONG );
									//clientOutput.flush();
								}
								else{
									//System.out.println("SERVER GET:"+line);
								}
								if ( line.contains( FedCloudProtocol.REQ_BYE ) ) {
									clientOutput.println( FedCloudProtocol.RES_BYE );
									clientOutput.flush();
									System.out.println("Server Thread Recvied Bye");
									nRunFlag = false;
									break;
								}
						/*		if ( line.contains( FedCloudProtocol.REQ_REGION_MAP_FINISHED ) ) {
									String namdenode = line.substring(20);
									clientOutput.println( FedCloudProtocol.RES_REGION_MAP_FINISHED );
									clientOutput.flush();
									System.out.println("-----------------------------");
									System.out.println("get region map stop time"+nSocket.getInetAddress().toString());
									System.out.println("-----------------------------");
									//System.out.println(namdenode);
									//System.out.println(nFedCloudInfos.toString());
									nFedCloudInfos.get(namdenode).setRegionMapTime((int) System.currentTimeMillis());
									nRunFlag = false;
									clientInput.close();
									clientOutput.close();
									break;
								}*/
								if ( line.contains( FedCloudProtocol.REQ_INTER_INFO ) ) {
									long minInputsize = Long.MAX_VALUE;
									String res = "";
									System.out.println("-----------------------------");
									System.out.println("get inter data Info"+nSocket.getInetAddress().toString());
									System.out.println("-----------------------------");
									for (Entry<String, FedCloudInfo> info : mFedCloudInfos.entrySet())
									{
										FedCloudInfo fedInfo = info.getValue();
										System.out.println("Inter size:"+fedInfo.getCloudName()+"="+fedInfo.getInterSize());
										if( fedInfo.getInterSize() < minInputsize){
											minInputsize = fedInfo.getInterSize();
										}
									}
									for (Entry<String, FedCloudInfo> info : mFedCloudInfos.entrySet()) 
									{
										FedCloudInfo fedInfo = info.getValue();
										fedInfo.setInterSize_normalized((double)fedInfo.getInterSize() /(double) minInputsize);
										res += info.getKey()+"="
												 +info.getValue().getInterSize_normalized()+",";
									}
									System.out.println(res);
									clientOutput.println( FedCloudProtocol.RES_INTER_INFO+" "+res );
									clientOutput.flush();
									
									
								}
								if ( line.contains( FedCloudProtocol.REQ_WAIT_BARRIER ) ) {
									String cluster = line.substring(12);
									if(clusterMapFinish(cluster)){
										System.out.println("ALL MAP FINISH");
										clientOutput.println( FedCloudProtocol.RES_FALSE_BARRIER );
										clientOutput.flush();
									}
									else{
										clientOutput.println( FedCloudProtocol.RES_TRUE_BARRIER );
										clientOutput.flush();
									}
								}
								if ( line.contains( FedCloudProtocol.REQ_INFO ) ) {
									
									System.out.println("-----------------------------");
									System.out.println("get Info 1"+nSocket.getInetAddress().toString());
									System.out.println("-----------------------------");
									while(!isSet.get()){
									}
									System.out.println("START REPLY:"+nSocket.getInetAddress().toString() );
									String res = "";
									//for (Map.Entry<String, Double> entry : mDownSpeed.entrySet())
									//{
									//	res += entry.getKey()+"="+entry.getValue()+",";
									//}
									long totalWanWaitingTime = 0;
									long totalTopWaitingTime = 0;
									for (Entry<String, FedCloudInfo> entry : mFedCloudInfos.entrySet())
									{
										/*res += entry.getKey()+"="+entry.getValue().getActiveNodes()+";"
																 +entry.getValue().getAvailableMB()+";"
																 +entry.getValue().getAvailableVcores()+";"
																 +entry.getValue().getInputSize()+",";
																 */
										totalWanWaitingTime += entry.getValue().getWanWaitingTime();
										totalTopWaitingTime += entry.getValue().getReduceWaitingTime();
										res += entry.getKey()+">>"
												 +entry.getValue().getDownLinkSpeed_normalized()+";"
												 +entry.getValue().getAvailableMB_normalized()+";"
												 +entry.getValue().getAvailableVcores_normalized()+",";
												 //+entry.getValue().getInputSize_normalized()+",";
									}
									System.out.println("totalWanWaitingTime:" + totalWanWaitingTime);
									System.out.println("totalTopWaitingTime:" + totalTopWaitingTime);
									if(totalWanWaitingTime > totalTopWaitingTime){
										// alpha add;minus beta add;minus
										res += "0.05;0;0;0.05,";
									}
									else if(totalWanWaitingTime < totalTopWaitingTime){
										res += "0;0.05;0.05;0,";
									}
									else{
										res += "0;0;0;0,";
									}
									System.out.println(res);
									clientOutput.println( FedCloudProtocol.RES_INFO+" "+res );
									clientOutput.flush();
									
									nRunFlag = false;
									clientInput.close();
									clientOutput.close();
									
								}
								if ( line.contains( FedCloudProtocol.REQ_INFO_2 ) ) {
									
									System.out.println("-----------------------------");
									System.out.println("get Info"+nSocket.getInetAddress().toString());
									System.out.println("-----------------------------");
									while(!isSet.get()){
									}
									System.out.println("START REPLY:"+nSocket.getInetAddress().toString() );
									String res = "";
									//for (Map.Entry<String, Double> entry : mDownSpeed.entrySet())
									//{
									//	res += entry.getKey()+"="+entry.getValue()+",";
									//}
									for (Entry<String, FedCloudInfo> entry : mFedCloudInfos.entrySet())
									{
										/*res += entry.getKey()+"="+entry.getValue().getActiveNodes()+";"
																 +entry.getValue().getAvailableMB()+";"
																 +entry.getValue().getAvailableVcores()+";"
																 +entry.getValue().getInputSize()+",";
																 */
										res += entry.getKey()+">>"
												 +entry.getValue().getDownLinkSpeed_normalized()+";"
												 +entry.getValue().getReduceSpeed_normalized()+";"
												// +entry.getValue().getReduceInputSize()+";"
												 +entry.getValue().getInterTime()+";"
												 +entry.getValue().getTopTime()+";"
												 +entry.getValue().getInterSize_normalized()+",";
									}
								
									System.out.println(res);
									clientOutput.println( FedCloudProtocol.RES_INFO+" "+res );
									clientOutput.flush();
									
									nRunFlag = false;
									clientInput.close();
									clientOutput.close();
									
								}
								if ( line.contains( FedCloudProtocol.REQ_REGION_WAN ) ) {
									String infos = line.substring(11);
									String[] info = infos.split(">");
									clientOutput.println( FedCloudProtocol.RES_REGION_WAN );
									clientOutput.flush();
									System.out.println("-----------------------------");
									System.out.println("get region map WAN"+nSocket.getInetAddress().toString()+ "|"+ infos );
									System.out.println("-----------------------------");
									nFedCloudInfos.get(info[0]).setWanSpeed(info[1], Double.parseDouble(info[2]));
									getNumber++;
									if(getNumber == nFedCloudInfos.size()){
										nRunFlag = false;
										clientInput.close();
										clientOutput.close();
									}
									
								}
								if ( line.contains( FedCloudProtocol.REQ_INTER_SIZE) ) {
									String infos = line.substring(15);
									String[] info = infos.split(",");
									clientOutput.println( FedCloudProtocol.RES_INTER_SIZE );
									clientOutput.flush();
									//System.out.println("-----------------------------");
									System.out.println("get region inter-data size: "+nSocket.getInetAddress().toString()+ " = "+ infos );
									//System.out.println("-----------------------------");
									nFedCloudInfos.get(info[0]).setInterSize(Long.parseLong(info[1]));
								}
								if ( line.contains( FedCloudProtocol.REQ_REGION_RESOURCE) ) {
									String infos = line.substring(14);
									String[] info = infos.split(",");
									clientOutput.println( FedCloudProtocol.RES_REGION_WAN );
									clientOutput.flush();
									System.out.println("-----------------------------");
									System.out.println("get region resource"+nSocket.getInetAddress().toString()+ "|"+ infos );
									System.out.println("-----------------------------");
									nFedCloudInfos.get(info[0]).setActiveNodes(Integer.parseInt(info[1]));
									nFedCloudInfos.get(info[0]).setAvailableMB(Integer.parseInt(info[2]));
									nFedCloudInfos.get(info[0]).setAvailableVcores(Integer.parseInt(info[3]));
									
									
								}
								if ( line.contains( FedCloudProtocol.REQ_RM_START) ) {
									System.out.println(line);
									String infos = line.substring(13);
									clientOutput.println( "ok" );
									clientOutput.flush();
									nFedCloudInfos.get(infos).setRegionStartTime();
								}
								if ( line.contains( FedCloudProtocol.REQ_RM_STOP) ) {
									System.out.println(line);
									String infos = line.substring(13);
									clientOutput.println( "ok" );
									clientOutput.flush();
									nFedCloudInfos.get(infos).setRegionMapStopTime();
								}
								if ( line.contains( FedCloudProtocol.REQ_INTER_START) ) {
									System.out.println(line);

									String infos = line.substring(13);
									clientOutput.println( "ok" );
									clientOutput.flush();
									nFedCloudInfos.get(infos).setInterStartTime();
									nRunFlag = false;
									clientInput.close();
									clientOutput.close();
								}
								if ( line.contains( FedCloudProtocol.REQ_INTER_STOP) ) {
									System.out.println(line);

									String infos = line.substring(13);
									clientOutput.println( "ok" );
									clientOutput.flush();
									nFedCloudInfos.get(infos).setInterStopTime();
									nRunFlag = false;
									clientInput.close();
									clientOutput.close();
								}
								if ( line.contains( FedCloudProtocol.REQ_TOP_START) ) {
									System.out.println(line);

									String infos = line.substring(10);
									clientOutput.println( "ok" );
									clientOutput.flush();
									nFedCloudInfos.get(infos).setTopStartTime();
									nRunFlag = false;
									clientInput.close();
									clientOutput.close();
								}
								if ( line.contains( FedCloudProtocol.REQ_TOP_STOP) ) {
									System.out.println(line);

									String infos = line.substring(10);
									clientOutput.println( "ok" );
									clientOutput.flush();
									nFedCloudInfos.get(infos).setTopStopTime();
									nRunFlag = false;
									clientInput.close();
									clientOutput.close();
								}
							} catch (Exception e) {
								//e.printStackTrace();
								//System.out.println("sockettimeout exeception occurs");
							}
						}
						Thread.sleep(500); // 500ms 
					}
				} catch ( Exception e ) { 
					//i++;
					e.printStackTrace();
					//System.out.println("accept error continue wait");
				} finally {
					synchronized ( nLock ) { 
						if ( nAcceptFedSocket != null ) {
							nAcceptFedSocket.close();
							nAcceptFedSocket = null;
						}
						nState = FedCloudProtocol.FedSocketState.DISCONNECTED;
					}
				}
			} 
			try {
				nSocket.close();
				this.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
	}
}
	

