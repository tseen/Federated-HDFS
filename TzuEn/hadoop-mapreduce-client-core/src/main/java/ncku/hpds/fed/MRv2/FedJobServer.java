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
import java.util.HashMap;
import java.util.Map;

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
	private Map<String,Double> mDownSpeed = new HashMap<String, Double>();

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
			System.out.println("Start fedjobServer with port " + mListenPort);
			//mServer = new ServerSocket(mListenPort);
			mServer = new ServerSocket();
			mServer.setSoTimeout(60000); //wait 60 second
			mServer.setReuseAddress(true);
            mServer.bind(new InetSocketAddress(mListenPort));
			//mServer.setTcpNoDelay(true);
			mRunFlag = true;
		} catch ( Exception e ) {
			System.out.println("FedJobServer exception " + e.printStackTrace() );
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
					System.out.println("ACCEPTED");
					jobServer js = new jobServer(s, mFedCloudInfos, mDownSpeed);
					
					js.start();
					if ( s == null ) {
						i ++;
						continue;
					}
					//mAcceptFedSocket = new FedClientSocket(s); 
					//something incoming
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		} // while
		try {
			if ( mServer != null ) {
					mServer.close();
					System.out.println("Fed Cloud sub Server Closed");
			} else {
					System.out.println("mServer == null");
			}
		} catch ( Exception e ) {
				e.printStackTrace();
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

	public void setFedCloudInfos(Map<String,FedCloudInfo> mFedCloudInfos) {
		this.mFedCloudInfos = mFedCloudInfos;
	}
	public Map<String,Double> getDownSpeed() {
		return mDownSpeed;
	}

	public void setDownSpeed(Map<String,Double> mDownSpeed) {
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
		private Map<String,Double> nDownSpeed = new HashMap<String, Double>();

		
		public jobServer(Socket s, Map<String, FedCloudInfo> nFedCloudInfos2, Map<String,Double> nDownSpeed2){
			super("jobServer");
			this.nSocket = s;
			this.nFedCloudInfos = nFedCloudInfos2;
			this.nDownSpeed = nDownSpeed2;
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
									System.out.println("SERVER GET:"+line);
								}
								if ( line.contains( FedCloudProtocol.REQ_BYE ) ) {
									clientOutput.println( FedCloudProtocol.RES_BYE );
									clientOutput.flush();
									System.out.println("Region Cloud Recvied Bye");
									nRunFlag = false;
									break;
								}
								if ( line.contains( FedCloudProtocol.REQ_REGION_MAP_FINISHED ) ) {
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
								}
								if ( line.contains( FedCloudProtocol.REQ_WAN_SPEED ) ) {
									String namdenode = line.substring(14);
									
									System.out.println("-----------------------------");
									System.out.println("get WAN speed"+nSocket.getInetAddress().toString());
									System.out.println("-----------------------------");
									String res = "";
									for (Map.Entry<String, Double> entry : nDownSpeed.entrySet())
									{
										res += entry.getKey()+"="+entry.getValue()+",";
									}
								/*	for (Map.Entry<String, FedCloudInfo> entry : nFedCloudInfos.entrySet())
									{
										for (Map.Entry<String, Double> entry1 : entry.getValue().getWanSpeedMap().entrySet())
										{
											res += entry.getValue().getCloudName()+">"+entry1.getKey() +"="+ Double.toString(entry1.getValue())+",";
										}									
									}
									*/
									System.out.println(res);
									clientOutput.println( FedCloudProtocol.RES_WAN_SPEED+" "+res );
									clientOutput.flush();
									
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
	

