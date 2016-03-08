/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.BufferedReader;
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
			try {
				BufferedReader clientInput = null;
				PrintWriter clientOutput = null;
				String line = null;
				synchronized ( mLock ) { 
					mState = FedCloudProtocol.FedSocketState.ACCEPTING;
				}
				if ( i > 12 ) {
					System.out.println("FedJobManager no incoming connection, break out");
					break;
				}
				Socket s = mServer.accept();
				if ( s == null ) {
					i ++;
					continue;
				}
				mAcceptFedSocket = new FedClientSocket(s); 
				//something incoming
				clientInput = mAcceptFedSocket.getInputReader();
				clientOutput = mAcceptFedSocket.getOutputWriter();
				synchronized ( mLock ) { 
					mState = FedCloudProtocol.FedSocketState.ACCEPTED;
				}
				while ( mRunFlag ) {
					synchronized ( mLock ) { 
						try {
							line = clientInput.readLine(); 
							//System.out.println("Recv : " + line );
							if ( line.contains( FedCloudProtocol.REQ_PING ) ) {
								//System.out.println("PINGGGG");
								clientOutput.println( FedCloudProtocol.RES_PONG );
								clientOutput.flush();
							}
							if ( line.contains( FedCloudProtocol.REQ_BYE ) ) {
								clientOutput.println( FedCloudProtocol.RES_BYE );
								clientOutput.flush();
								System.out.println("Region Cloud Recvied Bye");
								mRunFlag = false;
								break;
							}
							if ( line.contains( FedCloudProtocol.REQ_REGION_MAP_FINISHED ) ) {
								String namdenode = line.substring(20);
								clientOutput.println( FedCloudProtocol.RES_REGION_MAP_FINISHED );
								clientOutput.flush();
								System.out.println("-----------------------------");
								System.out.println("get region map stop time"+s.getInetAddress().toString());
								System.out.println("-----------------------------");
								System.out.println(namdenode);
								System.out.println(mFedCloudInfos.toString());
								mFedCloudInfos.get(namdenode).setRegionMapTime((int) System.currentTimeMillis());
								//TODO set region map finished time;
								//mRunFlag = false;
								break;
							}
						} catch (Exception e) {
							//e.printStackTrace();
							//System.out.println("sockettimeout exeception occurs");
						}
					}
					Thread.sleep(500); // 500ms 
				}
			} catch ( Exception e ) { 
				i++;
				e.printStackTrace();
				//System.out.println("accept error continue wait");
			} finally {
				synchronized ( mLock ) { 
					if ( mAcceptFedSocket != null ) {
						mAcceptFedSocket.close();
						mAcceptFedSocket = null;
					}
					mState = FedCloudProtocol.FedSocketState.DISCONNECTED;
				}
			}
		} // while
		try {
			if ( mServer != null ) {
				mServer.close();
				System.out.println("Fed Cloud Server Closed");
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
}
