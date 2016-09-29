/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

public class FedWANClient extends Thread {
	private PrintWriter mOutput = null;
	private String namenode = "";
    private String ip ="";
    private int mPort = 8799;
    private boolean mRunFlag = true;
    private Object mLock = new Object();

   public void setPort(int port ) {
	mPort = port;
   }
	
	public void run() {
		Socket socket = null;
		String host = ip;
		synchronized(mLock) {
            do {
				try {
          //int port = ip.hashCode()%10000;
          //port = Math.abs(port);
          int port = mPort;
					//socket = new Socket(host, ip.hashCode()%10000);
          System.out.println("wanClient port = " + port );
					socket = new Socket(host, port);
					if(socket.isConnected())
						mRunFlag = false;
				} catch (UnknownHostException e) {
					mRunFlag = true;
                    try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}

				} catch (IOException e) {
					mRunFlag = true;
                    try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}

				}
            }while(mRunFlag == true );
		}

		
		byte[] bytes = new byte[6 * 1024 * 1024];
		//Arrays.fill( bytes, (byte) 1 );
		//InputStream in = new ByteArrayInputStream(bytes);
		InputStream in = null;
		OutputStream out = null;
		try {
			out = socket.getOutputStream();
			in = socket.getInputStream();
			mOutput = new PrintWriter(out);
			mOutput.println("STOP "+namenode);
            mOutput.flush();
			
			
			long total = 0;
			long startTime = System.currentTimeMillis();
			//float speed = 0;
			
			int count = 0;
			while ((count = in.read(bytes)) > 0) {
				total += count;
				if(total >5210000)
					break;
			}
			long endTime = System.currentTimeMillis();
			double speed = (double)total/(double)(endTime-startTime)/1000d;
			System.out.println(ip +"->"+ namenode +": read "+total+" bytes, speed:"+speed+" MB");

	//		int count;
	//		while ((count = in.read(bytes)) > 0) {
	//			out.write(bytes, 0, count);
	//		}
			mOutput.println("SPEED "+speed);
            mOutput.flush();


			
            mOutput.close();
			out.close();
			in.close();
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getHost() {
		return namenode;
	}
	public void setHost(String namenode) {
		this.namenode = namenode;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
}
