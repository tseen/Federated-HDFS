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

	
	public void run() {
		Socket socket = null;
		String host = "10.3.1.2";

		try {
			socket = new Socket(host, 4444);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		byte[] bytes = new byte[20 * 1024 * 1024];
		Arrays.fill( bytes, (byte) 1 );
		InputStream in = new ByteArrayInputStream(bytes);
		OutputStream out;
		try {
			mOutput = new PrintWriter(socket.getOutputStream());
			mOutput.println("STOP "+namenode);
            mOutput.flush();
			out = socket.getOutputStream();

			int count;
			while ((count = in.read(bytes)) > 0) {
				out.write(bytes, 0, count);
			}
			
			
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
}
