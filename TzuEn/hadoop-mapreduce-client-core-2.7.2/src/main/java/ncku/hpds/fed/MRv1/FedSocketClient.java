package ncku.hpds.fed.MRv1;

import java.net.*;
import java.io.*;

class FedSocketClient extends Thread{

	private final static String SIGNAL_SUCCESS = "SIGNAL_SUCCESS";
        private final static String SIGNAL_NEXT = "SIGNAL_NEXT";
        private final static String SIGNAL_TERMINATE = "SIGNAL_TERMINATE";
        private final static String SIGNAL_CONNECT = "SIGNAL_CONNECT";
	

	Socket server = null;

	String serverIP = null;
	String sendSignal = null;
	int serverPort;

	FedSocketClient(String ip, int port, String signal){
		this.serverIP = ip;
		this.serverPort = port;
		this.sendSignal = signal;
	}
	public void run() {
		String line = null;
		while(true){
			try{
				this.server=new Socket (serverIP,serverPort);
				if(server != null){
					System.out.println("Connecting to server");
					break;
				}
			}catch(IOException e){
				//System.out.println("Waiting for SocketServer boot");
				try{
					Thread.sleep(1000);
				}catch(InterruptedException ie){
					ie.printStackTrace();
				}		
			}
		}
		try{
			BufferedReader in=new BufferedReader(new InputStreamReader(server.getInputStream()));
			PrintWriter out=new PrintWriter(server.getOutputStream());

			while(true){
				out.println(sendSignal);  //NEXT, TERMINATE, CONNECT
				out.flush();
				if(sendSignal.equals(SIGNAL_TERMINATE)){
					server.close();
					break;
				}
				line = in.readLine();
				System.out.println("Receive Message: " + line);
				if(line.equals(SIGNAL_SUCCESS)){
					System.out.println("1st-phase MR completed in region cloud");
					System.out.println("start 2nd-phase MR");
					break;
				}
			}
		}catch(IOException e){
			e.printStackTrace();	
		}
	}
}


