package ncku.hpds.fed.MRv1;

import java.io.*;
import java.net.*;
class FedSocketServer extends Thread{
	private ServerSocket server;
	private Socket client;
	private InputStream in;
	private OutputStream out;
	private BufferedReader br;
	private PrintWriter pr;
	public boolean endJob = false;

	private final String SIGNAL_SUCCESS = "SIGNAL_SUCCESS";
	private final String SIGNAL_NEXT = "SIGNAL_NEXT";
	private final String SIGNAL_TERMINATE = "SIGNAL_TERMINATE";
	private final String SIGNAL_CONNECT = "SIGNAL_CONNECT";
	
	public void run(){
		try{
			server = new ServerSocket(5678);
			
			while(true){
				client = server.accept();
				socketClientSetup(client);
				String line = br.readLine();
				System.out.println("Message: " + line);
				if(line.equals(SIGNAL_CONNECT)){
					pr.println(SIGNAL_SUCCESS);
                                        pr.flush();
				}
				else if(line.equals(SIGNAL_NEXT)){	
					synchronized(this){
						notify();
					}
					synchronized(this){
						wait();
					}
					pr.println(SIGNAL_SUCCESS);
					pr.flush();
				}				
				else if(line.equals(SIGNAL_TERMINATE)){
					synchronized(this){
						endJob = true;
                                                notify();
                                        }
					break;
				}
			}
			server.close();
			
		}catch(IOException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		
	}
	void socketClientSetup(Socket s){
		try{		
			in = s.getInputStream();
			out = s.getOutputStream();
		}catch(IOException e){
			e.printStackTrace();
		}
		br = new BufferedReader(new InputStreamReader(in));
		pr = new PrintWriter(out);
	}
}
