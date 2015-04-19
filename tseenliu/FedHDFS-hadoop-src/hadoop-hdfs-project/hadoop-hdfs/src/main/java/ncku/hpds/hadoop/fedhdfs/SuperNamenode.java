package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Element;

public class SuperNamenode {
	
	static GlobalNamespace GN1 = new GlobalNamespace();

	public static void main(String[] args) throws Exception {
		
		Thread newThread1 = new Thread(new GlobalNamespaceLD(GN1));
		newThread1.start();
		
		Thread newThread2 = new Thread(new GlobalNamespacePD(GN1));
		//newThread2.setDaemon(true);
		newThread2.start();
		
		Thread newThread3 = new Thread(new GlobalNamespaceServer(GN1));
		newThread3.start();

	}
}

class GlobalNamespaceLD implements Runnable {
	
	static GlobalNamespace GN;
	
	public GlobalNamespaceLD(GlobalNamespace GN) {
		this.GN = GN;
	}
	
	private boolean OutServer = false;
    private ServerSocket server;
    private final int ServerPort = 8765;
   
    @Override
    public void run() {
    	
    	Socket socket;
        java.io.BufferedInputStream sin;
        try {
			server = new ServerSocket(ServerPort);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
 
        System.out.println("SuperNamenode starting");
        while (!OutServer) {
            socket = null;
            try {
                synchronized (server) {
                    socket = server.accept();
                }
                socket.setSoTimeout(15000);
 
                sin = new java.io.BufferedInputStream(socket.getInputStream());
                byte[] b = new byte[1024];
                String data = "";
                int length;
                while ((length = sin.read(b)) > 0) // <=0的話就是結束了
                {
                    data += new String(b, 0, length);
                }
 
                System.out.println("FedUser input : " + data + "\n");
                sin.close();
                sin = null;
                socket.close();
                
                String[] split = data.split(" ");
                String command = split[0];
                
                if (command.equalsIgnoreCase("-mkdir")){
                    String globalFileName = split[1];
                    GN.mkdir(globalFileName);
                }
                
                else if (command.equalsIgnoreCase("-sunion") | command.equalsIgnoreCase("-sun")) {
                	String globalFileName = split[1];
                    String hostName = split[2];
                    String clusterPath = split[3];
                    GN.sput(globalFileName, hostName, clusterPath);
                }
                
                else if (command.equalsIgnoreCase("-union") | command.equalsIgnoreCase("-un")) {
                	String globalFileName = split[1];
                    String hostName = split[2];
                    String clusterPath = split[3];
                    GN.put(globalFileName, hostName, clusterPath);
                }
                
                else if (command.equalsIgnoreCase("-rmdir")) {
                	String globalFileName = split[1];
                    GN.rmdir(globalFileName);
                }
                
                else if (command.equalsIgnoreCase("-rm")) {
                	String globalFileName = split[1];
                    String hostName = split[2];
                    GN.rm(globalFileName, hostName);
                }
  
            } catch (java.io.IOException e) {
                System.out.println("Socket connect error");
                System.out.println("IOException :" + e.toString());
            }
        }
    }
}

class GlobalNamespacePD implements Runnable {
	
	static GlobalNamespace GN;
	
	public GlobalNamespacePD(GlobalNamespace GN) {
		this.GN = GN;
	}
	
	@Override
	public void run() {
		try{
			while(true){
				Thread.sleep(60000);
				//GlobalNamespace GN1 = new GlobalNamespace();
				GN.setFedConf();
				GN.DynamicConstructPD();
				System.out.println("!!!! PHashTable download sucessful !!!!");
			}
			
		}catch(InterruptedException e){	
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class GlobalNamespaceServer extends Thread {
	
	static GlobalNamespace GN;
	
	public GlobalNamespaceServer(GlobalNamespace GN) {
		this.GN = GN;
	}
	
	private boolean OutServer = false;
	private ServerSocket server;
	private final int ServerPort = 8764;
	
	public void run() {
		Socket socket;
		ObjectOutputStream ObjectOut;
		
		GlobalNamespaceObject test = new GlobalNamespaceObject();
		test.setGlobalNamespace(GN);
		
		try {
			server = new ServerSocket(ServerPort);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("GlobalNamespace Sever is running");
		while (!OutServer) {
			socket = null;
			try {
				synchronized (server) {
					socket = server.accept();
				}
				socket.setSoTimeout(15000);

				ObjectOut = new ObjectOutputStream(socket.getOutputStream());
				ObjectOut.writeObject(test);
				ObjectOut.flush();
				ObjectOut.close();
				ObjectOut = null;
				//test = null;
				socket.close();
				socket = null;
				
			} catch (java.io.IOException e) {
				System.out.println("Socket connect error");
				System.out.println("IOException :" + e.toString());
			}
		}
	}
}