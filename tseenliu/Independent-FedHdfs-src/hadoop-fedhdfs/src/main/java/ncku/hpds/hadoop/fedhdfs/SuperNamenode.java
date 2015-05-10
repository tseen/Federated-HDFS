package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Element;

public class SuperNamenode {
	
	static GlobalNamespace GN = new GlobalNamespace();

	public static void main(String[] args) throws Exception {
		
		Thread GNLD = new Thread(new GlobalNamespaceLD(GN));
		GNLD.start();
		
		Thread GNPD = new Thread(new GlobalNamespacePD(GN));
		//newThread2.setDaemon(true);
		GNPD.start();
		
		Thread GNSerialize = new Thread(new GlobalNamespaceServer(GN));
		GNSerialize.start();
		
		Thread test = new Thread(new GNQueryServer(GN));
		test.start();

	}
}

/* GET FedHDFS client message to construct GlobalNamespace */
class GlobalNamespaceLD implements Runnable {
	
	static GlobalNamespace GN;
	
	public GlobalNamespaceLD(GlobalNamespace GN) {
		this.GN = GN;
	}
	
	private boolean OutServer = false;
    private ServerSocket server;
    private final int ServerPort = 8765;
   
    public void run() {
    	
    	Socket socket;
        BufferedInputStream stringIn;
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
 
                stringIn = new BufferedInputStream(socket.getInputStream());
                byte[] buffstr = new byte[1024];
                String message = "";
                int length;
                while ((length = stringIn.read(buffstr)) > 0)
                {
                	message += new String(buffstr, 0, length);
                }
 
                System.out.println("FedUser input : " + message + "\n");
                stringIn.close();
                stringIn = null;
                socket.close();
                
                String[] split = message.split(" ");
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
                
                else if (command.equalsIgnoreCase("-rm")) {
                	if (split.length == 3) {
                		String globalFileName = split[1];
                        String hostName = split[2];
                        GN.rm(globalFileName, hostName);
                	}
                	if (split.length == 2) {
                		String globalFileName = split[1];
                        GN.rmdir(globalFileName);
                	}
                }
            } catch (java.io.IOException e) {
                System.out.println("Socket connect error");
                System.out.println("IOException :" + e.toString());
            }
        }
    }
}

/* GET information of each Namenodes' namespace */
class GlobalNamespacePD implements Runnable {
	
	static GlobalNamespace GN;
	
	public GlobalNamespacePD(GlobalNamespace GN) {
		this.GN = GN;
	}
	
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

/* GlobalNamespace Server running for FedHDFS client */
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
		
		GlobalNamespaceObject GNSerialize = new GlobalNamespaceObject();
		GNSerialize.setGlobalNamespace(GN);
		
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
				ObjectOut.writeObject(GNSerialize);
				ObjectOut.flush();
				ObjectOut.close();
				ObjectOut = null;
				socket.close();
				socket = null;
				
			} catch (IOException e) {
				System.out.println("Socket connect error");
				System.out.println("IOException :" + e.toString());
			}
		}
	}
}

class GNQueryServer extends Thread {
	
	static GlobalNamespace GN;
	
	public GNQueryServer(GlobalNamespace GN) {
		this.GN = GN;
	}
	
	private boolean OutServer = false;
	private ServerSocket server;
	private final int ServerPort = 8763;
	
	@Override
    public void run() {
    	
    	Socket socket;
    	InputStream stringIn;
        try {
			server = new ServerSocket(ServerPort);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
 
        System.out.println("GlobalNamespace query Server starting");
        while (!OutServer) {
            socket = null;
            try {
                synchronized (server) {
                    socket = server.accept();
                }
                socket.setSoTimeout(15000);
 
                byte buffstr[] = new byte[1024];
                stringIn = socket.getInputStream();
    			int str = stringIn.read(buffstr);
    			String globalFile = new String(buffstr, 0, str);
    			//System.out.println(globalFile);
    			System.out.println("FedHDDS client query GlobalFile : " + globalFile + "\n");
    			
    			ObjectOutputStream objectOut = new ObjectOutputStream(socket.getOutputStream());
                //BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
    			//out.write(requestGlobalFile.toString().getBytes());
    			ArrayList<String> requestGlobalFile = GN.queryGlobalFile(globalFile);
    			objectOut.writeObject(requestGlobalFile);
    			stringIn.close();
    			stringIn = null;
                objectOut.close();
                objectOut.flush();
                socket.close();
                
            } catch (java.io.IOException e) {
                System.out.println("Socket connect error");
                System.out.println("IOException :" + e.toString());
            }
        }
    }
}