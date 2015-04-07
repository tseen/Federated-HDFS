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
                System.out.println("InetAddress = " + socket.getInetAddress());
                // TimeOut時間
                socket.setSoTimeout(15000);
 
                sin = new java.io.BufferedInputStream(socket.getInputStream());
                byte[] b = new byte[1024];
                String data = "";
                int length;
                while ((length = sin.read(b)) > 0) // <=0的話就是結束了
                {
                    data += new String(b, 0, length);
                }
 
                System.out.println("UserDefined : " + data + "\n");
                sin.close();
                sin = null;
                socket.close();
                
                String[] split = data.split(" ");
                System.out.println("Logical File is : " + split[0]);
                String[] subSplit = split[1].split(":");
                System.out.println("HostName is : " + subSplit[0]);
                System.out.println("Physical Path is : " + subSplit[1]);
                
                GN.UserConstructLD(split[0], subSplit[0], subSplit[1]);
 
            } catch (java.io.IOException e) {
                System.out.println("Socket connection error!");
                System.out.println("IOException :" + e.toString());
            }
        }

    	/*GlobalNamespace GN1 = new GlobalNamespace();
        System.out.println("Please input logical mapping : ");
        System.out.println("(ex : logicalName --> hostName:Path)");
       
            final String line = in.nextLine();
            if ("end".equalsIgnoreCase(line)) {
                System.out.println("Ending one thread");
                break;
            }
            String[] split = line.split(" --> ");
            System.out.println("Logical File is : " + split[0]);
            String[] subSplit = split[1].split(":");
            System.out.println("HostName is : " + subSplit[0]);
            System.out.println("Physical Path is : " + subSplit[1]);
            
            try {
    			GN1.UserDefinedVD(split[0], subSplit[0], subSplit[1]);
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
            System.out.println("Please input logical mapping : ");
            System.out.println("(ex : logicalName --> hostName:Path)");*/
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
				System.out.println("Client/server Connetion : InetAddress = "
						+ socket.getInetAddress());
				socket.setSoTimeout(15000);
	
				System.out.println("test : " + test.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().entrySet());
				System.out.println("test : " + test.getGlobalNamespace().getPhysicalDrive().getPhysicalMappingTable().size());
				System.out.println("test : " + test.showLogicalMapping());
				
				
				ObjectOut = new ObjectOutputStream(socket.getOutputStream());
				ObjectOut.writeObject(test);
				ObjectOut.flush();
				ObjectOut.close();
				ObjectOut = null;
				//test = null;
				socket.close();
				socket = null;
				
			} catch (java.io.IOException e) {
				System.out.println("Socket連線有問題 !");
				System.out.println("IOException :" + e.toString());
			}
		}
		
	}
}