package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
		
		Thread newThread1 = new Thread(new ReadUserDefinedLT(GN1));
		newThread1.start();
		
		Thread newThread2 = new Thread(new GlobalNamespaceRunnable(GN1));
		//newThread2.setDaemon(true);
		newThread2.start();

	}
}

class ReadUserDefinedLT implements Runnable {
	
	static GlobalNamespace GN;
	
	public ReadUserDefinedLT(GlobalNamespace GN) {
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
 
        System.out.println("SuperNamenode starting!");
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

class GlobalNamespaceRunnable implements Runnable {
	
	static GlobalNamespace GN;
	
	public GlobalNamespaceRunnable(GlobalNamespace GN) {
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