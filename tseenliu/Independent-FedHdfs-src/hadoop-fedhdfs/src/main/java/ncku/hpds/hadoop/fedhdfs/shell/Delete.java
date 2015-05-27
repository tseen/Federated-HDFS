package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.BufferedOutputStream;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import ncku.hpds.hadoop.fedhdfs.GlobalNamespaceObject;
import ncku.hpds.hadoop.fedhdfs.SuperNamenodeInfo;

public class Delete {

	private String SNaddress = SuperNamenodeInfo.getSuperNamenodeAddress();
    private int SNport = SuperNamenodeInfo.getFedUserConstructGNPort();
    private int port = SuperNamenodeInfo.getGlobalNamespaceServerPort();
	
	public Delete(String command, String globalFileName, String hostName) {
		rmPathFromGN(command, globalFileName, hostName);
	}
	
	public Delete(String command, String globalFileName) {
		rmGlobalFileFromGN(command, globalFileName);
	}

	public void rmPathFromGN(String command, String globalFileName, String hostName) {

		Socket client = new Socket();
		ObjectInputStream ObjectIn;
		InetSocketAddress isa = new InetSocketAddress(this.SNaddress, this.port);
		try {
			client.connect(isa, 10000);
			ObjectIn = new ObjectInputStream(client.getInputStream());

			// received object
			GlobalNamespaceObject GN = new GlobalNamespaceObject();
			try {
				GN = (GlobalNamespaceObject) ObjectIn.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// doing...
			if (GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().containsKey(globalFileName)) {
				if (GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalFileName).containsKey(hostName)) {

					Socket SNclient = new Socket();
					InetSocketAddress SNisa = new InetSocketAddress(this.SNaddress, this.SNport);
					try {
						SNclient.connect(SNisa, 10000);
						BufferedOutputStream out = new BufferedOutputStream(SNclient.getOutputStream());

						String message = command + " " + globalFileName + " " + hostName;
						out.write(message.getBytes());
						out.flush();
						out.close();
						out = null;
						SNclient.close();
						SNclient = null;

					} catch (java.io.IOException e) {
						System.out.println("Socket connect error");
						System.out.println("IOException :" + e.toString());
					}
				} else {
					System.out.println("Error: " + " cluser:" + hostName + " not found ");
				}
			} else {
				System.out.println("Error: " + globalFileName + " not found ");
			}

			ObjectIn.close();
			ObjectIn = null;
			client.close();

		} catch (java.io.IOException e) {
			System.out.println("Socket connection error");
			System.out.println("IOException :" + e.toString());
		}

		System.out.println("globalFileName : " + globalFileName);
		System.out.println("host : " + hostName);
	}
	
	public void rmGlobalFileFromGN(String command, String globalFileName) {
		
		Socket client = new Socket();
		ObjectInputStream ObjectIn;
		InetSocketAddress isa = new InetSocketAddress(this.SNaddress, this.port);
		try {
			client.connect(isa, 10000);
			ObjectIn = new ObjectInputStream(client.getInputStream());

			// received object
			GlobalNamespaceObject GN = new GlobalNamespaceObject();
			try {
				GN = (GlobalNamespaceObject) ObjectIn.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			//doing...
			if (GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().containsKey(globalFileName)) {
				
				Socket SNclient = new Socket();
	            InetSocketAddress SNisa = new InetSocketAddress(this.SNaddress, this.SNport);
	            try {
	            	SNclient.connect(SNisa, 10000);
	                BufferedOutputStream out = new BufferedOutputStream(SNclient
	                        .getOutputStream());
	            // send message
	    	    String message = command + " " + globalFileName;
	                out.write(message.getBytes());
	                out.flush();
	                out.close();
	                out = null;
	                SNclient.close();
	                SNclient = null;
	     
	            } catch (java.io.IOException e) {
	                System.out.println("Socket connect error");
	                System.out.println("IOException :" + e.toString());
	            }	        	
	        }
	        else {
	        	System.out.println("Error: " + globalFileName + " not found ");
	        }

			ObjectIn.close();
			ObjectIn = null;
			client.close();

		} catch (java.io.IOException e) {
			System.out.println("Socket connection error");
			System.out.println("IOException :" + e.toString());
		}

		System.out.println("globalFileName : " + globalFileName);
	}
}