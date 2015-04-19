package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.BufferedOutputStream;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import ncku.hpds.hadoop.fedhdfs.GlobalNamespaceObject;

public class Union {
	
	private String SNaddress = "127.0.0.1";
    private int SNport = 8765;
    private int port = 8764;
    
	public void union(String command, String globalFileName, String Path) {

		String[] split = Path.split(":");
		String hostName = split[0];
		String clusterpath = split[1];

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
		
		
		if (GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().containsKey(globalFileName)) {
        	if (GN.getGlobalNamespace().getLogicalDrive().getLogicalMappingTable().get(globalFileName).containsKey(hostName)) {
        		System.out.println("Error: " +  " cluser:" + hostName + " already exists");
        	}
        	else {
        		Socket SNclient = new Socket();
                InetSocketAddress SNisa = new InetSocketAddress(this.SNaddress, this.SNport);
                try {
                	SNclient.connect(SNisa, 10000);
                    BufferedOutputStream out = new BufferedOutputStream(SNclient
                            .getOutputStream());
                // 送出字串
        	    String message = command + " " + globalFileName + " " + hostName + " " + clusterpath;
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
		System.out.println("host and path : " + Path);
	}

}
