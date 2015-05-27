package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.BufferedOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import ncku.hpds.hadoop.fedhdfs.SuperNamenodeInfo;


public class SecureUnion {
	
	private String SNaddress = SuperNamenodeInfo.getSuperNamenodeAddress();
    private int SNport = SuperNamenodeInfo.getFedUserConstructGNPort();
    
	public void union(String command, String globalFileName, String Path) {
		
		String[] split = Path.split(":");
		String hostName = split[0];
		String clusterpath = split[1];
		
		Socket client = new Socket();
        InetSocketAddress isa = new InetSocketAddress(this.SNaddress, this.SNport);
        try {
            client.connect(isa, 10000);
            BufferedOutputStream out = new BufferedOutputStream(client
                    .getOutputStream());
       
	    String message = command + " " + globalFileName + " " + hostName + " " + clusterpath;
            out.write(message.getBytes());
            out.flush();
            out.close();
            out = null;
            client.close();
            client = null;
 
        } catch (java.io.IOException e) {
            System.out.println("Socket connect error");
            System.out.println("IOException :" + e.toString());
        }
	    
		 System.out.println("globalFileName : " + globalFileName);
	     System.out.println("host and path : " + Path );
	}
}
