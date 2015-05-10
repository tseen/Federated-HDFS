package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.BufferedOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ConstructGN {
	
	private String SNaddress = "127.0.0.1";
    private int SNport = 8765;
    
	public void logicalMapping(String logicalName, String Path) {
		Socket client = new Socket();
        InetSocketAddress isa = new InetSocketAddress(this.SNaddress, this.SNport);
        try {
            client.connect(isa, 10000);
            BufferedOutputStream out = new BufferedOutputStream(client
                    .getOutputStream());

	    String test = logicalName + " " + Path;
            out.write(test.getBytes());
            out.flush();
            out.close();
            out = null;
            client.close();
            client = null;
 
        } catch (java.io.IOException e) {
            System.out.println("Socket connect error");
            System.out.println("IOException :" + e.toString());
        }
	    
		 System.out.println("\nlogicalName : " + logicalName);
	     System.out.println("host and path : " + Path + "\n");
	}
}
