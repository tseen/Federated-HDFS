package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ncku.hpds.hadoop.fedhdfs.GlobalNamespaceObject;

public class LsTableInfo {

	private String address = "127.0.0.1";
	private int port = 8764;
	
	public void GlobalNamespaceClient() {
		Socket client = new Socket();
		ObjectInputStream ObjectIn;
		InetSocketAddress isa = new InetSocketAddress(this.address, this.port);
		try {
			client.connect(isa, 10000);
			ObjectIn = new ObjectInputStream(client.getInputStream());
			
			//received object
			GlobalNamespaceObject GN = new GlobalNamespaceObject();
			try {
				GN = (GlobalNamespaceObject) ObjectIn.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("fedUserGet1 : " + GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalHashTable());
			System.out.println("fedUserGet2 : " + GN.getGlobalNamespace().getPhysicalDrive().getPhysicalMappingTable().size());
			System.out.println("fedUserGet3 : " + GN.showLogicalMapping());
			
			/*System.out.println(GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get("FedRDD").get("c34").get(1)
					+ String.format("%4d", GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get("FedRDD").get("c34").get(2))
					+ " "
					+ GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get("FedRDD").get("c34").get(3)
					+ " "
					+ GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get("FedRDD").get("c34").get(4)
					+ String.format("%11d", GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get("FedRDD").get("c34").get(5))
					+ " "
					+ GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get("FedRDD").get("c34").get(6)
					+ " " + GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get("FedRDD").get("c34").get(0));
			
			int VirtualSize = GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().size();
			Set<String> keySet = GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().keySet();
			Iterator<String> it = keySet.iterator();
			while(it.hasNext()){
				String key = it.next();
				LsTableInfo.showInfo(GN, key);
			}
			for (int i = 0; i < VirtualSize ; i++) {
				GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable();
			}*/
			
			ObjectIn.close();
			ObjectIn = null;
			client.close();
			
		} catch (java.io.IOException e) {
			System.out.println("Socket connection error!");
			System.out.println("IOException :" + e.toString());
		}
	}
	
	/*public static void showInfo(GlobalNamespaceObject GN, String key) {
		
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		System.out.println(GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get(key).get("c34").get(1)
				+ String.format("%4d", GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get(key).get("c34").get(2))
				+ " "
				+ GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get(key).get("c34").get(3)
				+ " "
				+ GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get(key).get("c34").get(4)
				+ String.format("%11d", GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get(key).get("c34").get(5))
				+ " "
				+ GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get(key).get("c34").get(6)
				+ " " + GN.getGlobalNamespace().getLogicalDrive().TestgetLogicalMappingTable().get(key).get("c34").get(0));
	}*/
	
}
