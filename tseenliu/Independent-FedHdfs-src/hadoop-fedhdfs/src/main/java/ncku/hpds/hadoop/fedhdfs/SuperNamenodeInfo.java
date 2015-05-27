package ncku.hpds.hadoop.fedhdfs;


public class SuperNamenodeInfo {

	public static String Default_SuperNamenodeAddress = "127.0.0.1";
	public static String Default_FedUserConstructGNPort = "8765";
	public static String Default_GlobalNamespaceServerPort = "8764";
	public static String Default_GNQueryServerPort = "8763";

	public void setSuperNamenodeAddress(String SuperNamenodeAddress) { this.Default_SuperNamenodeAddress = SuperNamenodeAddress; }
	public void setFedUserConstructGNPort(String FedUserConstructGNPort) { this.Default_FedUserConstructGNPort = FedUserConstructGNPort; }
	public void setGlobalNamespaceServerPort(String GlobalNamespaceServerPort) { this.Default_GlobalNamespaceServerPort = GlobalNamespaceServerPort; }
	public void setGNQueryServerPort(String GNQueryServerPort) { this.Default_GNQueryServerPort = GNQueryServerPort; }
	
	
	public static String getSuperNamenodeAddress() { return Default_SuperNamenodeAddress; }
	
	public static int getFedUserConstructGNPort() {
		return Integer.parseInt(Default_FedUserConstructGNPort);
	}
	
	public static int getGlobalNamespaceServerPort() { 
		return Integer.parseInt(Default_GlobalNamespaceServerPort);
	}
	
	public static int getGNQueryServerPort() {
		return Integer.parseInt(Default_GNQueryServerPort);
	}
	
	
	
	public static void show() {
		System.out.println(Default_SuperNamenodeAddress);
		System.out.println(Default_FedUserConstructGNPort);
		System.out.println(Default_GlobalNamespaceServerPort);
		System.out.println(Default_GNQueryServerPort);
	}

}
