package ncku.hpds.hadoop.fedhdfs;


public class SuperNamenodeInfo {

	private String Default_SuperNamenodeAddress = "127.0.0.1";
	private int Default_FedUserConstructGNPort = 8765;
	private int Default_GlobalNamespaceServerPort = 8764;
	private int Default_GNQueryServerPort = 8763;

	public void setFedUserConstructGNPort(int FedUserConstructGNPort) { this.Default_FedUserConstructGNPort = FedUserConstructGNPort; }
	public void setGlobalNamespaceServerPort(int GlobalNamespaceServerPort) { this.Default_GlobalNamespaceServerPort = GlobalNamespaceServerPort; }
	public void setGNQueryServerPort(int GNQueryServerPort) { this.Default_GNQueryServerPort = GNQueryServerPort; }

	public int getFedUserConstructGNPort() { return Default_FedUserConstructGNPort; }
	public int getGlobalNamespaceServerPort() { return Default_GlobalNamespaceServerPort; }	
	public int getGNQueryServerPort() { return Default_GNQueryServerPort; }

}
