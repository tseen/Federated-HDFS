package ncku.hpds.hadoop.fedhdfs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

public class GlobalNamespaceObject implements Serializable {
	
	private GlobalNamespace GN = new GlobalNamespace();
	
	public GlobalNamespaceObject() {
		
	}
	
	public void setGlobalNamespace(GlobalNamespace GN) {
		this.GN = GN;
	}
	
	public GlobalNamespace getGlobalNamespace() {
		return GN;
	}
	
	public Set<Entry<String, HashMap<String, String>>> showLogicalMapping() {
		return GN.getLogicalDrive().getLogicalMappingTable().entrySet();
	}
	
	public Set<Entry<String, String>> showPhysicalMapping() {
		return GN.getPhysicalDrive().getPhysicalMappingTable().entrySet();
	}
	
	public HashMap<String, HashMap<String, String>> getLogicalMappingTable() {
		return GN.getLogicalDrive().getLogicalMappingTable();
	}
	
	public HashMap<String, String> getPhysicalMappingTable() {
		return GN.getPhysicalDrive().getPhysicalMappingTable();
	}

}
