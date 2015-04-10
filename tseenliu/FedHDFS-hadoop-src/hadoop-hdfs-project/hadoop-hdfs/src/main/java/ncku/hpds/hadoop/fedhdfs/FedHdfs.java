package ncku.hpds.hadoop.fedhdfs;

import java.util.Vector;

import ncku.hpds.hadoop.fedhdfs.shell.ConstructGN;
import ncku.hpds.hadoop.fedhdfs.shell.FetchFsimage;
import ncku.hpds.hadoop.fedhdfs.shell.Ls;
import ncku.hpds.hadoop.fedhdfs.shell.LsTableInfo;
import ncku.hpds.hadoop.fedhdfs.shell.Lsr;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Element;

public class FedHdfs {

	public static void main(String[] args) throws Exception {
		
		String[] uri = args;
		String command = uri[0];
		
		java.io.File path = new java.io.File("etc/hadoop/fedhadoop-clusters.xml");
		FedHdfsConParser hdfsIpList = new FedHdfsConParser(path);
		Vector<Element> theElements = hdfsIpList.getElements();
		
		Configuration[] conf = new Configuration[theElements.size()];
		
		for (int i = 0; i < theElements.size(); i++) {
			conf[i] = new Configuration();
			conf[i].set(
					"fs.default.name",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theElements.elementAt(i)));
		}
		
		if (command.equalsIgnoreCase("-ls")){
			
			if (uri.length < 3) {
				System.out.println("Usage: hadoop fedfs [generic options]");
				System.out.println("        [-ls [hostName] [<path> ...]]\n");
				
				System.out.println("\nThe general command line syntax is");
				System.out.println("bin/fedhdfs command [genericOptions] [commandOptions]\n");
			}
			for (int i = 0; i < theElements.size(); i++) {
    			if (uri[1].equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
    				Ls.print_info(uri[2], conf[i], FedHdfsConParser.getValue("HostName",
    						theElements.elementAt(i)));
    			}
    		}
		}
		else if (command.equalsIgnoreCase("-lsr")) {
			
			if (uri.length < 3) {
				System.out.println("Usage: hadoop fedfs [generic options]");
				System.out.println("        [-ls [hostName] [<path> ...]]\n");
				
				System.out.println("\nThe general command line syntax is");
				System.out.println("bin/fedhdfs command [genericOptions] [commandOptions]\n");
			}
			for (int i = 0; i < theElements.size(); i++) {
    			if (uri[1].equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
    				Lsr.printFilesRecursively(uri[2], conf[i]);	
    			}
    		}
		}
		else if (command.equalsIgnoreCase("-fetchFedImage")){
			FetchFsimage.initialize();
    		for (int i = 0; i < theElements.size(); i++) {
    			FetchFsimage.downloadFedHdfsFsImage(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)), FedHdfsConParser.getValue("dfs.namenode.http-address", theElements.elementAt(i)));
    			//FetchFsimage.offlineImageViewer(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)));
    		}
		}
		else if (command.equalsIgnoreCase("-gn")){
			
			if (uri.length < 3) {
				System.out.println("Usage: hadoop fedfs [generic options]");
				System.out.println("        [-gn <logicalName>  <hostName>:<path>]\n");
				
				System.out.println("\nThe general command line syntax is");
				System.out.println("bin/fedhdfs command [genericOptions] [commandOptions]\n");
			}
			
			ConstructGN test = new ConstructGN();
			test.logicalMapping(uri[1], uri[2]);	
		}
		else if (command.equalsIgnoreCase("-lstable")){
			
			if (uri.length < 3) {
				System.out.println("Usage: hadoop fedfs [generic options]");
				System.out.println("        [-gn <logicalName>  <hostName>:<path>]\n");
				
				System.out.println("\nThe general command line syntax is");
				System.out.println("bin/fedhdfs command [genericOptions] [commandOptions]\n");
			}
			
			LsTableInfo test = new LsTableInfo();
			test.GlobalNamespaceClient();
		}
		else {
		
			System.out.println("The general command line syntax is");
			System.out.println("bin/fedhdfs command [genericOptions] [commandOptions]\n");
		}
	}
}