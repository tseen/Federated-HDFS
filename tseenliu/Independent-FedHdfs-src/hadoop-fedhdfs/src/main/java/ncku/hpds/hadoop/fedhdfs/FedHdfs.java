package ncku.hpds.hadoop.fedhdfs;

import java.util.Vector;

import ncku.hpds.hadoop.fedhdfs.shell.ConstructGN;
import ncku.hpds.hadoop.fedhdfs.shell.Delete;
import ncku.hpds.hadoop.fedhdfs.shell.DeleteDir;
import ncku.hpds.hadoop.fedhdfs.shell.FetchFsimage;
import ncku.hpds.hadoop.fedhdfs.shell.Ls;
import ncku.hpds.hadoop.fedhdfs.shell.LsGlobalNamespace;
import ncku.hpds.hadoop.fedhdfs.shell.Lsr;
import ncku.hpds.hadoop.fedhdfs.shell.Mkdir;
import ncku.hpds.hadoop.fedhdfs.shell.SecureUnion;
import ncku.hpds.hadoop.fedhdfs.shell.Union;

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
					"fs.defaultFS",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theElements.elementAt(i)));
		}
		
		if (command.equalsIgnoreCase("-ls")){
			
			if(uri.length == 1) {
				LsGlobalNamespace test = new LsGlobalNamespace();
			}
			if(uri.length == 2) {
				if (uri[1].contains(":")){
					if (uri.length < 2) {
						System.err.println("Usage: -ls [hostName]:[<path> ...]");
						System.err.println("Usage: -ls [globalFilePath]");
						System.exit(2);
					}
					String[] split = uri[1].split(":");
					String hostName = split[0];
					String Uri = split[1];
					for (int i = 0; i < theElements.size(); i++) {
		    			if (hostName.equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
		    				//Ls.print_info(uri[2], conf[i], FedHdfsConParser.getValue("HostName", theElements.elementAt(i)));
		    				Ls.printFiles(Uri, conf[i], hostName);
		    			}
		    		}	
				}
				else {LsGlobalNamespace test = new LsGlobalNamespace(uri[1]);}
			}
			
		} else if (command.equalsIgnoreCase("-lsr")) {
			
			if (uri[1].contains(":")) {
				if (uri.length < 2) {
					System.err.println("Usage: -ls [hostName]:[<path> ...]");
					System.err.println("Usage: -ls [globalFilePath]");
					System.exit(2);
				}
				String[] split = uri[1].split(":");
				String hostName = split[0];
				String Uri = split[1];
				for (int i = 0; i < theElements.size(); i++) {
	    			if (hostName.equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
	    				Lsr.printFilesRecursively(Uri, conf[i]);
	    			}
	    		}
			}
			else {
				if (uri.length < 2) {
					System.err.println("Usage: -ls [hostName]:[<path> ...]");
					System.err.println("Usage: -ls [globalFilePath]");
					System.exit(2);
				}
				boolean recursive = true;
				LsGlobalNamespace test = new LsGlobalNamespace(uri[1], recursive);
			}
			
		} else if (command.equalsIgnoreCase("-mkdir")){
			
			if (uri.length < 2) {
				System.err.println("Usage: -mkdir [globalfile]");
				System.exit(2);
			}
			
			Mkdir mkdirGN = new Mkdir();
			mkdirGN.constructGlobalFile(command, uri[1]);	
			
		}	else if (command.equalsIgnoreCase("-mv")){
			
			if (uri.length < 4) {
				System.err.println("Usage: -mv [globalfile] [hostName] [hostName:<path> ...]");
				System.exit(2);
			}
			Delete rm = new Delete("-rm", uri[1], uri[2]);
			Union unionGlobalFile = new Union();
			unionGlobalFile.union("-un", uri[1], uri[3]);
			
		} else if (command.equalsIgnoreCase("-rm")){

			if (uri.length < 2) {
				System.err.println("Usage: -rm [globalfile]");
				System.err.println("Usage: -rm [globalfile] [hostName]");
				System.exit(2);
			}
			
			if(uri.length == 3) {
				Delete rm = new Delete(command, uri[1], uri[2]);
			}
			
			if(uri.length == 2) {
				Delete rm = new Delete(command, uri[1]);
			}
			
		} else if (command.equalsIgnoreCase("-union") | command.equalsIgnoreCase("-un")) {

			if (uri.length < 3) {
				System.err.println("Usage: -union [globalfile] [hostName:<path> ...]");
				System.exit(2);
			}

			Union unionGlobalFile = new Union();
			unionGlobalFile.union(command, uri[1], uri[2]);
			
		} else if (command.equalsIgnoreCase("-sunion") | command.equalsIgnoreCase("-sun")) {

			if (uri.length < 3) {
				System.err.println("Usage: -sunion [globalfile] [hostName:<path> ...]");
				System.exit(2);
			}

			SecureUnion sunionGlobalFile = new SecureUnion();
			sunionGlobalFile.union(command, uri[1], uri[2]);
			
		} /*else if (command.equalsIgnoreCase("-lsP")) {
			
			if(uri.length == 1) {
				LsGlobalNamespace test = new LsGlobalNamespace();
			}
			if(uri.length == 2) {
				LsGlobalNamespace test = new LsGlobalNamespace(uri[1]);
			}
			//test.GlobalNamespaceClient(uri[1]);
			
		}	else if (command.equalsIgnoreCase("-lspr")) {
			boolean tag = true;
			LsGlobalNamespace test = new LsGlobalNamespace(uri[1], tag);
			
			
		}*/ else if (command.equalsIgnoreCase("-fetchFedImage")){
			
			FetchFsimage.initialize();
			
    		for (int i = 0; i < theElements.size(); i++) {
    			FetchFsimage.downloadFedHdfsFsImage(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)), FedHdfsConParser.getValue("dfs.namenode.http-address", theElements.elementAt(i)));
    			//FetchFsimage.offlineImageViewer(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)));
    		}
    		
		} else {

			System.out.println("The general command line syntax is");
			System.out.println("bin/fedhdfs command [genericOptions] [commandOptions]");
		}
	}
}