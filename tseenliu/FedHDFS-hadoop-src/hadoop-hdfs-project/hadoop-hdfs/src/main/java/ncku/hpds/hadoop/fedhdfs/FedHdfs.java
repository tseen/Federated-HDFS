package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

class FetchFsimage {
	
	/*static void DownloadFedHdfsFsImage(String hostName) throws Exception{
    Process pl = Runtime.getRuntime().exec("wget http://" + hostName + ":50070/imagetransfer?getimage=1/&txid=latest -O Fsimage" + hostName);
    String line = "";
    BufferedReader p_in = new BufferedReader(new InputStreamReader(pl.getInputStream()));
    while((line = p_in.readLine()) != null){
            System.out.println(line);
         }
         p_in.close();
    }*/
	
	static void initialize() {
		File FsIFolder = new File("FedFSImage");
		if (!FsIFolder.exists()) {
			if (FsIFolder.mkdir())
				System.out.println("\n[INFO] FedFSImage folder is created!");
		} else {
			System.out.println("\n[INFO] FedFSImage is already update ");
		}
	}

	static void downloadFedHdfsFsImage(String hostName,
			String dfsNamenodeHttpAddress) throws Exception {
		Process pl = Runtime
				.getRuntime()
				.exec("wget http://"
						+ dfsNamenodeHttpAddress
						+ "/imagetransfer?getimage=1/&txid=latest -O FedFSImage/Fsimage"
						+ hostName);
		String line = "";
		BufferedReader p_in = new BufferedReader(new InputStreamReader(
				pl.getInputStream()));
		while ((line = p_in.readLine()) != null) {
			System.out.println(line);
		}
		p_in.close();
	}

	static void offlineImageViewer(String hostName) throws Exception {
		Process pl = Runtime.getRuntime().exec(
				"bin/hdfs oiv -i FedFSImage/Fsimage" + hostName
						+ " -o FedFSImage/OIVFsimage" + hostName);
		String line = "";
		BufferedReader p_in = new BufferedReader(new InputStreamReader(
				pl.getInputStream()));
		while ((line = p_in.readLine()) != null) {
			System.out.println(line);
		}
		p_in.close();
	}
}

class lsr {
	public void printFilesRecursively(String Url) throws IOException {
		try {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			// fs.initialize(new URI(Url), new Configuration());
			FileStatus[] status = fs.listStatus(new Path(Url));
			for (int i = 0; i < status.length; i++) {
				if (status[i].isDirectory()) {
					System.out
							.println("Dir: " + status[i].getPath().toString());
					printFilesRecursively(status[i].getPath().toString());
				} else {
					try {
						System.out.println("  file: "
								+ status[i].getPath().toString());
						// System.out.println("  file: " +
						// status[i].getPath().getName());
						System.out.println("  file: " + status[i].toString());
					} catch (Exception e) {
						System.err.println(e.toString());
					}

				}

			}
		} catch (IOException e) {
			// Logger.getLogger(RecursivelyPrintFilesOnHDFS.class.getName()).log(Level.SEVERE,
			// null, ex);
		}
	}
}

class fedls {
	static void print_info(String Uri, Configuration conf, String hostName) throws IOException {

		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);

		if (fileStatus.isDirectory() == false) {
			System.out.println("\n");
			System.out
					.println("====================================================================================");
			System.out
					.println("=============================GlobalNamespace-Cluster:" + hostName + "============================");
			System.out
					.println("====================================================================================");

			System.out.println("\nThe metadatargs.lengtha of the file from HDFS.");
			System.out.println("This is a file.");
			System.out.println("file Name: " + fileStatus.getPath().getName());
			System.out.println("file Path: " + fileStatus.getPath());
			System.out.println("file Lehgth: " + fileStatus.getLen());
			System.out.println("file Modification time: "
					+ new Timestamp(fileStatus.getModificationTime())
							.toString());

			System.out.println("file last Modification time: "
					+ new Timestamp(fileStatus.getAccessTime()).toString());

			System.out.println("file replication: "
					+ fileStatus.getReplication());
			System.out.println("file's Block size: "
					+ fileStatus.getBlockSize());
			System.out.println("file owner: " + fileStatus.getOwner());
			System.out.println("file group: " + fileStatus.getGroup());
			System.out.println("file permission: "
					+ fileStatus.getPermission().toString());
			System.out.println();
		}

		else if (fileStatus.isDirectory() == true) {
			System.out.println("\n");
			System.out
					.println("====================================================================================");
			System.out
					.println("=============================GlobalNamespace-Cluster:" + hostName + "============================");
			System.out
					.println("====================================================================================");
			System.out
					.println("\nThe metadata of the directory from HDFS.");
			System.out.println("This is a directory.");

			System.out.println("dir Path: " + fileStatus.getPath());
			System.out.println("dir Length: " + fileStatus.getLen());
			System.out.println("dir Modification time: "
					+ new Timestamp(fileStatus.getModificationTime())
							.toString());

			System.out.println("dir last Modification time:"
					+ new Timestamp(fileStatus.getAccessTime()).toString());

			System.out.println("dir replication: "
					+ fileStatus.getReplication());
			System.out.println("dir's Blocak size: "
					+ fileStatus.getBlockSize());
			System.out.println("dir owner: " + fileStatus.getOwner());
			System.out.println("dir group: " + fileStatus.getGroup());
			System.out.println("dir permission: "
					+ fileStatus.getPermission().toString());

			System.out.println("The file or dir of this direcroty: ");
			for (FileStatus fs : FS.listStatus(new Path(Uri))) {
				System.out.println(fs.getPath());
			}

		}
	}
	
}

public class FedHdfs {

	public static void main(String[] args) throws Exception {
		
		/* lsr doing */
		// String Path = args[0];
		// Recursively rc = new Recursively();
		// rc.printFilesRecursively(Path);conf
		
		String[] uri = args;
		//Configuration[] conf = new Configuration[args.length];
		
		java.io.File path = new java.io.File("etc/hadoop/fedhadoop-clusters.xml");
		FedHdfsConParser hdfsIpList = new FedHdfsConParser(path);
		Vector<Element> theElements = hdfsIpList.getElements();
		
		Configuration[] conf = new Configuration[theElements.size()];
		
		//Setting fedHDFS conf.
		/*for (int i = 0; i < args.length; i++) {
			conf[i] = new Configuration();
			conf[i].set(
					"fs.default.name",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theElements.elementAt(i)));
		}*/
		
		for (int i = 0; i < theElements.size(); i++) {
			conf[i] = new Configuration();
			conf[i].set(
					"fs.default.name",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theElements.elementAt(i)));
		}
		
		
		
		for (int i = 0; i < theElements.size(); i++) {
			if (uri[0].equalsIgnoreCase(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)))) {
				fedls.print_info(uri[1], conf[i], FedHdfsConParser.getValue("HostName",
						theElements.elementAt(i)));
			}
		}

		/*for (int i = 0; i < args.length; i++) {
			fedls.print_info(uri[i], conf[i], FedHdfsConParser.getValue("HostName",
					theElements.elementAt(i)));
		}*/
		
		
		FetchFsimage.initialize();
		for (int i = 0; i < theElements.size(); i++) {
			FetchFsimage.downloadFedHdfsFsImage(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)), FedHdfsConParser.getValue("dfs.namenode.http-address", theElements.elementAt(i)));
			FetchFsimage.offlineImageViewer(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)));
		}
		
	}

}
