package ncku.hpds.hadoop.fedhdfs;

import java.io.*;
import java.net.*;
import java.util.*;

public class FetchFsimage {

	static void initialize() {
		File FsIFolder = new File("FedFSImage");
		if (!FsIFolder.exists()) {
			if (FsIFolder.mkdir())
				System.out.println("\nFedFSImage folder is created!");
		} else {
			System.out.println("Directory is exist!");
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