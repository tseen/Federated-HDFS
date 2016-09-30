package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class FetchFsimage {
	
	/*static void DownloadFedHdfsFsImage(String hostName) throws Exception{
    Process pl = Runtime.getRuntime().exec("wget http://" + hostName + ":50070/imagetransfer?getimage=1/&txid=latest -O Fsimage" + hostName);
    String line = "";
    BufferedReader p_in = new BufferedReader(new InputStreamReader(pl.getInputStream()));
    while((line = p_in.readLine()) != null){
            System.out.println(line);
         }
         p_in.close();
    }*/
	
	public static void initialize() {
		File FsIFolder = new File("FedFSImage");
		if (!FsIFolder.exists()) {
			if (FsIFolder.mkdir())
				System.out.println("\n[INFO] FedFSImage folder is created!");
		} else {
			System.out.println("\n[INFO] FedFSImage is already update ");
		}
	}

	public static void downloadFedHdfsFsImage(String hostName,
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

	public static void offlineImageViewer(String hostName) throws Exception {
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
