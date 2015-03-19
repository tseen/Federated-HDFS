package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Element;

public class SuperNamenode {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String[] uri = args;
		Configuration[] conf = new Configuration[args.length];
		
		java.io.File path = new java.io.File("etc/hadoop/fedhadoop-clusters.xml");
		FedHdfsConParser hdfsIpList = new FedHdfsConParser(path);
		Vector<Element> theElements = hdfsIpList.getElements();

		new Thread(new FetchFsimageRunnable()).start();
		new Thread(new PrintRunnable()).start();
	}
}

class PrintRunnable implements Runnable {

	@Override
	public void run() {
		final Scanner in = new Scanner(System.in);
		while (in.hasNext()) {
			final String line = in.nextLine();
			System.out.println("Input line: " + line);
			if ("end".equalsIgnoreCase(line)) {
				System.out.println("Ending one thread");
				break;
			}
		}
	}

}

class FetchFsimageRunnable implements Runnable {

	@Override
	public void run() {
		int i = 5;
		while (i > 0) {
			
			System.out.println("download loading: " + i--);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
				throw new IllegalStateException(ex);
			}
		}
		System.out.println("!!!! FedHdfsFsImage download sucessful !!!!");
		
		/*FetchFsimage.initialize();
		for (int i = 0; i < args.length; i++) {
			//FetchFsimage.initialize();
			FetchFsimage.downloadFedHdfsFsImage(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)), FedHdfsConParser.getValue("dfs.namenode.http-address", theElements.elementAt(i)));
			FetchFsimage.offlineImageViewer(FedHdfsConParser.getValue("HostName", theElements.elementAt(i))); */
		
		
	}

}