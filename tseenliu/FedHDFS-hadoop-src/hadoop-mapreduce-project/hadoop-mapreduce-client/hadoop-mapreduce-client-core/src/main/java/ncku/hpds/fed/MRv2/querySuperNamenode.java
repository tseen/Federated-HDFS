package ncku.hpds.fed.MRv2;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

import ncku.hpds.hadoop.fedhdfs.FedHdfsConParser;

public class querySuperNamenode {

	private static ArrayList<String> requestGlobalFile;

	public ArrayList<String> query(File XMLfile) throws Throwable {

		String SNaddress = "10.3.1.34";
		int SNport = 8763;

		String globalfileInput = FedHdfsConParser.getFedInputFile(XMLfile);
		Socket client = new Socket(SNaddress, SNport);

		try {
			OutputStream stringOut = client.getOutputStream();
			// 送出字串
			// String globalFile = parseArg[3];
			stringOut.write(FedHdfsConParser.getFedInputFile(XMLfile).getBytes());
			System.out.println("send globalFile to GN query server : " + globalfileInput);
			ObjectInputStream objectIn = new ObjectInputStream(client.getInputStream());
			Object object = objectIn.readObject();

			requestGlobalFile = (ArrayList<String>) object;

			stringOut.flush();
			stringOut.close();
			stringOut = null;
			objectIn.close();
			client.close();
			client = null;

		} catch (IOException e) {
			System.out.println("Socket connect error");
			System.out.println("IOException :" + e.toString());
		}
		return requestGlobalFile;
	}
}