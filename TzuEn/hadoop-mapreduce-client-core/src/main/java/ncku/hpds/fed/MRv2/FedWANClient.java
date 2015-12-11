package ncku.hpds.fed.MRv2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

public class FedWANClient extends Thread {

	public void run() {
		Socket socket = null;
		String host = "10.3.1.2";

		try {
			socket = new Socket(host, 4444);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		byte[] bytes = new byte[5 * 1024 * 1024];
		Arrays.fill( bytes, (byte) 1 );
		InputStream in = new ByteArrayInputStream(bytes);
		OutputStream out;
		try {
			out = socket.getOutputStream();

			int count;
			while ((count = in.read(bytes)) > 0) {
				out.write(bytes, 0, count);
			}

			out.close();
			in.close();
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
