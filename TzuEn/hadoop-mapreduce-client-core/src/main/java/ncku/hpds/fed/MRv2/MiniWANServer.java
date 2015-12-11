package ncku.hpds.fed.MRv2;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class MiniWANServer extends Thread {

	private Socket socket = null;

	public MiniWANServer(Socket socket) {

		super("MiniServer");
		this.socket = socket;

	}

	public void run() {
		InputStream in = null;
		OutputStream out = null;
		try {
			in = socket.getInputStream();
		} catch (IOException ex) {
			System.out.println("Can't get socket input stream. ");
		}

	/*	try {
			out = new FileOutputStream("test.xml");
		} catch (FileNotFoundException ex) {
			System.out.println("File not found. ");
		}*/

		byte[] bytes = new byte[16 * 1024];

		int count;
		try {
			System.out.println("START");
			long startTime = System.currentTimeMillis();
			while ((count = in.read(bytes)) > 0) {
			//	out.write(bytes, 0, count);
			}
			long endTime = System.currentTimeMillis();
			System.out.println("END");
			System.out.println(socket.getInetAddress().toString()+"WAN:" + (float)((float) 5.0000f/  (((float)endTime - (float)startTime) / 1000f)));

			//out.close();
			in.close();
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	// implement your methods here

}
