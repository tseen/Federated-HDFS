package ncku.hpds.fed.MRv2;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class FedWANServer extends Thread {
	boolean listeningSocket = true;
	public void run() {
		
		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(4444);
		} catch (IOException ex) {
			System.out.println("Can't setup server on this port number. ");
		}

		Socket socket = null;
		InputStream in = null;
		OutputStream out = null;
		while (listeningSocket) {

			try {
				socket = serverSocket.accept();
				MiniWANServer mini = new MiniWANServer(socket);
				mini.start();
			} catch (IOException ex) {
				System.out.println("Can't accept client connection. ");
			}
		}

	}
	public void stopServer() {
		try { 
			Thread.sleep(5000);
			listeningSocket = false;
			this.join();
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}

}
