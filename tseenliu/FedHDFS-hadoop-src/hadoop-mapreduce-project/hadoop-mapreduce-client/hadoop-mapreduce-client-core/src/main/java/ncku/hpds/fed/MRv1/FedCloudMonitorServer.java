package ncku.hpds.fed.MRv1;

import java.io.*;
import java.net.*;
public class FedCloudMonitorServer extends Thread {
	private ServerSocket mServer;
    private int mListenPort = 0;
    private boolean mRunFlag = false;
    private FedClientSocket mAcceptFedSocket = null;
    private FedCloudProtocol.FedSocketState mState = 
        FedCloudProtocol.FedSocketState.NONE;
    private Object mLock = new Object();

    class FedClientSocket {
        public Socket mClientSocket = null;
        public InputStream mIS = null;
        public OutputStream mOUT = null;
        public BufferedReader mInputReader = null;
        public PrintWriter mOutputWriter = null;
        public FedClientSocket ( Socket s ) throws Exception {
            mClientSocket = s;
            mClientSocket.setSoTimeout(500); // 500ms, 0.5s
            mClientSocket.setTcpNoDelay(true);
            mIS = mClientSocket.getInputStream();
            mOUT = mClientSocket.getOutputStream();
            mInputReader = new BufferedReader(new InputStreamReader(mIS));
            mOutputWriter = new PrintWriter(mOUT);
        }
        public BufferedReader getInputReader() { return mInputReader; }
        public PrintWriter getOutputWriter() { return mOutputWriter;  }
        public void close() { 
            try { 
                mClientSocket.close(); 
            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }
    } 

    public FedCloudMonitorServer(int port) {
        mListenPort = port;
    } 
    public void run() { 
        try {
            mServer = new ServerSocket(mListenPort);
            mServer.setSoTimeout(5000); //wait 5 second
            mServer.setReuseAddress(true);
            //mServer.setTcpNoDelay(true);
            mRunFlag = true;
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        int i = 0;
        while ( mRunFlag ) {
            try {
                BufferedReader clientInput = null;
                PrintWriter clientOutput = null;
                String line = null;
                synchronized ( mLock ) { 
                    mState = FedCloudProtocol.FedSocketState.ACCEPTING;
                }
                if ( i > 12 ) {
                    System.out.println("no incoming connection, break out");
                    break;
                }
                Socket s = mServer.accept();
                if ( s == null ) {
                    i ++;
                    continue;
                }
                mAcceptFedSocket = new FedClientSocket(s); 
                clientInput = mAcceptFedSocket.getInputReader();
                clientOutput = mAcceptFedSocket.getOutputWriter();
                synchronized ( mLock ) { 
                    mState = FedCloudProtocol.FedSocketState.ACCEPTED;
                }
                while ( mRunFlag ) {
                    synchronized ( mLock ) { 
                        line = clientInput.readLine(); 
                        //System.out.println("Recv : " + line );
                        if ( line.contains( FedCloudProtocol.REQ_PING ) ) {
                            clientOutput.println( FedCloudProtocol.RES_PONG );
                            clientOutput.flush();
                        }
                        if ( line.contains( FedCloudProtocol.REQ_BYE ) ) {
                            clientOutput.println( FedCloudProtocol.RES_BYE );
                            clientOutput.flush();
                            break;
                        }
                    }
                    Thread.sleep(500); // 500ms 
                }
            } catch ( Exception e ) { 
                i++;
                e.printStackTrace();
                //System.out.println("accept error continue wait");
            } finally {
                synchronized ( mLock ) { 
                    if ( mAcceptFedSocket != null ) {
                        mAcceptFedSocket.close();
                        mAcceptFedSocket = null;
                    }
                    mState = FedCloudProtocol.FedSocketState.DISCONNECTED;
                }
            }
        } // while
        try {
            if ( mServer != null ) {
                mServer.close();
                System.out.println("Region Cloud Server Closed");
            } else {
                System.out.println("mServer == null");
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }    
    public void stopServer() {
        try { 
            mRunFlag = false;
            this.join();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    private String sendMessage(String m) {
        String res = "";
        try {
            synchronized ( mLock ) {
                if ( mState == FedCloudProtocol.FedSocketState.ACCEPTED && mAcceptFedSocket != null ) {
                    PrintWriter pr = mAcceptFedSocket.getOutputWriter();
                    pr.println(m);
                    pr.flush();
                    BufferedReader br = mAcceptFedSocket.getInputReader();
                    res = br.readLine();
                    pr = null;
                    br = null;
                }
            }
            Thread.sleep(500);
        } catch ( Exception e ) {
        }
        return res;
    }
    public String sendPing() { 
        return sendMessage( FedCloudProtocol.REQ_PING );
    }
    public String sendMapPRFinished() { 
        return sendMessage( FedCloudProtocol.REQ_MAP_PROXY_REDUCE_FINISHED );
    }
    public String sendMigrateData(String payload) { 
        return sendMessage( FedCloudProtocol.REQ_MIGRATE_DATA +";"+payload );
    }
    public String sendMigrateDataFinished(String payload) {
        return sendMessage( FedCloudProtocol.REQ_MIGRATE_DATA_FINISHED + ";"+payload );
    }
	
}
