package ncku.hpds.fed.MRv1;

import java.net.*;
import java.io.*;

class FedCloudMonitorClient extends Thread {

    private String mIP = "";
    private int mPort = 0;
    private boolean mRunFlag = false;
    private Object mLock = new Object();
    private Socket mSocket = null;
    private FedCloudProtocol.FedSocketState mState = 
        FedCloudProtocol.FedSocketState.NONE;
    private BufferedReader mInput = null;
    private PrintWriter mOutput = null;

    public	FedCloudMonitorClient (String ip, int port ){
        mIP = ip;
        mPort = port;
	}

	public void run() {
        int i = 0;
        synchronized(mLock) {
            do {
                try { 
                    mRunFlag = false;
                    mState = FedCloudProtocol.FedSocketState.CONNECTING;
                    mSocket = new Socket();
                    mSocket.setSoTimeout(10000); //wait 10 second
                    mSocket.setTcpNoDelay(true);
                    mSocket.setReuseAddress(true);
                    mSocket.setKeepAlive(true);
                    mSocket.connect( new InetSocketAddress(mIP, mPort), 300000 );
                    if ( mSocket.isConnected() )  {
                        mRunFlag = true;
                        mInput = new BufferedReader( new InputStreamReader( mSocket.getInputStream()));
                        mOutput = new PrintWriter( mSocket.getOutputStream() );
                        mState = FedCloudProtocol.FedSocketState.CONNECTED;
                        break;
                    }
                } catch ( Exception e ) {
                    e.printStackTrace();
                    mRunFlag = false;
                } 
                i ++;
                try {
                    System.out.println("connect to " + mIP + " failed reconnect ");
                    Thread.sleep(3000);
                } catch ( Exception e ) {
                    e.printStackTrace();
                }
            } while ( i < 20 && mRunFlag == false );
        }
        String line = "";
        System.out.println("Client to " + mIP + "  mRunFlag = " + mRunFlag );
        while ( mRunFlag ) {
            try{ 
                synchronized ( mLock ) { 
                    mOutput.println( FedCloudProtocol.REQ_PING ); 
                    mOutput.flush();
                    line = mInput.readLine();
                    //System.out.println("Recv from " + mIP + " : " + line );
                    if ( line.contains( FedCloudProtocol.REQ_MAP_PROXY_REDUCE_FINISHED ) ) {
                        mOutput.println( FedCloudProtocol.RES_OK ); 
                        mOutput.flush();
                    } else if ( line.contains( FedCloudProtocol.REQ_MIGRATE_DATA ) ) {
                        mOutput.println( FedCloudProtocol.RES_OK ); 
                        mOutput.flush();
                    } else if ( line.contains( FedCloudProtocol.REQ_MIGRATE_DATA_FINISHED ) ) {
                        mOutput.println( FedCloudProtocol.RES_OK ); 
                        mOutput.flush();
                        //TODO
                        mOutput.println( FedCloudProtocol.REQ_BYE ); 
                        mOutput.flush();
                        break;
                    } 
                }
                Thread.sleep(500); // 500ms 
            } catch ( Exception e ) {
                mRunFlag = false; 
            }
        }
        try { 
            synchronized( mLock ) {
                if ( mSocket.isConnected() ) {
                    mSocket.close();
                    mState = FedCloudProtocol.FedSocketState.DISCONNECTED;
                }
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
	} // end of run

    private String sendMessage(String m) {
        String res = "";
        try {
            synchronized ( mLock ) {
                if ( mState == FedCloudProtocol.FedSocketState.CONNECTED && 
                        mSocket != null ) {
                    mOutput.println(m);
                    mOutput.flush();
                    res = mInput.readLine();
                }
            }
        } catch ( Exception e ) {
        }
        return res;
    }
    public void stopClientProbe() {
        try { 
            sendMessage( FedCloudProtocol.REQ_BYE ); 
            mRunFlag = false;
            this.join();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}


