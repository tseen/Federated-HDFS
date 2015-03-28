package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Element;

public class SuperNamenode {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Thread newThread = new Thread(new TimerThread());
		newThread.start();
		
		/*Thread newThread1 = new Thread(new ReadUserDefinedLT());
		newThread1.start();*/
		
		Thread newThread2 = new Thread(new GlobalNamespaceRunnable());
		newThread2.setDaemon(true);
		newThread2.start();

	}
}

class TimerThread implements Runnable {

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(1000);
				Date now = new Date();
				System.out.println("Time now : " + now);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

class ReadUserDefinedLT implements Runnable {

    @Override
    public void run() {
    	
    	GlobalNamespace GN1 = new GlobalNamespace();
        final Scanner in = new Scanner(System.in);
        System.out.println("Please input logical mapping : ");
        System.out.println("(ex : logicalName --> hostName:Path)");
        while(in.hasNext()) {
            final String line = in.nextLine();
            if ("end".equalsIgnoreCase(line)) {
                System.out.println("Ending one thread");
                break;
            }
            String[] split = line.split(" --> ");
            System.out.println("Logical File is : " + split[0]);
            String[] subSplit = split[1].split(":");
            System.out.println("HostName is : " + subSplit[0]);
            System.out.println("Physical Path is : " + subSplit[1]);
            
            try {
    			GN1.UserDefinedVD(split[0], subSplit[0], subSplit[1]);
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
  
           
            System.out.println("Please input logical mapping : ");
            System.out.println("(ex : logicalName --> hostName:Path)");
        }
    }

}

class GlobalNamespaceRunnable implements Runnable {

	@Override
	public void run() {
		try{
			while(true){
				Thread.sleep(60000);
				GlobalNamespace GN1 = new GlobalNamespace();
				GN1.setFedConf();
				GN1.DynamicConstructPD();
				
				System.out.println("!!!! PHashTable download sucessful !!!!");
				
				//GN1.createVDrive();
				
				//System.out.println("!!!! LHashTable download sucessful !!!!");
			}
			
		}catch(InterruptedException e){	
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}