package ncku.hpds.hadoop.fedhdfs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

class ShellMonitor extends Thread {

    private InputStream mIS;
    private String mPrefix;

    public ShellMonitor(InputStream is, String prefix)  {
        mIS = is;
        mPrefix = prefix;
    }
    public void run()
    {
        try {
            InputStreamReader isr = new InputStreamReader(mIS);
            BufferedReader br = new BufferedReader(isr);
            String line=null;
            while ( (line = br.readLine()) != null) {
                System.out.println(mPrefix + ">" + line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Null InputStream in " + ShellMonitor.class );
        }
    }
}
