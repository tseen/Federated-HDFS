package ncku.hpds.fed.MRv2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.List;
import java.util.ArrayList;
import java.io.File;

public class FedJobConf extends AbstractFedJobConf{
    //---------------------------------------------------------------------
    private static String DEFAULT_COWORKING_CONF = "/conf/coworking.xml"; 
    private static String FS_DEFAULT_NAME_KEY = "fs.default.name";
    private String mHadoopHome = "";
    private String mDefaultCoworkingConf = DEFAULT_COWORKING_CONF ;

    private List<FedRegionCloudJob> mRegionJobList = null;
    private List<FedRegionCloudJobDistcp> mRegionJobDistcpList = null;
    private List<JarCopyJob> mJarCopyJobList = null;
    private List<FedCloudMonitorClient> mFedCloudMonitorClientList = null;

    private FedJobConfParser mParser; 
    
    private boolean mFedFlag = false;   
    private boolean mTopCloudFlag = false;

    private boolean mRegionCloudFlag = false;
    private boolean mFedLoopFlag = false;
    private boolean mFedTestFlag = false;
    private boolean mRegionCloudDone = false;
    private ProxySelector mSelector;
    private String mCoworkingConf = "";
    private String mTopCloudHDFSURL ="";
    private Configuration mJobConf = null;
    private Job mJob = null;
    private String mTopCloudInputPath ="";
    private String mTopCloudOutputPath ="";
    private String mTopCloudHadoopHome ="";
    private int mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I;
    private String mRegionCloudOutputPath = "";
    private String mRegionCloudHadoopHome = "";
    private Path [] mRegionCloudOutputPaths = null;
    //---------------------------------------------------------------------
    public FedJobConf(Configuration jobConf,Job job) {
        // if command cotain "-D fed=on"
        mJob = job;
        mHadoopHome = System.getenv("HADOOP_HOME");
        System.out.println("Hadoop Home Path : " + mHadoopHome );
        if ( mHadoopHome != null ) {
            mDefaultCoworkingConf = mHadoopHome + mDefaultCoworkingConf ;
        } 

        mJobConf = jobConf;
	try {
	  Class outputFormat = mJob.getOutputFormatClass(); 
	  //mJob.setMapperClass ( mSelector.getProxyMapperClass( keyClz, valueClz ));
          System.out.println("###### outputFormat.getCanonicalName() = " + outputFormat.getCanonicalName() );
	} catch ( Exception e ) {
	}

    /*    String fed = mJobConf.get("fed","off");
        System.out.println("fed = " + fed );
        if ( fed.toLowerCase().equals("on") || 
             fed.toLowerCase().equals("true") ) 
        {
            mFedFlag = true;
        } */
        String regionCloud = mJobConf.get("regionCloud","off") ;
        // if region cloud mode
        if ( regionCloud.toLowerCase().equals("on") ||
             regionCloud.toLowerCase().equals("true") ) 
        {
            mRegionCloudFlag = true;
            mTopCloudHDFSURL = mJobConf.get("topCloudHDFS","");
            String port = mJobConf.get("regionCloudServerPort",
                    FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT);
            try { 
                mRegionCloudServerListenPort = Integer.valueOf(port);
            } catch ( Exception e  ) {
                e.printStackTrace();
                mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I;
            }
            mRegionCloudOutputPath = mJobConf.get("regionCloudOutput","");
            mRegionCloudHadoopHome = mJobConf.get("regionCloudHadoopHome","");
        }
        String topCloud = mJobConf.get("topCloud","off") ;
        // if top cloud mode
        if ( topCloud.toLowerCase().equals("on") ||
             topCloud.toLowerCase().equals("true") ) 
        {
            mTopCloudFlag = true;
          //  mTopCloudHDFSURL = mJobConf.get("topCloudHDFS","");
            
            mTopCloudOutputPath = mJobConf.get("topCloudOutput","");
            mTopCloudInputPath = mJobConf.get("topCloudInput","");
            mTopCloudHadoopHome = mJobConf.get("topCloudHadoopHome","");
        }
        
        mSelector = new ProxySelector(mJobConf, mJob);
    }
    public boolean isFedMR() { return mFedFlag; }
    public boolean isTopCloud() { return mTopCloudFlag; }
    public boolean isRegionCloud() { return mRegionCloudFlag; }
    public boolean isFedLoop() { return mFedLoopFlag; }
    public boolean isFedTest() { return mFedTestFlag; } 
    public void selectProxyReduce() {
        try {
            Class keyClz = mJob.getMapOutputKeyClass();
            Class valueClz = mJob.getMapOutputValueClass();
            mJob.setReducerClass( mSelector.getProxyReducerClass( keyClz, valueClz ));
            mJob.setMapOutputKeyClass ( keyClz );
            mJob.setMapOutputValueClass ( valueClz );
            mJob.setOutputKeyClass ( Text.class );
            mJob.setOutputValueClass ( Text.class );
            Class outputFormat = mJob.getOutputFormatClass(); 
	    mJob.setOutputFormatClass(TextOutputFormat.class);
        } catch (Exception e) {
        }
    }
    public void selectProxyMap() {
        //TODO in TopCloud
        try {
            Class keyClz = mJob.getMapOutputKeyClass();
            Class valueClz = mJob.getMapOutputValueClass();
            
	    /*
            Class outputFormat = mJob.getOutputFormatClass(); 
            //mJob.setMapperClass ( mSelector.getProxyMapperClass( keyClz, valueClz ));
            System.out.println("outputFormat.getCanonicalName() = " + outputFormat.getCanonicalName() );
            System.out.println("SequenceFileOutputFormat.class.getCanonicalName() = " + SequenceFileOutputFormat.class.getCanonicalName() );
            if ( outputFormat.getCanonicalName().equals(SequenceFileOutputFormat.class.getCanonicalName())){
                //mJob.setMapperClass ( mSelector.getProxyMapperClassSeq( keyClz, valueClz ));
                mJob.setMapperClass ( mSelector.getProxyMapperClass( keyClz, valueClz ));
            } else {
                mJob.setMapperClass ( mSelector.getProxyMapperClass( keyClz, valueClz ));
            }
	    */
	    mJob.setMapperClass ( mSelector.getProxyMapperClass( keyClz, valueClz ));

        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    
    public String getCoworkingConf() { return mCoworkingConf; };
    public FedHadoopConf getTopCloudConf() { return mParser.getTopCloudConf(); }
    public List<FedHadoopConf> getRegionCloudConfList() { 
        return mParser.getRegionCloudConfList(); }
    public List<FedRegionCloudJob> getRegionCloudJobList() {
        return this.mRegionJobList;
    }
    public List<FedCloudMonitorClient> getFedCloudMonitorClientList() {
        return this.mFedCloudMonitorClientList; 
    }
    public List<JarCopyJob> getJarCopyJobList() {
        return this.mJarCopyJobList;
    }
    public Configuration getHadoopJobConf() { return mJobConf; } 
    public String getTopCloudHDFSURL() { return mTopCloudHDFSURL; } 
    public String getTopCloudInputPath() { return mTopCloudInputPath; }
    public String getTopCloudOutputPath() { return mTopCloudOutputPath; }

    public int getRegionCloudServerListenPort() { return mRegionCloudServerListenPort; }
    // getRegionCloudOutputPath is used in Region Cloud mode
    public String getRegionCloudOutputPath() { return mRegionCloudOutputPath; }
    public String getRegionCloudHadoopHome() { return mRegionCloudHadoopHome; }  
    // getRegionCloudOutputPaths is used in Top Cloud mode instead of original input path
    public Path[] getRegionCloudOutputPaths() { return mRegionCloudOutputPaths; }
	@Override
	public FedTopCloudJob getTopCloudJob() {
		// TODO Auto-generated method stub
		return null;
	}  
}
