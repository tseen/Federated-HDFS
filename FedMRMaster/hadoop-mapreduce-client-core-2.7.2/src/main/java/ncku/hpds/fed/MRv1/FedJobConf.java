package ncku.hpds.fed.MRv1;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import java.util.List;
import java.util.ArrayList;
import java.io.File;

public class FedJobConf {
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
    private boolean mRegionCloudFlag = false;
    private boolean mFedLoopFlag = false;
    private boolean mFedTestFlag = false;
    private boolean mRegionCloudDone = false;
    private ProxySelector mSelector;
    private String mCoworkingConf = "";
    private String mTopCloudHDFSURL ="";
    private JobConf mJobConf = null;
    private int mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I;
    private String mRegionCloudOutputPath = "";
    private String mRegionCloudHadoopHome = "";
    private Path [] mRegionCloudOutputPaths = null;
    //---------------------------------------------------------------------
    public FedJobConf(JobConf jobConf) {
        // if command cotain "-D fed=on"
        mHadoopHome = System.getenv("HADOOP_HOME");
        System.out.println("Hadoop Home Path : " + mHadoopHome );
        if ( mHadoopHome != null ) {
            mDefaultCoworkingConf = mHadoopHome + mDefaultCoworkingConf ;
        } 

        mJobConf = jobConf;

        String fed = mJobConf.get("fed","off");
        System.out.println("fed = " + fed );
        if ( fed.toLowerCase().equals("on") || 
             fed.toLowerCase().equals("true") ) 
        {
            mFedFlag = true;
            /*
            if ( mJobConf.isIterative() ) {
                mFedLoopFlag = true;
            } 
            */
            mTopCloudHDFSURL = jobConf.get(FS_DEFAULT_NAME_KEY, FedJobConfParser.INVALID_VALUE);
        } 
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
        
        // if top cloud mode
        String fedTest = mJobConf.get("fedTest","off");
        if ( fedTest.toLowerCase().equals("on") ||
             fedTest.toLowerCase().equals("true") ) 
        {
            mFedTestFlag = true;
        }
        // check coworking configuration existed or not, 
        // if not existed, use the default coworking configuration
        System.out.println("mFedFlag = " + mFedFlag );
        if ( mFedFlag ) {  
            String mCoworkingConf = mJobConf.get("fedconf",mDefaultCoworkingConf);
            File conF = new File(mCoworkingConf); 
            if ( conF.exists() ) {
                if ( mCoworkingConf.equals(mDefaultCoworkingConf) == false  ) {
                    java.io.File coworkingFile = new java.io.File(mCoworkingConf);
                    if ( coworkingFile.exists() == false ) {
                        mCoworkingConf = mDefaultCoworkingConf;
                    }
                } 
                mParser = new FedJobConfParser( mCoworkingConf );
                mParser.parse();
                // add Region Cloud Job
                System.out.println("make FedRegionCloudJobs");
                //make RegionCloudJob
                mRegionJobList = new ArrayList<FedRegionCloudJob>();
                for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
                    conf.setRegionCloudServerListenPort( 
                            mParser.getRegionCloudServerListenPort() );
                    conf.setTopCloudHDFSURL( mTopCloudHDFSURL ) ;
                    FedRegionCloudJob regionJob = new FedRegionCloudJob(conf);
                    mRegionJobList.add(regionJob) ;
                }

                if ( mRegionJobList.size() > 0 ) {
                    mRegionCloudOutputPaths = new Path[ mRegionJobList.size() ];
                    int i = 0;
                    for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
                        String remote_path = conf.getHDFSOutputPath() + "/";
                        System.out.println("add path [" + remote_path + "] in InputPaths");
                        mRegionCloudOutputPaths[i] = new Path( remote_path ); 
                        i++;
                    }
                }
                System.out.println("FedRegionCloudJobs size : " + mRegionJobList.size() );
                /*
                 * move to the region cloud 
                // add Region Cloud Job Distcp
                mRegionJobDistcpList = new ArrayList<FedRegionCloudJobDistcp>();
                for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
                    FedRegionCloudJobDistcp regionJobDistcp = 
                        new FedRegionCloudJobDistcp(conf, mJobConf);
                    mRegionJobDistcpList.add(regionJobDistcp) ;
                }
                */
                System.out.println("make FedCloudMonitorClients");
                mFedCloudMonitorClientList = new ArrayList<FedCloudMonitorClient>();
                // make Region Cloud Monitor Job
                for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
                    FedCloudMonitorClient client = new FedCloudMonitorClient(
                            conf.getAddress(), 
                            Integer.valueOf( conf.getRegionCloudServerListenPort() ));
                    mFedCloudMonitorClientList.add(client);
                }
                // make Job Copy List 
                FedHadoopConf topConf = mParser.getTopCloudConf();
                mJarCopyJobList = new ArrayList<JarCopyJob>();
                if ( topConf.getJarPath().equals( FedJobConfParser.INVALID_VALUE ) == false ) {
                    for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
                        JarCopyJob jcj = new JarCopyJob( topConf, conf );
                        mJarCopyJobList.add(jcj);
                    }
                }
            }
        }
        mSelector = new ProxySelector(mJobConf);
    }
    public boolean isFedMR() { return mFedFlag; }
    public boolean isRegionCloud() { return mRegionCloudFlag; }
    public boolean isFedLoop() { return mFedLoopFlag; }
    public boolean isFedTest() { return mFedTestFlag; } 
    public void selectProxyReduce() {
        try {
            Class keyClz = mJobConf.getMapOutputKeyClass();
            Class valueClz = mJobConf.getMapOutputValueClass();
            mJobConf.setReducerClass( mSelector.getProxyReducerClass( keyClz, valueClz ));
        } catch (Exception e) {
        }
    }
    public void selectProxyMap() {
        //TODO in TopCloud
        try {
            Class keyClz = mJobConf.getMapOutputKeyClass();
            Class valueClz = mJobConf.getMapOutputValueClass();
            mJobConf.setMapperClass ( mSelector.getProxyMapperClass( keyClz, valueClz ));

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
    public JobConf getHadoopJobConf() { return mJobConf; } 
    public String getTopCloudHDFSURL() { return mTopCloudHDFSURL; } 
    public int getRegionCloudServerListenPort() { return mRegionCloudServerListenPort; }
    // getRegionCloudOutputPath is used in Region Cloud mode
    public String getRegionCloudOutputPath() { return mRegionCloudOutputPath; }
    public String getRegionCloudHadoopHome() { return mRegionCloudHadoopHome; }  
    // getRegionCloudOutputPaths is used in Top Cloud mode instead of original input path
    public Path[] getRegionCloudOutputPaths() { return mRegionCloudOutputPaths; }  
}
