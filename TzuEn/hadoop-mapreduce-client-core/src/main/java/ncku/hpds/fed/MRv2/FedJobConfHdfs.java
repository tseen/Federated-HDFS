package ncku.hpds.fed.MRv2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import ncku.hpds.hadoop.fedhdfs.TopcloudSelector;
import ncku.hpds.hadoop.fedhdfs.shell.GetRegionPath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class FedJobConfHdfs extends AbstractFedJobConf {
	private static String DEFAULT_COWORKING_CONF = "/conf/coworking.xml"; 
    private static String FS_DEFAULT_NAME_KEY = "fs.default.name";
    private String mHadoopHome = "";
    private String mDefaultCoworkingConf = DEFAULT_COWORKING_CONF ;
    
    
   // private FedTopCloudJob mTopCloudJob = null;
    private List<FedTopCloudJob> mTopJobList = new ArrayList<FedTopCloudJob>();
    private List<FedRegionCloudJob> mRegionJobList = null;
    private List<FedRegionCloudJobDistcp> mRegionJobDistcpList = null;
    private List<JarCopyJob> mJarCopyJobList = null;
    private List<FedCloudMonitorClient> mFedCloudMonitorClientList = null;

    private FedHdfsConfParser mParser; 
    private boolean mTopCloudFlag = false;
    private boolean mFedFlag = false;
    private boolean mFedSubmitFlag = false;
    private boolean mRegionCloudFlag = false;
    private boolean mFedLoopFlag = false;
    private boolean mFedTestFlag = false;
    private boolean mRegionCloudDone = false;
    private ProxySelector mSelector;
    private String mCoworkingConf = "";
    private String mTopCloudHDFSURL ="";
    private Configuration mJobConf = null;
    private Job mJob = null;
    private int mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT_I;
    private String mRegionCloudOutputPath = "";
    private String mRegionCloudHadoopHome = "";
    private Path [] mRegionCloudOutputPaths = null;
    private String mTopHost = "";
	 public FedJobConfHdfs(Configuration jobConf,Job job,String inputName) throws Throwable {
	        // if command cotain "-D fed=on -D fedHdfs=on"
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
		    
		    String fedHdfs = mJobConf.get("fedHdfs","off");
	        System.out.println("fedHdfs = " + fedHdfs );
	      
	        if ( fedHdfs.toLowerCase().equals("on") || 
	        		fedHdfs.toLowerCase().equals("true") ) 
	        {
	            mFedFlag = true;
	          
	        } 	
	        
	        
			String globalfileInput ="";
			//Select top cloud
		    Path[] Inputs = FileInputFormat.getInputPaths((JobConf) mJobConf); 
		    if(Inputs.length > 0){
		    	String p[] = Inputs[0].toString().split("/");
		    	globalfileInput = p[p.length-1];
		    	System.out.println("Global File Name:"+globalfileInput);
		    }
			TopcloudSelector top;
			String realTop = "";
			try {
				top = new TopcloudSelector(globalfileInput, false);
				realTop = top.getTopCloud();
	
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		//	System.out.println("Top Cloud of FedMR:" + realTop);
	       
	       
	        
	 
	        // check coworking configuration existed or not, 
	        // if not existed, use the default coworking configuration
	        System.out.println("mFedFlag = " + mFedFlag );
	        if ( mFedFlag ) {  
	    //      String mCoworkingConf = mJobConf.get("fedconf",mDefaultCoworkingConf);
	            String mCoworkingConf = "etc/hadoop/fedhadoop-clusters.xml";

	            File conF = new File(mCoworkingConf); 
	            if ( conF.exists() ) {
	          //      if ( mCoworkingConf.equals(mDefaultCoworkingConf) == false  ) {
	          //          java.io.File coworkingFile = new java.io.File(mCoworkingConf);
	          //          if ( coworkingFile.exists() == false ) {
	          //              mCoworkingConf = mDefaultCoworkingConf;
	          //          }
	          //      } 
	                mParser = new FedHdfsConfParser( mCoworkingConf );
	                //TODO:
	                // remove parsing XML file of fedMR, instead using XML of fedHdfs
	                // DONE
	                // parse xml file
	                
	                mParser.parse();
	                // add Region Cloud Job
	                System.out.println("make FedRegionCloudJobs");
	                //make RegionCloudJob
	                mRegionJobList = new ArrayList<FedRegionCloudJob>();
	                // get some top cloud configuration from region cloud
	                String topAddress="";
	                String topHome="";
	                for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
	                	if(conf.getName().equalsIgnoreCase(realTop)){
	                		topAddress = conf.getAddress();
	                		topHome = conf.getHadoopHome();
	                		mTopCloudHDFSURL = "hdfs://"+conf.getTopCloudHDFSURL()+"/";
	                	}
	                }
	                //-----
	                String submitJarPath = mJob.getJar();
                    int lastSlash = submitJarPath.lastIndexOf("/"); // in unix/linux environment 
                    String regJarFileName = submitJarPath.substring( lastSlash+1 );
                    System.out.println("jar file name:"+ regJarFileName);
                    
                    //-----
                    String main = mJobConf.get("main","");
                    System.out.println("main class name:" +main);
                    //-----
                    String[] arg = new String[10];
                    for ( int j = 0 ; j < 10 ; j++ ) {
                        String argTag = "Arg" + String.valueOf(j);
            	        arg[j] = mJobConf.get(argTag,"none");
                    }

                    GetRegionPath fedHdfsInputGetter = new GetRegionPath();
	                for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
	                    conf.setRegionCloudServerListenPort( 
	                            mParser.getRegionCloudServerListenPort() );
	                    conf.setTopCloudHDFSURL( mTopCloudHDFSURL ) ;
	                    // set jar path
	                    String remoteJarPath = conf.getHadoopHome();
                        remoteJarPath = remoteJarPath +"/fed_task/" + regJarFileName;
                        conf.setJarPath( remoteJarPath );
                        conf.setMainClass(main);
                        //TODO configure the input of region cloud ( fedHdfs)
                        String inputpath = fedHdfsInputGetter.getRegionPath(conf.getName(),"AirDrive/"+globalfileInput ).toString();
                        System.out.println("set"+conf.getName()+ "Input Path:"+inputpath);
                        conf.setHDFSInputPath( inputpath);
                        //--
                        conf.setHDFSOutputPath( 
                            conf.getName() + "_" + conf.getMainClass()+"_OUT_"+ mParser.getSubmittedTime() );
                        for ( int j = 0 ; j < 10 ; j++ ) {
                        	conf.setArgs(j, arg[j]);
                        }
                        FedRegionCloudJob regionJob = new FedRegionCloudJob(conf);
	                    mRegionJobList.add(regionJob) ;
	                }

	                if ( mRegionJobList.size() > 0 ) {
	                    mRegionCloudOutputPaths = new Path[ mRegionJobList.size() ];
	                    int i = 0;
	                    for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
	                        String remote_path = conf.getHDFSOutputPath();
	                        System.out.println("add path [" + remote_path + "] in InputPaths");
	                        mRegionCloudOutputPaths[i] = new Path( remote_path ); 
	                        i++;
	                    }
	                }
	                System.out.println("FedRegionCloudJobs size : " + mRegionJobList.size() );
	                
	                System.out.println("make FedTopCloudJob");
	                //top cloud
	              //  FedHadoopConf topConf = mParser.getTopCloudConf();
	                
	                JobConf jobconf = new JobConf(mJobConf);   
	                for ( FedHadoopConf topConf : mParser.getTopCloudConfList() ) {
                    
	                    topConf.setRole(FedHadoopConf.ROLE.TopCloud);
	                    for ( int j = 0 ; j < 10 ; j++ ) {
	                    	topConf.setArgs(j, arg[j]);
	                    }
	                    //topConf.setName(realTop);
	                    //topConf.setAddress(topAddress);
	                    //topConf.setHadoopHome(topHome);
	                   // String srcHDFS = mJobConf.get(FS_DEFAULT_NAME_KEY,"");
	                    String topInputPath = "";
	                    System.out.println("asasas: "+mRegionCloudOutputPaths[0].toString().split("_")[0]);
	                    System.out.println("asassSSas: "+topConf.getName());

                    	if(mRegionCloudOutputPaths[0].toString().split("_")[0].equals(topConf.getName())){
                    		topInputPath = mRegionCloudOutputPaths[0].toString()+ TopCloudHasher.getFileName(topConf.getTopCloudHDFSURL());
                    	}
                    	else{
                    		topInputPath = mRegionCloudOutputPaths[0].toString();
                    	}
	                    for(int i =1; i< mRegionCloudOutputPaths.length; i++){
	                    	if(mRegionCloudOutputPaths[i].toString().split("_")[0].equals(topConf.getName())){
	                    		topInputPath += ","+mRegionCloudOutputPaths[i].toString() + TopCloudHasher.getFileName(topConf.getTopCloudHDFSURL());
	                    	}
	                    	else{
	                    		topInputPath += ","+mRegionCloudOutputPaths[i].toString();
	                    	}
	                    }
	                    String remoteJarPath = topConf.getHadoopHome();
	                    remoteJarPath = remoteJarPath +"/fed_task/" + regJarFileName;
	                    topConf.setJarPath( remoteJarPath );
	                    topConf.setMainClass(main);
	                    topConf.setHDFSInputPath(topInputPath);
		                topConf.setHDFSOutputPath( FileOutputFormat.getOutputPath(jobconf).toString());
	                    FedTopCloudJob topJob = new FedTopCloudJob(topConf);
	
	                    mTopJobList.add(topJob) ;
	                }

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
	                
	                mJarCopyJobList = new ArrayList<JarCopyJob>();
	              //  if ( topConf.getJarPath().equals( FedJobConfParser.INVALID_VALUE ) == false ) {
	                    for ( FedHadoopConf conf : mParser.getRegionCloudConfList() ) {
	                        JarCopyJob jcj = new JarCopyJob( submitJarPath, conf );
	                        mJarCopyJobList.add(jcj);
	                    }
	                //}
	            }
	        }
	       // mSelector = new ProxySelector(mJobConf, mJob);
	    }

	    public boolean isFedMR() { return mFedFlag; }
	    public boolean isTopCloud() { return mTopCloudFlag; }
	    public boolean isRegionCloud() { return mRegionCloudFlag; }
	    public boolean isFedLoop() { return mFedLoopFlag; }
	    public boolean isFedTest() { return mFedTestFlag; } 
	
	

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
	        return mJarCopyJobList;
	    }
	    public Configuration getHadoopJobConf() { return mJobConf; } 
	    public String getTopCloudHDFSURL() { return mTopCloudHDFSURL; } 
	    public int getRegionCloudServerListenPort() { return mRegionCloudServerListenPort; }
	    // getRegionCloudOutputPath is used in Region Cloud mode
	    public String getRegionCloudOutputPath() { return mRegionCloudOutputPath; }
	    public String getRegionCloudHadoopHome() { return mRegionCloudHadoopHome; }  
	    // getRegionCloudOutputPaths is used in Top Cloud mode instead of original input path
	    public Path[] getRegionCloudOutputPaths() { return mRegionCloudOutputPaths; }

		public void selectProxyReduce() {
			// TODO Auto-generated method stub
			
		}

		
		public void selectProxyMap() {
			// TODO Auto-generated method stub
			
		}

	
		public List<FedTopCloudJob> getTopCloudJobList() {
			return mTopJobList;
		}

		public String getTopCloudInputPath() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getTopCloudOutputPath() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getRegionCloudInputPath() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public FedTopCloudJob getTopCloudJob() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<String> getTopCloudHDFSURLs() {
			// TODO Auto-generated method stub
			return null;
		}  

}
