package ncku.hpds.fed.MRv2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public abstract class AbstractFedJobConf {
	 
	    public AbstractFedJobConf() {
	       
	    }
	    public abstract boolean isFedMR();
	    public abstract boolean isRegionCloud() ;
	    public abstract boolean isFedLoop() ;
	    public abstract boolean isFedTest();
	    public abstract void selectProxyReduce() ;
	    public abstract void selectProxyMap();
	    public abstract String getCoworkingConf() ;
	    public abstract FedHadoopConf getTopCloudConf() ;
	    public abstract List<FedHadoopConf> getRegionCloudConfList() ;
	    public abstract List<FedRegionCloudJob> getRegionCloudJobList() ;
	    public abstract List<FedCloudMonitorClient> getFedCloudMonitorClientList() ;
	    public abstract List<JarCopyJob> getJarCopyJobList() ;
	    public abstract Configuration getHadoopJobConf();
	    public abstract String getTopCloudHDFSURL();
	    public abstract int getRegionCloudServerListenPort() ;
	    public abstract String getRegionCloudOutputPath() ;
	    public abstract String getRegionCloudHadoopHome() ;
	    public abstract Path[] getRegionCloudOutputPaths() ;
	    public abstract String getRegionCloudInputPath() ;

		public abstract FedTopCloudJob getTopCloudJob() ;
		public abstract boolean isTopCloud() ;
		public abstract String getTopCloudInputPath() ;
	    public abstract String getTopCloudOutputPath() ;

		
	}



