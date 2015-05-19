package ncku.hpds.fed.MRv2;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import ncku.hpds.hadoop.fedhdfs.FedHdfsConParser;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.apache.hadoop.mapred.JobConf;

public class FedJobConfParser {
    public static String INVALID_VALUE="none";
    private String mPath = "";
    private DocumentBuilderFactory mDBF = null;
    private DocumentBuilder mBuilder = null;
    private Document mDoc = null;
    private boolean mParsable = false;
    private FedHadoopConf mTopCloudConf = new FedHadoopConf(
            FedHadoopConf.ROLE.TopCloud);
    private List<FedHadoopConf> mRegionCloudList = new ArrayList<FedHadoopConf>();
    private String mFedJobSubmittedTime = "";
    private String mRegionCloudServerListenPort = "";

    
    public FedJobConfParser () {
    	
    }
    
    public FedJobConfParser (String CoworkingConfPath) {
        mPath = CoworkingConfPath;
        mFedJobSubmittedTime = getCurrentTime();
        java.io.File file = new java.io.File(mPath);
        if ( file.exists() ) {
            mParsable = true;
            try {
                mDBF = DocumentBuilderFactory.newInstance();
                mBuilder = mDBF.newDocumentBuilder();
                mDoc = mBuilder.parse(mPath);
            } catch ( Exception e ) {
                mParsable = false;
                e.printStackTrace();
            }
        } else {
            System.out.println("Coworking File [" + mPath + "] was not existed");
        }
    }
    public void parse() {
        if ( mParsable == false ) {
            return ;
        }
        try {
            String addr = "";
            String regJarFileName = INVALID_VALUE;
            mDoc.getDocumentElement().normalize();
            NodeList topCloudList = mDoc.getElementsByTagName("TopCloud");
            Node topCloudNode = topCloudList.item(0);
            if ( topCloudNode.getNodeType() == topCloudNode.ELEMENT_NODE ){
                Element topCloudElement = (Element) topCloudNode;
                mTopCloudConf.setAddress(
                        getTagValue("CloudAddress", topCloudElement ,INVALID_VALUE ));
                mTopCloudConf.setJobName(
                        getTagValue("JobName", topCloudElement ,INVALID_VALUE));
                mRegionCloudServerListenPort = 
                    getTagValue("RegionCloudListenPort", topCloudElement, 
                            FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT );
                mTopCloudConf.setJarPath(
                        getTagValue("JarPath", topCloudElement, INVALID_VALUE));
               if ( mTopCloudConf.getJarPath().equals( INVALID_VALUE ) == false ) {
                    String topJarPath = mTopCloudConf.getJarPath();
                    int lastSlash = topJarPath.lastIndexOf("/"); // in unix/linux environment 
                    regJarFileName = topJarPath.substring( lastSlash+1 );
               } 
            }
            System.out.println("regJarFileName = " + regJarFileName );
            NodeList regionCloudList = mDoc.getElementsByTagName("RegionCloud");
            String sArgPrefix = "Arg";
            for ( int i = 0 ; i < regionCloudList.getLength(); i++ ) {
                Node regionCloudNode = regionCloudList.item(i);
                if ( regionCloudNode.getNodeType() == Node.ELEMENT_NODE ) {
                    Element regionCloudElement = (Element) regionCloudNode;
                    FedHadoopConf fedHadoopConf = new FedHadoopConf(
                            FedHadoopConf.ROLE.RegionCloud); 
                    // get tag value
                    fedHadoopConf.setName(
                            getTagValue("Name", regionCloudElement ,INVALID_VALUE));
                    fedHadoopConf.setAddress(
                            getTagValue("CloudAddress", regionCloudElement ,INVALID_VALUE));
                    fedHadoopConf.setHadoopHome(
                            getTagValue("HadoopHome", regionCloudElement ,INVALID_VALUE));
                    fedHadoopConf.setJobName(
                            getTagValue("JobName", regionCloudElement ,INVALID_VALUE));
                    if ( regJarFileName.equals(INVALID_VALUE) ) {   
                        fedHadoopConf.setJarPath(
                                getTagValue("JarPath", regionCloudElement ,INVALID_VALUE));
                    } else {
                        String remoteJarPath = fedHadoopConf.getHadoopHome();
                        remoteJarPath = remoteJarPath +"/fed_task/" + regJarFileName;
                        fedHadoopConf.setJarPath( remoteJarPath );
                    }
                    fedHadoopConf.setMainClass(
                            getTagValue("MainClass", regionCloudElement ,INVALID_VALUE));
                    fedHadoopConf.setHDFSInputPath(
                            getTagValue("HDFSInputPath", regionCloudElement ,INVALID_VALUE));
                    fedHadoopConf.setHDFSOutputPath( 
                            getTagValue("HDFSOutputPath", regionCloudElement ,  fedHadoopConf.getName() + "_" + fedHadoopConf.getJobName()+"_OUT_"+ mFedJobSubmittedTime ));
                    for ( int j = 0 ; j < 10 ; j++ ) {
                        String argTag = "Arg" + String.valueOf(j);
                        fedHadoopConf.setArgs( j, getTagValue(argTag, regionCloudElement ,INVALID_VALUE));
                    }
                    fedHadoopConf.setOtherArgs(
                            getTagValue("OtherArgs", regionCloudElement ,INVALID_VALUE));
                    mRegionCloudList.add(fedHadoopConf);
               } 
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    
    public void fedHdfsConfparse(String CoworkingConfPath, ArrayList<String> requestGlobalFile, String topCloud) {
    	
    	mFedJobSubmittedTime = getCurrentTime();
    	File XMLfile = new File(CoworkingConfPath);
        String regJarFileName = INVALID_VALUE;
        String split[] = FedHdfsConParser.getHdfsUri(XMLfile, topCloud).split(":");
		String HostAddress = split[0];
        mTopCloudConf.setAddress(HostAddress);
        mTopCloudConf.setJobName(FedHdfsConParser.getFedMainClass(XMLfile) + "-YARN");
        mRegionCloudServerListenPort = FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT;
        mTopCloudConf.setJarPath(FedHdfsConParser.getFedJarPath(XMLfile));
		if (mTopCloudConf.getJarPath().equals(INVALID_VALUE) == false) {
			String topJarPath = mTopCloudConf.getJarPath();
			int lastSlash = topJarPath.lastIndexOf("/"); // in unix/linux environment
			regJarFileName = topJarPath.substring(lastSlash + 1);
		}
        System.out.println("TseEn-regJarFileName = " + regJarFileName );
        
        for ( int i = 0 ; i < requestGlobalFile.size(); i++ ) {
        	
        	String tmpHostPath[] = requestGlobalFile.get(i).split(":");
        	String tmpHost = tmpHostPath[0];
        	String subInput = tmpHostPath[1];
        	
        	//看有幾個region,就動態宣告 FedHadoopConf並加到要跑運算的ArrayList
            FedHadoopConf fedHadoopConf = new FedHadoopConf(FedHadoopConf.ROLE.RegionCloud); 
            // get tag value
            fedHadoopConf.setName(tmpHost);
            String HdfsUriSplit[] = FedHdfsConParser.getHdfsUri(XMLfile, tmpHost).split(":");
    		String Address = HdfsUriSplit[0];
            fedHadoopConf.setAddress(Address);
            fedHadoopConf.setHadoopHome(FedHdfsConParser.getHadoopHOME(XMLfile, tmpHost));
            fedHadoopConf.setJobName(FedHdfsConParser.getFedMainClass(XMLfile) + "-YARN");
            
            if ( regJarFileName.equals(INVALID_VALUE) ) {   
                fedHadoopConf.setJarPath(FedHdfsConParser.getFedJarPath(XMLfile));
            } else {
                String remoteJarPath = fedHadoopConf.getHadoopHome();
                remoteJarPath = remoteJarPath +"/fed_task/" + regJarFileName;
                fedHadoopConf.setJarPath( remoteJarPath );
            }
            
            fedHadoopConf.setMainClass(FedHdfsConParser.getFedMainClass(XMLfile));
            fedHadoopConf.setHDFSInputPath(INVALID_VALUE);
            fedHadoopConf.setHDFSOutputPath(fedHadoopConf.getName() + "_" + fedHadoopConf.getJobName() + "_OUT_" + mFedJobSubmittedTime);
            
            fedHadoopConf.setArgs( 0, subInput);
            /*for ( int j = 1 ; j < 10 ; j++ ) {*/
            for ( int j = 1 ; j < 2 ; j++ ) {
                String argTag = "Arg" + String.valueOf(j);
                fedHadoopConf.setArgs( j, FedHdfsConParser.getFedArgs(XMLfile, argTag));
            }
            fedHadoopConf.setOtherArgs(FedHdfsConParser.getFedOtherArgs(XMLfile));
            
            mRegionCloudList.add(fedHadoopConf); //不能更改
        }
    }
    
    public String getRegionCloudServerListenPort() {
        return mRegionCloudServerListenPort;
    }
    public FedHadoopConf getTopCloudConf() { 
        return this.mTopCloudConf;
    }
    public List<FedHadoopConf> getRegionCloudConfList() {
        return this.mRegionCloudList; 
    }
    private String getTagValue(String sTag, Element eElement,String defaultValue) {
        try { 
            NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
            Node nValue = (Node) nlList.item(0);
            return nValue.getNodeValue();
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Tag : " + sTag + " use default value : " + defaultValue);
        }
        return defaultValue;
    }
    private String getCurrentTime(){
        SimpleDateFormat sdFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        String strDate = sdFormat.format(date);
        return strDate;
    }
}