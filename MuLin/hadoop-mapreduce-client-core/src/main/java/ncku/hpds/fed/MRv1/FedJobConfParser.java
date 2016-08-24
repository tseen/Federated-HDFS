package ncku.hpds.fed.MRv1;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

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
