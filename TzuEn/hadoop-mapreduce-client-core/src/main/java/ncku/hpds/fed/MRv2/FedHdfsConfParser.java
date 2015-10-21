package ncku.hpds.fed.MRv2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class FedHdfsConfParser {
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

    public FedHdfsConfParser (String CoworkingConfPath) {
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
           
            NodeList clusterList = mDoc.getElementsByTagName("Cluster");
            String sArgPrefix = "Arg";
            String paths = "";
            for ( int i = 0 ; i < clusterList.getLength(); i++ ) {
                Node cluster = clusterList.item(i);
                if ( cluster.getNodeType() == Node.ELEMENT_NODE ) {
                    Element clusterElement = (Element) cluster;
                    FedHadoopConf fedHadoopConf = new FedHadoopConf(
                           FedHadoopConf.ROLE.RegionCloud); 
                    // get tag value
                    String[] fs = getTagValue("fs.default.name", clusterElement ,INVALID_VALUE).split(":");
                    fedHadoopConf.setName(
                            getTagValue("HostName", clusterElement ,INVALID_VALUE));
                    fedHadoopConf.setAddress(fs[0]);
                    fedHadoopConf.setHadoopHome(
                            getTagValue("hadoop-home.dir", clusterElement ,INVALID_VALUE));
                    fedHadoopConf.setTopCloudHDFSURL(
                    		getTagValue("fs.default.name", clusterElement,INVALID_VALUE) );
                    mRegionCloudServerListenPort = 
                            getTagValue("RegionCloudListenPort", clusterElement, 
                                    FedHadoopConf.DEFAULT_REGION_CLOUD_SERVER_LISTEN_PORT );
                    mRegionCloudList.add(fedHadoopConf);
               } 
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    public String getSubmittedTime(){
    	return mFedJobSubmittedTime;
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
