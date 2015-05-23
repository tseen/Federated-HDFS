package ncku.hpds.hadoop.fedhdfs;

import java.io.File;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class XMLTransformer {
	
	public static String FedMR = "FedMR.xml";
	private static File FedConfpath = new File("etc/hadoop/fedhadoop-clusters.xml");
	DocumentBuilderFactory icFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder icBuilder;
    
    public void transformer(ArrayList<String> requestGlobalFile, String topCloud, String TopJarPath) {
    	try {
            icBuilder = icFactory.newDocumentBuilder();
            Document doc = icBuilder.newDocument();
            Element mainRootElement = doc.createElement("FedHadoop");
            doc.appendChild(mainRootElement);
           
            // append child elements to root element
            mainRootElement.appendChild(getTopCloud(doc, topCloud, TopJarPath));
            
            for ( int i = 0 ; i < requestGlobalFile.size(); i++ ) {
            	String tmpHostPath[] = requestGlobalFile.get(i).split(":");
            	String tmpHost = tmpHostPath[0];
            	String subInput = tmpHostPath[1];
            	
            	mainRootElement.appendChild(getRegionCloud(doc, tmpHost, subInput));
            }
            
            // output DOM XML to console
            /*Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource source = new DOMSource(doc);
            StreamResult console = new StreamResult(System.out);
            transformer.transform(source, console);*/
            
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(FedMR));
            transformer.transform(source, result);
            
            System.out.println("\nXML DOM Created Successfully");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static Node getTopCloud(Document doc, String topCloud, String TopJarPath) {
    	String HdfsUriSplit[] = FedHdfsConParser.getHdfsUri(FedConfpath, topCloud).split(":");
		String CloudAddress = HdfsUriSplit[0];
    	
        Element TopCloud = doc.createElement("TopCloud");
        TopCloud.appendChild(getCompanyElements(doc, TopCloud, "CloudAddress", CloudAddress));
        TopCloud.appendChild(getCompanyElements(doc, TopCloud, "JobName", FedHdfsConParser.getFedMainClass(FedConfpath) + "-YARN"));
        TopCloud.appendChild(getCompanyElements(doc, TopCloud, "JarPath", TopJarPath));
        //TopCloud.appendChild(getCompanyElements(doc, TopCloud, "JarPath", FedHdfsConParser.getFedJarPath(FedConfpath)));
        return TopCloud;
    }
    
    private static Node getRegionCloud(Document doc, String hostName, String input) {
        Element RegionCloud = doc.createElement("RegionCloud");
        
        String HdfsUriSplit[] = FedHdfsConParser.getHdfsUri(FedConfpath, hostName).split(":");
		String CloudAddress = HdfsUriSplit[0];
        //company.setAttribute("id", id);
        RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, "Name", hostName));
        RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, "CloudAddress", CloudAddress));
        RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, "HadoopHome", FedHdfsConParser.getHadoopHOME(FedConfpath, hostName)));
        RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, "JobName", FedHdfsConParser.getFedMainClass(FedConfpath) + "-YARN"));
        RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, "MainClass", FedHdfsConParser.getFedMainClass(FedConfpath)));
        RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, "OtherArgs", FedHdfsConParser.getFedOtherArgs(FedConfpath)));
        RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, "Arg0", input));
        for ( int j = 1 ; j <= FedHdfsConParser.getFedElementLen(FedConfpath) ; j++ ) {
        	String argTag = "Arg" + String.valueOf(j);
        	RegionCloud.appendChild(getCompanyElements(doc, RegionCloud, argTag, FedHdfsConParser.getFedArgs(FedConfpath, argTag)));
        }
        return RegionCloud;
    }
    
    // utility method to create text node
    private static Node getCompanyElements(Document doc, Element element, String name, String value) {
        Element node = doc.createElement(name);
        node.appendChild(doc.createTextNode(value));
        return node;
    }
}
