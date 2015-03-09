package ncku.hpds.hadoop.fedhdfs;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;



//import org.apache.hadoop.mapred.JobConf;

public class FedHdfsConParser {

	private Vector<Element> elementArray = new Vector<Element>(); //宣告Vector,並指定type為Element,目的是為了動態疊加Element物件
	
	public static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}

	public FedHdfsConParser(java.io.File file) {
		// get the factory
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		try {
			// Using factory get an instance of document builder
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			// parse using builder to get DOM representation of the XML file
			
			// java.io.File file = new java.io.File("mpath");
			Document doc = dBuilder.parse(file);
			doc.getDocumentElement().normalize();

			NodeList clusterlist = doc.getElementsByTagName("Cluster");

			for (int i = 0; i < clusterlist.getLength(); i++) {
				Node clustersNode = clusterlist.item(i);
				if (clustersNode.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) clustersNode;
					elementArray.addElement(element); //動態疊加Element物件
					
					/*System.out
							.println("==============================================================================================");

					System.out.println("Cluster Name : "
							+ getValue("HostName", element));
					System.out.println("Hdfs URL: "
							+ getValue("fs.default.name", element));

					System.out
							.println("==============================================================================================");*/
							

				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public Vector<Element> getElements() {
		return elementArray;
	}
	
}
