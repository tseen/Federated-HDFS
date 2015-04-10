package ncku.hpds.hadoop.fedhdfs;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

public class FedHdfsConParser {

	private Vector<Element> elementArray = new Vector<Element>(); // 宣告Vector,並指定type為Element,目的是為了動態疊加Element物件

	public static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}

	public FedHdfsConParser(File XMLfile) {
		// get the factory
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		try {
			// Using factory get an instance of document builder
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			// parse using builder to get DOM representation of the XML file

			Document doc = dBuilder.parse(XMLfile);
			doc.getDocumentElement().normalize();

			NodeList clusterlist = doc.getElementsByTagName("Cluster");

			for (int i = 0; i < clusterlist.getLength(); i++) {

				Node clustersNode = clusterlist.item(i);

				if (clustersNode.getNodeType() == Node.ELEMENT_NODE) {

					Element element = (Element) clustersNode;
					elementArray.addElement(element); // 動態疊加Element物件
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String getHdfsUri(File XMLfile, String hostName) {
		
		String data = null;
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		try {
			
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(XMLfile);
			doc.getDocumentElement().normalize();

			NodeList clusterlist = doc.getElementsByTagName("Cluster");

			for (int i = 0; i < clusterlist.getLength(); i++) {

				Node clustersNode = clusterlist.item(i);
				Element element = (Element) clustersNode;
				/*if (hostName.equals(element.getAttributeNode("HostName").getChildNodes().toString())) {
					data = getValue("fs.default.name", element);
					break;
				}*/
				if (hostName.equals(getValue("HostName", element))) {
					data = getValue("fs.default.name", element);
					break;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data;
	}

	public Vector<Element> getElements() {
		return elementArray;
	}

}