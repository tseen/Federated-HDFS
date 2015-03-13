package ncku.hpds.hadoop.fedhdfs;

import java.net.URI;
import java.util.Collection;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.DFSUtil;

import org.w3c.dom.Element;

public class GlobalNamespace {

	public static void main(String[] args) throws Exception {

		// NamespaceInfo nsInfo = new NamespaceInfo();
		// nsInfo.getNamespaceID();

		String[] uri = args;

		Configuration[] conf = new Configuration[args.length];

		java.io.File path = new java.io.File("file.xml");
		FedHdfsConParser hdfsIpList = new FedHdfsConParser(path);
		Vector<Element> theElements = hdfsIpList.getElements();

		for (int i = 0; i < args.length; i++) {
			conf[i] = new Configuration();
			conf[i].set(
					"fs.default.name",
					"hdfs://"
							+ FedHdfsConParser.getValue("fs.default.name",
									theElements.elementAt(i)));
		}

		for (int i = 0; i < args.length; i++) {
			ShowInfo.print_info(uri[i], conf[i], FedHdfsConParser.getValue("HostName",
					theElements.elementAt(i)));
		}
		
		FetchFsimage.initialize();
		for (int i = 0; i < args.length; i++) {
			//FetchFsimage.initialize();
			FetchFsimage.downloadFedHdfsFsImage(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)), FedHdfsConParser.getValue("dfs.namenode.http-address", theElements.elementAt(i)));
			FetchFsimage.offlineImageViewer(FedHdfsConParser.getValue("HostName", theElements.elementAt(i)));
		}
		
		
		/*DatanodeRegistration createBPRegistration(NamespaceInfo nsInfo) {
		StorageInfo storageInfo = storage.getBPStorage(nsInfo.getBlockPoolID());
		if (storageInfo == null) {
			// it's null in the case of SimulatedDataSet
			storageInfo = new StorageInfo(
					DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
					nsInfo.getNamespaceID(), nsInfo.clusterID,
					nsInfo.getCTime(), NodeType.DATA_NODE);
		}

		String nsId = DFSUtil.getNamenodeNameServiceId(conf[0]);
		String test = HAUtil.getNameNodeId(conf[0], nsId);
		String clusterId = StartupOption.CLUSTERID.getClusterId();
		clusterId = NNStorage.newClusterID();*/	

	}
}
