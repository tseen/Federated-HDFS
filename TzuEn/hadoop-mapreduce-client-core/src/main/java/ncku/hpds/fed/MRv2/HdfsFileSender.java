package ncku.hpds.fed.MRv2;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.tools.CacheAdmin;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileSender {
	private static final org.slf4j.Logger logger = LoggerFactory
			.getLogger(HdfsFileSender.class);

	UserGroupInformation ugi;
	public OutputStream out = null;
	public DFSClient client;
	private static final String utf8 = "UTF-8";
	public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
	private static final byte[] newline;
	static {
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}

	public HdfsFileSender() {
	}

	public ArrayList<DFSClient> DFS = new ArrayList<DFSClient>();
	public ArrayList<BufferedOutputStream> OUT = new ArrayList<BufferedOutputStream>();

	public void send(ArrayList<String> hdfsList, String fileName)
			throws UnresolvedLinkException, IOException, URISyntaxException {

		for (String hdfsUri : hdfsList) {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://" + hdfsUri);
			DFSClient dfs = new DFSClient(new URI("hdfs://" + hdfsUri), conf);
			try {
				DFS.add(dfs);
				OUT.add(new BufferedOutputStream(dfs.create(fileName, false)));
			} catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
				OUT.add(new BufferedOutputStream(dfs.append(fileName, 4096,
						null, null)));
			} catch(org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException e1){
				OUT.add(new BufferedOutputStream(dfs.append(fileName, 4096,
						null, null)));
			}
			

		}
		send(DFS, OUT, fileName);
	}

	public void send(ArrayList<DFSClient> DFS,
			ArrayList<BufferedOutputStream> OUT, String fileName)
			throws UnresolvedLinkException, IOException {
		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", remoteHdfs);
		DFSClient client;
		try {
			client = new DFSClient(new URI(conf.get("fs.defaultFS")), conf);

			DFSInputStream in = client.open(fileName);
			byte[] bytes = new byte[16 * 1024];

			int count;
			while ((count = in.read(bytes)) > 0) {
				for (BufferedOutputStream out : OUT) {
					out.write(bytes, 0, count);
				}
			}
			in.close();
			for (BufferedOutputStream out : OUT) {
				out.close();
			}
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}