package ncku.hpds.fed.MRv2;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsWriter {
	
	private String remoteHdfs;
	private String remoteUser;
	UserGroupInformation ugi;
	private String fileName;
	public OutputStream out = null;
	public DFSClient client;

	//public FSDataOutputStream out ;
	
	public HdfsWriter(String rHdfs, String rUser){
		remoteHdfs = rHdfs;
		remoteUser = rUser;
		ugi = UserGroupInformation.createRemoteUser(rUser);
		
	}
	
/*	public void writeUTF(String text){
		try {
			out.writeUTF(text);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}*/
	
	public void writeByte(byte[] in){
		try {
			out.write(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void closeWriter(){
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void init(){
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", remoteHdfs);
			client = new DFSClient(new URI(remoteHdfs), conf);
			out = new BufferedOutputStream(client.create(fileName, false));
			
		/*	ugi.doAs(new PrivilegedExceptionAction<Void>(){
				public Void run() throws Exception {
					Configuration conf = new Configuration();
					conf.set("fs.defaultFS", remoteHdfs);
					conf.set("hadoop.job.ugi", remoteUser);
					
					FileSystem fs = FileSystem.get(conf);
					
					fs.createNewFile(new Path(fileName));
					out = fs.append(new Path(fileName));
					System.out.println("APPEDN");
					return null;
				}
			});*/
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}




	public String getRemoteHdfs() {
		return remoteHdfs;
	}



	public void setRemoteHdfs(String remoteHdfs) {
		this.remoteHdfs = remoteHdfs;
	}

	public String getRemoteUser() {
		return remoteUser;
	}

	public void setRemoteUser(String remoteUser) {
		this.remoteUser = remoteUser;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

}
