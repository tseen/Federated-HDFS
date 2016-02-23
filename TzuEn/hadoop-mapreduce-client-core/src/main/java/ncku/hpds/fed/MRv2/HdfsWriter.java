package ncku.hpds.fed.MRv2;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
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

public class HdfsWriter<K, V> {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HdfsWriter.class);
	private String remoteHdfs;
	private String remoteUser;
	private static CachePoolInfo poolInfo = new CachePoolInfo("FedJob");
	UserGroupInformation ugi;
	private String fileName;
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

	// public FSDataOutputStream out ;

	public HdfsWriter(String rHdfs, String rUser) {
		remoteHdfs = rHdfs;
		remoteUser = rUser;
		ugi = UserGroupInformation.createRemoteUser(rUser);

	}

	/*
	 * public void writeUTF(String text){ try { out.writeUTF(text); } catch
	 * (IOException e) { e.printStackTrace(); }
	 * 
	 * }
	 */
	private void writeObject(Object o) throws IOException {
		if (o instanceof Text) {
			Text to = (Text) o;
			out.write(to.getBytes(), 0, to.getLength());
		} else {
			out.write(o.toString().getBytes(utf8));
		}
	}

	public synchronized void write(K key, V value) throws IOException {

		boolean nullKey = key == null || key instanceof NullWritable;
		boolean nullValue = value == null || value instanceof NullWritable;
		if (nullKey && nullValue) {
			return;
		}
		if (!nullKey) {
			writeObject(key);
		}
		if (!(nullKey || nullValue)) {
			out.write("=".getBytes());
		}
		if (!nullValue) {
			writeObject(value);
		}
		out.write(newline);
	}

	public void writeByte(byte[] in) {
		try {
			out.write(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void closeWriter() {
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void initCache(){
		 	try {
				client.addCachePool(poolInfo);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 	CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
	        builder.setPath(new Path(fileName));
	        builder.setPool("FedJob");
	        CacheDirectiveInfo directive = builder.build();
	        EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
	        try {
				client.addCacheDirective(directive, flags);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
		
	}
	public void delete(String filename){
		try {
			client.delete(filename, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	String newFileName(String fn){
		int i = Integer.parseInt(fn.substring(fn.length() - 1));
		i += 3;
		fn = fn.substring(0,fn.length()-1) + Integer.toString(i);
		return fn;
	}
	public void init() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", remoteHdfs);
			client = new DFSClient(new URI(remoteHdfs), conf);
			boolean existFile = false;
			do{
				try{
					
					existFile = false;
					out = new BufferedOutputStream(client.create(fileName, false));
					//DistributedFileSystem dfs = getDFS(conf);
		     
				}
				catch(org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException e){
					fileName = newFileName(fileName);
					existFile = true;
				}
				catch(org.apache.hadoop.ipc.RemoteException e){
					fileName = newFileName(fileName);
					existFile = true;
				}
				catch(org.apache.hadoop.fs.FileAlreadyExistsException e){
					fileName = newFileName(fileName);
					existFile = true;
				}
			}while(existFile);
			/*catch(org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException e){
				fileName = newFileName(fileName);
				out = new BufferedOutputStream(client.create(fileName, false));
				//out = new BufferedOutputStream(client.append(fileName, 4096, null, null));
			}
			catch(org.apache.hadoop.ipc.RemoteException e){
				fileName = newFileName(fileName);
				out = new BufferedOutputStream(client.create(fileName, false));
			}
			catch(org.apache.hadoop.fs.FileAlreadyExistsException e){
				fileName = newFileName(fileName);
				out = new BufferedOutputStream(client.create(fileName, false));
			}*/
			
			/*
			 * ugi.doAs(new PrivilegedExceptionAction<Void>(){ public Void run()
			 * throws Exception { Configuration conf = new Configuration();
			 * conf.set("fs.defaultFS", remoteHdfs); conf.set("hadoop.job.ugi",
			 * remoteUser);
			 * 
			 * FileSystem fs = FileSystem.get(conf);
			 * 
			 * fs.createNewFile(new Path(fileName)); out = fs.append(new
			 * Path(fileName)); System.out.println("APPEDN"); return null; } });
			 */
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void setIterFile(String s){
		fileName = s;
	}
	public void getIterFile(String s){
		
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
	private static DistributedFileSystem getDFS(Configuration conf)
		      throws IOException {
		    FileSystem fs = FileSystem.get(conf);
		    if (!(fs instanceof DistributedFileSystem)) {
		      throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
		      " is not an HDFS file system");
		    }
		    return (DistributedFileSystem)fs;
		  }

}
