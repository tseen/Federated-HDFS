package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class Ls {
	
	public static void printFiles(String Uri, Configuration conf, String hostName) throws IOException {
		
		System.out.println("=============================GlobalNamespace-Cluster:" + hostName + "============================");
		try {

			FileSystem FS = FileSystem.get(URI.create(Uri), conf);
			FileStatus[] status = FS.listStatus(new Path(Uri));

			for (int i = 0; i < status.length; i++) {
				if (status[i].isDirectory()) {
					show.DirFileInfo(status[i].getPath().toString(), conf);
				} else {
					try {
						show.DirFileInfo(status[i].getPath().toString(), conf);
					} catch (Exception e) {
						System.err.println(e.toString());
					}
				}
			}
		} catch (IOException e) {
		}
	}
	
	public static void print_info(String Uri, Configuration conf, String hostName) throws IOException {

		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);

		if (fileStatus.isDirectory() == false) {
			System.out.println("\n");
			System.out
					.println("====================================================================================");
			System.out
					.println("=============================GlobalNamespace-Cluster:" + hostName + "============================");
			System.out
					.println("====================================================================================");

			System.out.println("\nThe metadatargs.lengtha of the file from HDFS.");
			System.out.println("This is a file.");
			System.out.println("file Name: " + fileStatus.getPath().getName());
			System.out.println("file Path: " + fileStatus.getPath());
			System.out.println("file Lehgth: " + fileStatus.getLen());
			System.out.println("file Modification time: "
					+ new Timestamp(fileStatus.getModificationTime())
							.toString());

			System.out.println("file last Modification time: "
					+ new Timestamp(fileStatus.getAccessTime()).toString());

			System.out.println("file replication: "
					+ fileStatus.getReplication());
			System.out.println("file's Block size: "
					+ fileStatus.getBlockSize());
			System.out.println("file owner: " + fileStatus.getOwner());
			System.out.println("file group: " + fileStatus.getGroup());
			System.out.println("file permission: "
					+ fileStatus.getPermission().toString());
			System.out.println();
		}

		else if (fileStatus.isDirectory() == true) {
			System.out.println("\n");
			System.out
					.println("====================================================================================");
			System.out
					.println("=============================GlobalNamespace-Cluster:" + hostName + "============================");
			System.out
					.println("====================================================================================");
			System.out
					.println("\nThe metadata of the directory from HDFS.");
			System.out.println("This is a directory.");

			System.out.println("dir Path: " + fileStatus.getPath());
			System.out.println("dir Length: " + fileStatus.getLen());
			System.out.println("dir Modification time: "
					+ new Timestamp(fileStatus.getModificationTime())
							.toString());

			System.out.println("dir last Modification time:"
					+ new Timestamp(fileStatus.getAccessTime()).toString());

			System.out.println("dir replication: "
					+ fileStatus.getReplication());
			System.out.println("dir's Blocak size: "
					+ fileStatus.getBlockSize());
			System.out.println("dir owner: " + fileStatus.getOwner());
			System.out.println("dir group: " + fileStatus.getGroup());
			System.out.println("dir permission: "
					+ fileStatus.getPermission().toString());

			System.out.println("The file or dir of this direcroty: ");
			for (FileStatus fs : FS.listStatus(new Path(Uri))) {
				System.out.println(fs.getPath());
			}

		}
	}
	
	public static String getPath(String Uri, Configuration conf) throws IOException {
		
		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);
		return fileStatus.getPath().getPathWithoutSchemeAndAuthority(Path).toString();
	}
	
	public static long getLen(String Uri, Configuration conf) throws IOException {
		
		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);
		return fileStatus.getLen();
	}
	
	public static long getModificationTime(String Uri, Configuration conf) throws IOException {
		
		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);
		return fileStatus.getModificationTime();
	}
	
	public static short getReplication(String Uri, Configuration conf) throws IOException {
		
		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);
		return fileStatus.getReplication();
	}
	
	public static String getOwner(String Uri, Configuration conf) throws IOException {
		
		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);
		return fileStatus.getOwner();
	}
	
	public static String getGroup(String Uri, Configuration conf) throws IOException {
		
		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);
		return fileStatus.getGroup();
	}
	
	public static String getPermission(String Uri, Configuration conf) throws IOException {
		
		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);
		return fileStatus.getPermission().toString();
	}
}
