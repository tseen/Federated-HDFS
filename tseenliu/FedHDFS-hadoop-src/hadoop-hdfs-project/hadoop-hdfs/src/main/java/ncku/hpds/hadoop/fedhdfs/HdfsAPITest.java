package ncku.hpds.hadoop.fedhdfs;

import java.net.URI;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HdfsAPITest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String fileUri = args[0];
		String dirUri = args[1];

		// 讀取hadoop文件系统的配置
		Configuration conf = new Configuration();
		// conf.set("hadoop.job.ugi", "hadoop-user,hadoop-user");

		FileSystem fileFS = FileSystem.get(URI.create(fileUri), conf);
		FileSystem dirFS = FileSystem.get(URI.create(dirUri), conf);

		Path filePath = new Path(fileUri);
		FileStatus fileStatus = fileFS.getFileStatus(filePath);

		Path dirPath = new Path(dirUri);
		FileStatus dirStatus = dirFS.getFileStatus(dirPath);

		if (fileStatus.isDirectory() == false) {
			System.out.println("\n");
			System.out.println("====================================================================================");
			System.out.println("==============================GlobalNamespace-Cluter1===============================");
			System.out.println("====================================================================================");
			// 查看HDFS中某文件的 matadata
			System.out.println("\nTest1: The metadata of the file from HDFS.");
			System.out.println("This is a file.");
			System.out.println("file Name: " + fileStatus.getPath().getName());
			System.out.println("file Path: " + fileStatus.getPath());
			System.out.println("file Lehgth: " + fileStatus.getLen());
			System.out.println("file Modification time:"
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

		if (dirStatus.isDirectory() == true) {
			System.out.println("\n");
			System.out.println("====================================================================================");
			System.out.println("==============================GlobalNamespace-Cluter2===============================");
			System.out.println("====================================================================================");
			System.out
					.println("\nTest2: The metadata of the directory from HDFS.");
			System.out.println("This is a directory.");

			System.out.println("dir Path: " + dirStatus.getPath());
			System.out.println("dir Length: " + dirStatus.getLen());
			System.out
					.println("dir Modification time: "
							+ new Timestamp(dirStatus.getModificationTime())
									.toString());
			System.out.println("dir last Modification time: "
					+ new Timestamp(dirStatus.getAccessTime()).toString());
			System.out
					.println("dir replication: " + dirStatus.getReplication());
			System.out
					.println("dir's Blocak size: " + dirStatus.getBlockSize());
			System.out.println("dir owner: " + dirStatus.getOwner());
			System.out.println("dir group: " + dirStatus.getGroup());
			System.out.println("dir permission: "
					+ dirStatus.getPermission().toString());

			System.out.println("The file or dir of this direcroty: ");
			for (FileStatus fs : dirFS.listStatus(new Path(dirUri))) {
				System.out.println(fs.getPath());
			}
		}
		
		else{
			System.out.println(dirStatus.getPath().getName()+ " is not a directory.");
		}
		

	}

}

