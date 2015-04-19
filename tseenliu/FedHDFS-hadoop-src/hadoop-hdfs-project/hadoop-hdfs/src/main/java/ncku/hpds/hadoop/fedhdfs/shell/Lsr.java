package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Lsr {
	
	public static void printFilesRecursively(String Uri, Configuration conf)
			throws IOException {
		try {

			FileSystem FS = FileSystem.get(URI.create(Uri), conf);
			FileStatus[] status = FS.listStatus(new Path(Uri));

			for (int i = 0; i < status.length; i++) {
				if (status[i].isDirectory()) {
					show.DirFileInfo(status[i].getPath().toString(), conf);
					printFilesRecursively(status[i].getPath().toString(), conf);

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
}

class show {
	
	public static void DirFileInfo(String Uri, Configuration conf) throws IOException {

		FileSystem FS = FileSystem.get(URI.create(Uri), conf);
		Path Path = new Path(Uri);
		FileStatus fileStatus = FS.getFileStatus(Path);

		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		System.out.println(fileStatus.getPermission().toString()
				+ String.format("%4d", fileStatus.getReplication())
				+ " "
				+ fileStatus.getOwner()
				+ " "
				+ fileStatus.getGroup()
				+ String.format("%11d", fileStatus.getLen())
				+ " "
				+ f.format(new Timestamp(fileStatus.getModificationTime()))
				+ " " + fileStatus.getPath().getName());
	}
}
