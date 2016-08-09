package ncku.hpds.hadoop.fedhdfs.shell;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class InfoAggreration {

	private ArrayList<Long> tmpFsLenElement = new ArrayList<Long>();
	private ArrayList<Short> tmpReplica = new ArrayList<Short>();
	
	public void recursivelySumOfLen(String Uri, Configuration conf) throws IOException {
		
		try {

			FileSystem FS = FileSystem.get(URI.create(Uri), conf);
			FileStatus[] status = FS.listStatus(new Path(Uri));

			for (int i = 0; i < status.length; i++) {
				if (status[i].isDirectory()) {
					//show.DirFileInfo(status[i].getPath().toString(), conf);
					recursivelySumOfLen(status[i].getPath().toString(), conf);

				} else {
					try {
						FileStatus fileStatus = FS.getFileStatus(new Path(status[i].getPath().toString()));
						tmpFsLenElement.add(fileStatus.getLen());
					} catch (Exception e) {
						System.err.println(e.toString());
					}
				}
			}
		} catch (IOException e) {
			
		}
	}
	
	public void maxOfReplica(String Uri, Configuration conf) throws IOException {
		
		try {

			FileSystem FS = FileSystem.get(URI.create(Uri), conf);
			FileStatus[] status = FS.listStatus(new Path(Uri));

			for (int i = 0; i < status.length; i++) {
				if (status[i].isDirectory()) {
					//show.DirFileInfo(status[i].getPath().toString(), conf);
					recursivelySumOfLen(status[i].getPath().toString(), conf);

				} else {
					try {
						FileStatus fileStatus = FS.getFileStatus(new Path(status[i].getPath().toString()));
						tmpReplica.add(fileStatus.getReplication());
					} catch (Exception e) {
						System.err.println(e.toString());
					}
				}
			}
		} catch (IOException e) {
			
		}
	}
	
	public long getSumOfLen(){
		
		long sumOfLen = 0;
		for (int i = 0; i < tmpFsLenElement.size(); i++) {
			sumOfLen +=  tmpFsLenElement.get(i);
		}
		return sumOfLen;
	}
	
	public short getMaxOfReplica() throws IOException{
		
		short maxOfReplica = 0;
		if (tmpReplica.isEmpty()) {
			return 0;
		}
		else {
			maxOfReplica = Collections.max(tmpReplica);
			return maxOfReplica;
		}
	}
	
	public void show(){
		
		for ( long Len : tmpFsLenElement ) { System.out.println(Len); }
	}
}
