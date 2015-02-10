/*package ncku.hpds.hadoop.fedhdfs;

import java.net.URI;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


public class SuperNamenode {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String uri1 = args[0];
		String uri2 = args[1];
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri1), conf);
		
		FSDataInputStream in = null;
		System.out.println("\n");
		System.out.println("====================================================================================");
		System.out.println("==============================GlobalNamespace-Cluter1===============================");
		System.out.println("====================================================================================");
		try {
			in = fs.open(new Path(uri1));
			IOUtils.copyBytes(in, System.out, 4096, false);

		} catch (Exception e) {
			IOUtils.closeStream(in);
		}
		
		
		System.out.println("\n");
		System.out.println("====================================================================================");
		System.out.println("==============================GlobalNamespace-Cluter2===============================");
		System.out.println("====================================================================================");
		try{
			
			Path dirPath = new Path(uri2);
			FileStatus[] files = fs.listStatus(dirPath);			
			
			//FileSystem fs2 = FileSystem.get(URI.create(uri2), conf);
			
			System.out.println(files.length);
			
			for (int i=0;i<=files.length;i++){
				FileStatus stat = files[i];
			     
				System.out.println(stat.getPath().getName());	
			}
			
			
		
        }catch(Exception e){
            System.out.println("File not found");
        }
		
	}
}*/



package ncku.hpds.hadoop.fedhdfs;

import java.net.URI;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


public class SuperNamenode {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String uri = args[0];
		String uri1 = args[1];
	
		

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		System.out.println("\n");
		System.out.println("====================================================================================");
		System.out.println("==============================GlobalNamespace-Cluter1===============================");
		System.out.println("====================================================================================");
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);

		} catch (Exception e) {
			IOUtils.closeStream(in);
		}
		
		
		System.out.println("\n");
		System.out.println("====================================================================================");
		System.out.println("==============================GlobalNamespace-Cluter2===============================");
		System.out.println("====================================================================================");
		try{
			Path dirPath = new Path(uri1);
			FileStatus[] files = fs.listStatus(dirPath);
			
			/*for (int i=0;i<=files.length;i++){
			 System.out.println(files[i].getPath().getName());
			}*/
			
			for (FileStatus filelist : files)
			{
		     System.out.println(filelist.getPath().getName());
			}
			
		} catch(Exception e){
			IOUtils.closeStream(in);
		}
		
	}
}





 




 


