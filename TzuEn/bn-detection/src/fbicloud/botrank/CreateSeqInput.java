package fbicloud.botrank;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;


public class CreateSeqInput {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		FileSystem fileSys = null;
		try {
			fileSys = FileSystem.get(conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		for (int i = 0; i < Integer.parseInt(args[0]) ; ++i) {
			//Creating input file for init
			Path file = new Path(args[1], "part-" + String.format("%05d", i));
			SequenceFile.Writer writer = null;
			try {
				writer = SequenceFile.createWriter(fileSys, conf, file,
						LongWritable.class, LongWritable.class,
						CompressionType.NONE);
			} catch (Exception e) {
				System.out.println("Exception while instantiating writer");
				e.printStackTrace();
			}

			// Generate dummy input
			LongWritable[] individual = new LongWritable[1];
			try {
				writer.append(new LongWritable(new Random().nextInt(Integer.MAX_VALUE)), new LongWritable(new Random().nextInt(Integer.MAX_VALUE)));
			} catch (Exception e) {
				System.out.println("Exception while appending to writer");
				e.printStackTrace();
			}

			try {
				writer.close();
			} catch (Exception e) {
				System.out.println("Exception while closing writer");
				e.printStackTrace();
			}
			System.out.println("Writing input for Map #" + i);
		}
	}

}