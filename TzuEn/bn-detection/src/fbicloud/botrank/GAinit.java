package fbicloud.botrank;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class GAinit extends Configured implements Tool{
	public static final int LONG_BITS = 64;
	public static int LONGS_PER_ARRAY = 6000;
	public static int POPULATION = 12000;


	public static class InitialGAMapper extends
			Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {
		Random rng;

		LongWritable[] individual;

		@Override
		public void setup(Context ctx) {
			Configuration c = ctx.getConfiguration();
			rng = new Random(System.nanoTime());
			individual = new LongWritable[LONGS_PER_ARRAY];
		}

		public void map(LongArrayWritable key, LongWritable value, Context context)
		// OutputCollector<LongArrayWritable, LongWritable> oc, Reporter rep)
				throws IOException {

			for (int i = 0; i < value.get(); i++) {
				// Generate initial individual
				for (int l = 0; l < LONGS_PER_ARRAY; l++) {
					long ind = 0;
					for (int m = 0; m < LONG_BITS; m++) {
						ind = ind | (rng.nextBoolean() ? 0 : 1);
						// Don't shift for the last bit
						if (m != LONG_BITS - 1)
							ind = ind << 1;
					}
					individual[l] = new LongWritable(ind);
					// System.out.print(individual[l].get());
				}
				try {
					context.write(new LongArrayWritable(individual), new LongWritable(0));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
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
						LongArrayWritable.class, LongWritable.class,
						CompressionType.NONE);
			} catch (Exception e) {
				System.out.println("Exception while instantiating writer");
				e.printStackTrace();
			}

			// Generate dummy input
			LongWritable[] individual = new LongWritable[1];
			individual[0] = new LongWritable(LONGS_PER_ARRAY);
			try {
				writer.append(new LongArrayWritable(individual), new LongWritable(
						6000));
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
			System.out.println("Writing dummy input for Map #" + i);
		}

		Job job = Job.getInstance(conf, "GAinit");
		job.setJarByClass(GAinit.class);
		job.setJobName("GAinit");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(LongArrayWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(InitialGAMapper.class);
		// jobConf.setReducerClass(IdentityReducer.class);
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GAinit(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}


}
