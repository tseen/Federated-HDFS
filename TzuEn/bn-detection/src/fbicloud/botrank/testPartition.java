package fbicloud.botrank;
import java.io.*;
import java.util.*;
import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.DecimalFormat;

import ncku.hpds.fed.MRv2.HdfsWriter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.JobConf;

public class testPartition extends Configured implements Tool{
	static String[] centerPoints = new String[8];

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = new Configuration();
			FileSystem filesystem = FileSystem.get(conf);
			Path center = new Path("/user/hpds/center");
			BufferedReader br = new BufferedReader(new InputStreamReader(
					filesystem.open(center)));
			String line;
			// ArrayList<Double> center = null;
			int i = 0;
			while ((line = br.readLine()) != null) {
				String input[] = line.split("\t");
				centerPoints[i] = input[1];
				i++;
			}
			br.close();

		}
		boolean i = true;
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			double[] row = parseStringToVector(line);
			double minDistance = Double.MAX_VALUE;
			String minID = "";
			String id = "";
			double[] point = new double[10];
			double currentDistance = 0;
		
				context.write(new Text(line), new Text(line));
		
		}

		private double distance(double[] d1, double[] d2) {
			double distance = 0;
			int len = d1.length < d2.length ? d1.length : d2.length;

			for (int i = 0; i < 8; i++) {
				distance += (d1[i] - d2[i]) * (d1[i] - d2[i]);
			}
			return Math.sqrt(distance);
		}
	}

	private static double[] parseStringToVector(String line) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens();
			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {

				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
			return row;
		} catch (Exception e) {
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int size = tokenizer.countTokens();
			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
			return row;
		}
	}

	private static void accumulate(double[] sum, double[] array) {
		for (int i = 0; i < sum.length; i++)
			sum[i] += array[i];
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			double nodecount = 0;
			int counter = 0;
			double[] sum = { 0, 0 ,0 ,0 ,0 ,0 ,0 ,0 };
			NumberFormat formatter = new DecimalFormat("#0.00");
			String value = "";
			// String bonus = "";
			for (Text v : values) {
				value = v.toString();
				String features[] = value.split(",");
				for (int i = 0; i < features.length; i++) {
					sum[i] += Double.parseDouble(features[i]);
				}
				counter++;
			}
			String result = "";
			for (int i = 0; i < sum.length; i++) {
				sum[i] = sum[i] / counter;
				sum[i] = round(sum[i], 2);
				// result += formatter.format(sum[i]);
				result += (sum[i]);
				if (i + 1 != sum.length)
					result += (",");
			}
			context.write(key, new Text(result));
	
		}

		double round(double v, int scale) {
			if (scale < 0) {
				throw new IllegalArgumentException(
						"The scale must be a positive integer or zero");
			}
			BigDecimal b = new BigDecimal(Double.toString(v));
			BigDecimal one = new BigDecimal("1");
			return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
		}

	}

	

	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;

		Job job = Job.getInstance(conf, "testPartition6");
		job.setJarByClass(testPartition.class);
		job.setJobName("testPartition");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// job.setCombinerClass(Combiner.class);
		// job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new testPartition(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}
}
