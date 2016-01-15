package fbicloud.algorithm.botnetDetectionFS;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FilterPhase2MR_2 extends Configured implements Tool
{
	
	
	public static class job2Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable>{
		private Text interKey = new Text();
		private DoubleWritable interValue = new DoubleWritable();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input Format :
			//		SrcIP>DstIP FCBH(Failed Connection between hosts)
			//Input Example:
			//		140.116.1.1>61.64.19.192	0.6742

			String srcIP;
			String line = value.toString();
			String[] str = line.split("\t");
			if(str.length == 2){
				srcIP = str[0].split(">")[0];
				interKey.set(srcIP);
				interValue.set(Double.parseDouble(str[1]));
				context.write(interKey,interValue);
			}
		}
	}
	
	public static class job2Reducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		private double flowlossratio=0;
		private MultipleOutputs<Text, DoubleWritable> mos;
		private DoubleWritable outputValue = new DoubleWritable();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			//Multiple output Directory
			mos = new MultipleOutputs<Text, DoubleWritable>(context);
			flowlossratio = Double.parseDouble(config.get("flowlossratio"));//Hadoop will parse the -D flowlossratio=xxx(ms) and set the flowlossratio value ,and then call run()
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {	
			//Set output folder
			//output
			//		LowFCIP
			//				IP-m-00000
			//				IP-m-00001
			//		HighFCIP
			//				IP-m-00000
			//				IP-m-00001
			long noOfdstIPs = 0;		//number of destination IPs
			double sumOfFC = 0;			//Total Flow Loss Rate
			String LowFCIP_OutFileName = "LowFCIP/IP";
			String HighFCIP_OutFileName = "HighFCIP/IP";

			for(DoubleWritable val : values){
				noOfdstIPs++;
				sumOfFC += val.get();
			}

			outputValue.set(sumOfFC/noOfdstIPs);
			if(sumOfFC/noOfdstIPs > flowlossratio)
				mos.write("HighFCIP", key, outputValue, HighFCIP_OutFileName);
			else
				mos.write("LowFCIP", key, outputValue, LowFCIP_OutFileName);
		}
	}
	
	
	
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2 ) {
			System.out.println("FilterPhase2MR_2: <in> <out>");
			System.out.println("otherArgs.length = "+otherArgs.length);
			System.exit(2);
		}

		
		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Calculate Failed Connection for each host						*
		 *------------------------------------------------------------------*/

		Job job2 = Job.getInstance ( conf, "Filter 2 - job 2" ) ;
		job2.setJarByClass(FilterPhase2MR.class);
		
		job2.setMapperClass(job2Mapper.class);
		job2.setReducerClass(job2Reducer.class);
	
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job2, "HighFCIP", TextOutputFormat.class, Text.class, DoubleWritable.class);
		MultipleOutputs.addNamedOutput(job2, "LowFCIP", TextOutputFormat.class, Text.class, DoubleWritable.class);
		
		job2.waitForCompletion(true);
		
		
		
		return job2.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new FilterPhase2MR_2(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit ( res ) ;
	}
}
