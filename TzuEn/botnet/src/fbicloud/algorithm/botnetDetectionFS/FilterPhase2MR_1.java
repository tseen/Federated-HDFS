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


public class FilterPhase2MR_1 extends Configured implements Tool
{
	public static class job1Mapper extends Mapper <LongWritable, Text, Text, IntWritable>
	{
		// map intermediate key and value
		private Text interKey = new Text() ;
		private IntWritable interValue = new IntWritable() ;
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// value :																					index
			// timestamp    Protocol    SrcIP:SrcPort>DstIP:DstPort										0~2
			// S2D_noP    S2D_noB    S2D_Byte_Max    S2D_Byte_Min    S2D_Byte_Mean						3~7
			// D2S_noP    D2S_noB    D2S_Byte_Max    D2S_Byte_Min    D2S_Byte_Mean						8~12
			// ToT_noP    ToT_noB    ToT_Byte_Max    ToT_Byte_Min    ToT_Byte_Mean    ToT_Byte_STD		13~18
			// ToT_Prate    ToT_Brate    ToT_ToT_BTransferRatio    DUR    Loss							19~23
			
			
			String srcInfo, srcIP ;
			String dstInfo, dstIP ;
			String line = value.toString() ;
			String[] str = line.split("\t") ;
			
			if ( str.length == 24 )
			{
				srcInfo = str[2].split(">")[0] ; // SrcIP:SrcPort
				dstInfo = str[2].split(">")[1] ; // DstIP:DstPort
				srcIP = srcInfo.split(":")[0] ; // SrcIP
				dstIP = dstInfo.split(":")[0] ; // DstIP
				
				interKey.set( srcIP + ">" + dstIP ) ;
				interValue.set( Integer.parseInt(str[23]) ) ;
				context.write( interKey, interValue ) ;
			}
		}
	}
	
	public static class job1Reducer extends Reducer <Text, IntWritable, Text, DoubleWritable>
	{
		private DoubleWritable outputValue = new DoubleWritable() ;
		
		public void reduce ( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
		{	
			long noOfFG = 0 ;				// number of Flow Group(FG) 
			long noOfFGWOResponsed = 0 ;	// number of Flow Group(FG) without response
			
			for ( IntWritable val : values )
			{
				noOfFG ++ ;
				noOfFGWOResponsed += val.get();
			}
			outputValue.set( (double)noOfFGWOResponsed/noOfFG ) ;
			context.write( key, outputValue ) ;
		}
	}
	
	
	
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2 ) {
			System.out.println("FilterPhase2MR_1: <in> <out>");
			System.out.println("otherArgs.length = "+otherArgs.length);
			System.exit(2);
		}

		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	Calculate Failed Connection between any two hosts(FCBH)			*
		 *------------------------------------------------------------------*/
		
		Job job1 = Job.getInstance ( conf, "Filter 2 - job 1" ) ;
		job1.setJarByClass(FilterPhase2MR.class);
		
		job1.setMapperClass(job1Mapper.class);
		job1.setReducerClass(job1Reducer.class);
	
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		

		
		return job1.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new FilterPhase2MR_1(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit ( res ) ;
	}
}

