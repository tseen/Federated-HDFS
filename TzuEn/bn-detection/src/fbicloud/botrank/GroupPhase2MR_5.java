package fbicloud.botrank;


import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GroupPhase2MR_5 extends Configured implements Tool
{

	public static class AssignFVIDMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		// length of line
		private final static int lengthOfLine = 4 ;
		
		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// value :												index
			// protocol,G#    srcIPs    dstIPs    20 features		0~3
			
			
			String line = value.toString() ;
			String[] subLine = line.split ("\t") ;
			
			String protocol_gid = subLine[0] ;
			String srcIPs = subLine[1] ;
			String dstIPs = subLine[2] ;
			String features = subLine[3] ;
			
			if ( subLine.length == lengthOfLine )
			{
				interKey.set ( "Key" ) ;
				interValue.set ( protocol_gid + "\t" + srcIPs + "\t" + dstIPs + "\t" + features ) ;
				context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class AssignFVIDReducer extends Reducer <Text, Text, NullWritable, Text>
	{
		private String tag ;
		
		// reduce output value
		private Text outputValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			tag = config.get( "tag", "None" ) ;
			// test
			//System.out.println ( "tag = " + tag ) ;
		}
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// value :												index
			// protocol,G#    srcIPs    dstIPs    20 features		0~3
			
			
			int fvID = 0 ;
			
			for ( Text val : values )
			{
				fvID ++ ;
				String[] subLine = val.toString().split("\t") ;
				
				String protocol_gid = subLine[0] ;
				String srcIPs = subLine[1] ;
				String dstIPs = subLine[2] ;
				String features = subLine[3] ;
				
				outputValue.set ( tag + "-" + String.valueOf(fvID) + "\t" + 
								  protocol_gid + "\t" + srcIPs + "\t" + dstIPs + "\t" + features ) ;
				context.write ( NullWritable.get(), outputValue ) ;
			}
		}
	}
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		/*------------------------------------------------------------------*
		 *								Job 5		 						*
		 *	Assign Feature Vector ID										*
		 *------------------------------------------------------------------*/
		
		Job job5 = Job.getInstance(conf, "Group 2 - job5");
		job5.setJarByClass(GroupPhase2MR_5.class);
		
		job5.setMapperClass(AssignFVIDMapper.class);
		job5.setReducerClass(AssignFVIDReducer.class);
		
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);
		
		job5.setOutputKeyClass(NullWritable.class);
		job5.setOutputValueClass(Text.class);
		
		job5.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job5, new Path(args[0]));
		FileOutputFormat.setOutputPath(job5, new Path(args[1]+"_assignFVID"));
		
		return job5.waitForCompletion(true)? 0 : 1;
	}
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GroupPhase2MR_5(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}
}