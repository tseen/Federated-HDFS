package fbicloud.botrank;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class FVGraphMR_2 extends Configured implements Tool{
	public static class job2Mapper extends Mapper <LongWritable, Text, Text , Text>
	{
		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// FVID1,FVID2    [initial score]
			
			String line = value.toString() ;
			String[] subLine = line.split ( "\t" ) ;
			String[] fvid = subLine[0].split(",");
			
			if ( subLine.length == 2 )
			{
				interKey.set ( fvid[0] ) ;
				interValue.set ( fvid[1] ) ;
				context.write ( interKey, interValue ) ;
				
				//interKey.set ( fvid[1] ) ;
				//interValue.set ( fvid[0] ) ;
				//context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class job2Reducer extends Reducer <Text, Text, Text, Text>
	{
		// reduce output key, value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			//Input Format :
			//		Key		: 	FVID1
			//		Value	:	FVID2
			
			// TreeSet : ordered set
			Set<String> FVSet = new TreeSet<String>() ;
			StringBuffer outsb = new StringBuffer() ;
			
			for ( Text val : values )
			{
				FVSet.add ( val.toString() ) ;
			}
			for ( String FV : FVSet )
			{
				outsb.append ( FV + "," ) ;
			}
			
			outputKey.set ( key ) ;
			outputValue.set ( outsb.toString() ) ;
			context.write ( outputKey, outputValue ) ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;

		//conf.set ( "MaxMinFV", args[0] + "_MaxMinFV" ) ;
		//conf.set ( "FGInfo", args[1] ) ;
		
		
		
		//return job1.waitForCompletion(true) ? 0 : 1 ;
		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Get Adjacency list												*
		 *------------------------------------------------------------------*/
		
		Job job2 = Job.getInstance ( conf, "FVGraph - job2" ) ;
		job2.setJarByClass ( FVGraphMR_2.class ) ;
		
		job2.setMapperClass ( job2Mapper.class ) ;
		job2.setReducerClass ( job2Reducer.class ) ;
		
		job2.setMapOutputKeyClass ( Text.class ) ;
		job2.setMapOutputValueClass ( Text.class ) ;
		
		job2.setOutputKeyClass ( Text.class ) ;
		job2.setOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job2, new Path ( args[0] )) ;
		FileOutputFormat.setOutputPath ( job2, new Path ( args[1] ) ) ;
		
		return job2.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new FVGraphMR_2(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}

