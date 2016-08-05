package fbicloud.botrank;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileStatus ;
import org.apache.hadoop.fs.FileSystem ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.NullWritable ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs ;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.util.Tool ;
import org.apache.hadoop.util.ToolRunner ;


public class GetDetectedIPs extends Configured implements Tool
{
	public static class GetDetectedIPsMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		private HashSet<String> detectedIPs = new HashSet<String>() ;
		
		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		// test
		private int count = 0 ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// value :										index
			// FVID    protocol,G#    feature vectors		0~2
			// srcIPs    dstIPs								3~4
			
			
			String line = value.toString() ;
			String[] subLine = line.split ("\t") ;
			String[] srcIP = subLine[3].split (",") ;
			
			for ( String ip : srcIP )
			{
				detectedIPs.add ( ip ) ;
				count ++ ;
			}
		}
		
		public void cleanup ( Context context ) throws IOException, InterruptedException
		{
			for ( String ip : detectedIPs )
			{
				interKey.set ( "key" ) ;
				interValue.set ( ip ) ;
				context.write ( interKey, interValue ) ;
			}
			// test
			System.out.println ( "count = " + count ) ;
			System.out.println ( "detectedIPs.size() = " + detectedIPs.size() ) ;
		}
	}
	
	public static class GetDetectedIPsReducer extends Reducer <Text, Text, NullWritable, Text>
	{
		private Set<String> detectedIPs = new TreeSet<String>() ;
		
		// reduce output value
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			for ( Text val : values )
			{
				detectedIPs.add ( val.toString() ) ;
			}
		}
		
		public void cleanup ( Context context ) throws IOException, InterruptedException
		{
			for ( String ip : detectedIPs )
			{
				outputValue.set ( ip ) ;
				context.write ( NullWritable.get(), outputValue ) ;
			}
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		
		if ( otherArgs.length != 2 )
		{
			System.out.println ( "GetDetectedIPs: <GroupPhase2MR_out> <DetectedIPs>" ) ;
			System.out.println ( "otherArgs.length = " + otherArgs.length ) ;
			System.exit (2) ;
		}
		
		Job job = Job.getInstance ( conf, "Get detected IP" ) ;
		job.setJarByClass ( GetDetectedIPs.class ) ;
		
		job.setMapperClass ( GetDetectedIPsMapper.class ) ;
		job.setReducerClass ( GetDetectedIPsReducer.class ) ;
		
		job.setMapOutputKeyClass ( Text.class ) ;
		job.setMapOutputValueClass ( Text.class ) ;
		
		job.setOutputKeyClass ( NullWritable.class ) ;
		job.setOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job, new Path ( args[0] ) ) ;
		FileOutputFormat.setOutputPath ( job, new Path ( args[1] ) ) ;
		
		job.setNumReduceTasks (1) ;
		
		return job.waitForCompletion (true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GetDetectedIPs(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}
}
