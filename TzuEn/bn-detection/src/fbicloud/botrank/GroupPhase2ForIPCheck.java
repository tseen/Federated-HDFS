package fbicloud.botrank;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileSystem ;
import org.apache.hadoop.fs.FileStatus ;
import org.apache.hadoop.fs.FSDataOutputStream ;
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.NullWritable ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.util.Tool ;
import org.apache.hadoop.util.ToolRunner ;


public class GroupPhase2ForIPCheck extends Configured implements Tool
{
	public static class job1Mapper extends Mapper <LongWritable, Text, Text, Text>
	{
		// detected IP list
		private Set<String> detectedIPs = new HashSet<String>() ;
		
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// protocol and src and dst IP
		//private String prot ;
		private String ipPair ;
		private String srcIP, dstIP ;
		
		// map intermediate key and value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		// load detected IP list
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			String line ;
			FileSystem fs = FileSystem.get ( config ) ;
			FileStatus[] status = fs.listStatus ( new Path ( config.get ( "detectedIP" ) ) ) ;
			
			for ( int i = 0 ; i < status.length ; i ++ )
			{
				BufferedReader br = new BufferedReader ( new InputStreamReader ( fs.open (status[i].getPath()) ) ) ;
				
				line = br.readLine() ;
				while ( line != null )
				{
					detectedIPs.add ( line ) ;
					line = br.readLine() ;
				}
			}
			// test
			System.out.println ( "detectedIPs.size() = " + detectedIPs.size() ) ;
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																					index
			// Protocol,srcIP>dstIP,FGN:#,G#    wPER/woPER												0~1
			// S2D_noP    S2D_noB    S2D_Byte_Max    S2D_Byte_Min    S2D_Byte_Mean						2~6
			// D2S_noP    D2S_noB    D2S_Byte_Max    D2S_Byte_Min    D2S_Byte_Mean						7~11
			// ToT_noP    ToT_noB    ToT_Byte_Max    ToT_Byte_Min    ToT_Byte_Mean    ToT_Byte_STD		12~17
			// ToT_Prate    ToT_Brate    ToT_BTransferRatio    DUR										18~21
			// IAT_Max    IAT_Min    IAT_Mean    IAT_STD												22~25
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			if ( subLine.length == 26 && subLine[1].equals("wPER") ) // Only preserve wPER behavior
			{
				//prot = subLine[0].split(",")[0] ;
				ipPair = subLine[0].split(",")[1] ;
				srcIP = ipPair.split(">")[0] ;
				dstIP = ipPair.split(">")[1] ;
				
				if ( detectedIPs.contains( srcIP ) )
				{
					interKey.set ( srcIP ) ;
					interValue.set ( dstIP ) ;
					context.write ( interKey, interValue ) ;
				}
			}
		}	
	}
	
	public static class job1Reducer extends Reducer <Text, Text, NullWritable, Text>
	{
		// test
		private int src_count = 0 ;
		private int dst_count = 0 ;
		
		// reduce output value
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// test
			src_count ++ ;
			
			Set<String> dstIPs = new TreeSet<String>() ;
			
			
			for ( Text val : values )
			{
				dstIPs.add ( val.toString() ) ;
			}
			
			// write "==src IP=="
			outputValue.set ( "==" + key + "==" ) ;
			context.write ( NullWritable.get(), outputValue ) ;
			// write "dst IP"
			for ( String ip : dstIPs )
			{
				outputValue.set ( ip ) ;
				context.write ( NullWritable.get(), outputValue ) ;
				// test
				dst_count ++ ;
			}
		}
		
		public void cleanup ( Context context ) throws IOException, InterruptedException
		{
			// write last line
			outputValue.set ( "==" ) ;
			context.write ( NullWritable.get(), outputValue ) ;
			
			// test
			System.out.println ( "src_count = " + src_count ) ;
			System.out.println ( "dst_count = " + dst_count ) ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		if ( otherArgs.length < 2 )
		{
			System.out.println ( "GroupPhase2ForIPCheck: <in> <out>" ) ;
			System.exit(2) ;
		}
		
		
		conf.set ( "detectedIP", args[1] ) ;
		Job job1 = Job.getInstance ( conf, "Group 1 output -> IP check input format" ) ;
		job1.setJarByClass ( GroupPhase2ForIPCheck.class ) ;
		
		job1.setMapperClass ( job1Mapper.class ) ;
		job1.setReducerClass ( job1Reducer.class ) ;
		
		job1.setMapOutputKeyClass ( Text.class ) ;
		job1.setMapOutputValueClass ( Text.class ) ;
		
		job1.setOutputKeyClass ( NullWritable.class ) ;
		job1.setOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job1, new Path ( args[0] ) ) ;
		FileOutputFormat.setOutputPath ( job1, new Path ( args[2] ) ) ;
		
		return job1.waitForCompletion (true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GroupPhase2ForIPCheck(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}
}
