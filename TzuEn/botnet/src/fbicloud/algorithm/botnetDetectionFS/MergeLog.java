package fbicloud.algorithm.botnetDetectionFS; 

import fbicloud.algorithm.botnetDetectionFS.MergeLogSecondarySort.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import ncku.hpds.fed.MRv2.FedJobConf;
import ncku.hpds.fed.MRv2.ProxySelector.ProxyMapperVLongLong;
import ncku.hpds.fed.MRv2.proxy.GenericProxyMapper;
import ncku.hpds.fed.MRv2.proxy.GenericProxyReducer;



//org.apache.hadoop.mapreduce.x -> Map Reduce 2.0
//org.apache.hadoop.mapred.x -> Map Reduce 1.0
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MergeLog extends Configured implements Tool
{
	public static class RouterLogMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// a HashSet to store remove IP list
		private Set<String> removeIPlist = new HashSet<String>() ;
		
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// netflow attribute
		private long timestamp ; // the time flow start
		private long duration ; // the flow duration
		
		// set map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			// Read File from HDFS -- remove ip list : remove the src IPs in the collected logs
			Path path = new Path( "removeIPlist" ) ;
			FileSystem fs = path.getFileSystem( config ) ;
			BufferedReader br = new BufferedReader( new InputStreamReader(fs.open(path)) ) ;
			line = br.readLine() ;
			while ( line != null )
			{
				removeIPlist.add( line ) ;
				line = br.readLine() ;
			}
			br.close() ;
			// test
			//System.out.println( "removeIPlist size = " + removeIPlist.size() ) ;
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// LongWritable
			// value :																	index	used
			// "unix_secs"    "unix_nsecs"    sysUpTime									0~2		0,1
			// exaddr    "srcaddr"    "dstaddr"    nexthop    input    output			3~8		4,5
			// "dPkts"    "dOctets"    "First"    "Last"    "srcport"    "dstport"		9~14	9~14
			// "prot"    tos    tcp_flags    engine_type    engine_id					15~19	15
			// src_mask    dst_mask    src_as    dst_as									20~23	
			
			
			line = value.toString() ;
			subLine = line.split("\\s+") ; // split by one or more "space"
			
			
			// subLine[4] : src IP , subLine[5] : dst IP
			if ( ! removeIPlist.contains( subLine[4] ) && ! removeIPlist.contains( subLine[5] ) )
			{
				// unix_secs * 1000 + unix_nsecs / 1000000 , unit : millisecond
				timestamp = ( Long.parseLong(subLine[0]) * 1000 ) + ( Long.parseLong(subLine[1]) / 1000000 ) ;
				// unit : millisecond
				duration = Long.parseLong(subLine[12]) - Long.parseLong(subLine[11]) ;
				// test
				//System.out.println( "timestamp = " + timestamp ) ;
				//System.out.println( "duration = " + duration ) ;
				
				
				comKey.setSameKey( "netflow" ) ;
				comKey.setTime( timestamp ) ;
				
				interValue.set( String.valueOf(timestamp) + "\t" + String.valueOf(duration) + "\t" +
								subLine[15] + "\t" + subLine[4] + "\t" + subLine[13] + "\t" +
								subLine[5] + "\t" + subLine[14] + "\t" +
								subLine[9] + "\t" + subLine[10] ) ;
				
				context.write( comKey, interValue ) ;
				// output format
				// value :
				// timestamp    duration
				// protocol    srcIP    srcPort
				// dstIP    dstPort
				// packets    bytes
				
				// test
				//System.out.println( "interValue = " + interValue.toString() ) ;
			}
		}
	}
	
	
	public static class CollectLogMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// the time difference between RouterLog and CollectLog
		private long diffTime ;
		
		// input value reader
		private String line ;
		private String[] subLine ;
		private String[] srcInfo ;
		private String[] dstInfo ;
		
		// nfdump (pcap -> netflow) attribute
		private long timestamp ;
		private long duration ;
		
		// set map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			// Get the parameter from -D command line option
			String pcapInitialTime = config.get( "pcapInitialTime" ) ;
			
			long pcapTime = 0L ;
			long netflowTime = 0L ;
			
			try
			{
				SimpleDateFormat pattern = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS") ;
				Date date = pattern.parse( pcapInitialTime ) ;
				pcapTime = date.getTime() ;
				
				netflowTime = Long.parseLong( config.get("netflowTime") ) ;
				// pcapTime, netflowTime, diffTime => unit : millisecond
				diffTime = Math.abs( pcapTime - netflowTime ) ;
				
				// test
				//System.out.println( "pcapTime = " + pcapTime ) ;
				//System.out.println( "netflowTime = " + netflowTime ) ;
				//System.out.println( "diffTime = " + diffTime ) ;
			}
			catch ( Exception e )
			{
				System.out.println("Error:" + e.getMessage());
				System.out.println("pcapInitialTime = " + pcapInitialTime);
				diffTime=Long.MAX_VALUE;
			}
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :															index
			// Date    flow start    Duration    Protocol						0~3
			// Src IP Addr:Port    ->    Dst IP Addr:Port    Flags    Tos		4~8
			// Packets    Bytes    Flows										9~11
			
			
			line = value.toString() ;
			subLine = line.split("\\s+") ;
			srcInfo = subLine[4].split(":") ;
			dstInfo = subLine[6].split(":") ;
			
			
			long pcapTime = 0L ;
			
			
			// modify the pcap timestamp the same as netflow
			try
			{
				SimpleDateFormat pattern = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS") ;
				Date date = pattern.parse( subLine[0] + "_" + subLine[1] ) ;
				pcapTime = date.getTime() ;
				// pcapTime, diffTime, timestamp => unit : millisecond
				timestamp = pcapTime - diffTime ;
				
				// test
				//System.out.println( "pcapTime = " + pcapTime ) ;
				//System.out.println( "diffTime = " + diffTime ) ;
				//System.out.println( "timestamp = " + timestamp ) ;
			}
			catch ( Exception e )
			{
				System.out.println(key.toString() + "\t" +"Error:"+e.getMessage());
				System.out.println(key.toString() + "\t" +"date = "+ subLine[0]+"_"+subLine[1]);
			}
			
			// duration : covert second to millisecond
			duration = (long) (Float.parseFloat(subLine[2]) * 1000) ;
			// test
			//System.out.println( "duration = " + duration ) ;
			
			
			comKey.setSameKey( "netflow" ) ;
			comKey.setTime( timestamp ) ;
			
			interValue.set( String.valueOf(timestamp) + "\t" + String.valueOf(duration) + "\t" +
							subLine[3] + "\t" + srcInfo[0] + "\t" + srcInfo[1] + "\t" +
							dstInfo[0] + "\t" + dstInfo[1] + "\t" +
							subLine[9] + "\t" + subLine[10] ) ;
			
			context.write( comKey, interValue ) ;
			// output format
			// value :
			// timestamp    duration
			// protocol    srcIP    srcPort
			// dstIP    dstPort
			// packets    bytes
			
			// test
			//System.out.println( "interValue = " + interValue.toString() ) ;
		}
	}
	
	
	public static class MergeLogReducer extends Reducer <CompositeKey, Text, NullWritable, Text>
	{
		// reduce output value
		private Text outputValue = new Text() ;
		
		public void reduce ( CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{	
			for ( Text val : values )
			{
				outputValue.set( val ) ;
				context.write( NullWritable.get(), outputValue ) ;
				
				// test
				//System.out.println( "outputValue = " + outputValue.toString() ) ;
				// output format
				// value :
				// timestamp    duration
				// protocol    srcIP    srcPort
				// dstIP    dstPort
				// packets    bytes
			}
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		if ( otherArgs.length != 3 )
		{
			System.out.println( "MergeLog: <routerLog> <collectLog> <MergeOut>" ) ;
			System.out.println( "otherArgs.length = " + otherArgs.length ) ;
			System.exit(2) ;
		}
		
		// merge the router log(netflow) and collected log(pcap -> netflow) into one file
		// and sort by timestamp
		Job job = Job.getInstance( conf, "Merge Log" ) ;
		job.setJarByClass(MergeLog.class) ;
		
		job.setMapOutputKeyClass(CompositeKey.class) ;
		job.setMapOutputValueClass(Text.class) ;
		job.setOutputKeyClass(NullWritable.class) ;
		job.setOutputValueClass(Text.class) ;
		
		MultipleInputs.addInputPath( job, new Path(args[0]), TextInputFormat.class, RouterLogMapper.class) ;
		MultipleInputs.addInputPath( job, new Path(args[1]), TextInputFormat.class, CollectLogMapper.class) ;
		job.setReducerClass(MergeLogReducer.class) ;
		FileOutputFormat.setOutputPath( job, new Path(args[2]) ) ;
		job.setKeyValueReduceClass(CompositeKey.class, Text.class, userDefineReducer.class);
		job.setKeyValueMapClass(CompositeKey.class, Text.class, userDefineMapper.class);
		
		job.setPartitionerClass(KeyPartitioner.class) ;
		job.setSortComparatorClass(CompositeKeyComparator.class) ;
		job.setGroupingComparatorClass(KeyGroupingComparator.class) ;
		
		job.setNumReduceTasks(1) ; // set only one reduce
		
		return job.waitForCompletion(true) ? 0 : 1 ;
	}
	public static class userDefineMapper extends GenericProxyMapper <CompositeKey,Text>{
		public userDefineMapper() throws Exception { super(CompositeKey.class,Text.class); }
		 @Override
			public void stringToKey(String in, CompositeKey key){
			 	System.out.println("MerLog key:"+ in );
				String k[] = in.split(",");
				key.setSameKey(k[0]);
				key.setTime(Long.parseLong(k[1]));
			}
	}
	public static class userDefineReducer extends GenericProxyReducer <CompositeKey,Text>{
		public userDefineReducer() throws Exception { super(CompositeKey.class,Text.class); }
	   
	}
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run( new Configuration(), new MergeLog(), args ) ;
		// Run the class MergeLog() after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}
