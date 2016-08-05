package fbicloud.botrank;


import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import ncku.hpds.fed.MRv2.proxy.GenericProxyMapper;
import ncku.hpds.fed.MRv2.proxy.GenericProxyReducer;

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

import fbicloud.botrank.KeyComparator.CompositeKey;
import fbicloud.botrank.KeyComparator.CompositeKeyComparator;
import fbicloud.botrank.KeyComparator.KeyGroupingComparator;
import fbicloud.botrank.KeyComparator.KeyPartitioner;


public class GroupPhase2MR_2 extends Configured implements Tool
{
	public static class GetAverageFVOfLevelTwoClusterMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// length of line
		private final static int lengthOfLine = 4 ;
		
		
		// map intermediate key
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// LongWritable
			// value :																									index
			// srcIP,protocol,G#    timestamp    dstIP																	0~2		split by "\t"
			// srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean		3
			// dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean
			// total_NumOfPkts, total_NumOfBytes, total_Byte_Max, total_Byte_Min, total_Byte_Mean, total_Byte_STD
			// total_PktsRate, total_BytesRate, total_BytesTransferRatio, duration
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			
			String srcIP_protocol = subLine[0] ;
			String timestamp = subLine[1] ;
			String dstIP = subLine[2] ;
			String features = subLine[3] ;
			
			
			if ( subLine.length == lengthOfLine )
			{
				comKey.setTuples( srcIP_protocol ) ;
				comKey.setTime( Long.parseLong( timestamp ) ) ;
				
				interValue.set( timestamp + "\t" + dstIP + "\t" + features ) ;
				context.write( comKey, interValue ) ;
			}
		}
	}
	
	public static class GetAverageFVOfLevelTwoClusterReducer extends Reducer <CompositeKey, Text, Text, Text>
	{
		// feature vectors variable
		private double srcToDst_NumOfPkts ;
		private double srcToDst_NumOfBytes ;
		private double srcToDst_Byte_Max ;
		private double srcToDst_Byte_Min ;
		private double srcToDst_Byte_Mean ;
		
		private double dstToSrc_NumOfPkts ;
		private double dstToSrc_NumOfBytes ;
		private double dstToSrc_Byte_Max ;
		private double dstToSrc_Byte_Min ;
		private double dstToSrc_Byte_Mean ;
		
		private double total_NumOfPkts ;
		private double total_NumOfBytes ;
		private double total_Byte_Max ;
		private double total_Byte_Min ;
		private double total_Byte_Mean ;
		private double total_Byte_STD ;
		
		private double total_PktsRate ;
		private double total_BytesRate ;
		private double total_BytesTransferRatio ;
		private double duration ;
		
		// input value reader
		private String[] line ;
		private String[] feature ;
		// counter
		private int count ;
		// timestamp
		private long timestamp ;
		
		// a HashSet to store dstIPs
		Set<String> dstIPSet = new HashSet<String>() ;
		
		
		// reduce output value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( CompositeKey key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// srcIP,protocol,G#
			// value :																									index
			// timestamp    dstIP																						0~1		split by "\t"
			// srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean,		0~4		split by ","
			// dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean,		5~9
			// total_NumOfPkts, total_NumOfBytes, total_Byte_Max, total_Byte_Min, total_Byte_Mean, total_Byte_STD,		10~15
			// total_PktsRate, total_BytesRate, total_BytesTransferRatio, duration										16~19
			
			
			// initialize
			srcToDst_NumOfPkts = 0d ;
			srcToDst_NumOfBytes = 0d ;
			srcToDst_Byte_Max = 0d ;
			srcToDst_Byte_Min = 0d ;
			srcToDst_Byte_Mean = 0d ;
			
			dstToSrc_NumOfPkts = 0d ;
			dstToSrc_NumOfBytes = 0d ;
			dstToSrc_Byte_Max = 0d ;
			dstToSrc_Byte_Min = 0d ;
			dstToSrc_Byte_Mean = 0d ;
			
			total_NumOfPkts = 0d ;
			total_NumOfBytes = 0d ;
			total_Byte_Max = 0d ;
			total_Byte_Min = 0d ;
			total_Byte_Mean = 0d ;
			total_Byte_STD = 0d ;
			
			total_PktsRate = 0d ;
			total_BytesRate = 0d ;
			total_BytesTransferRatio = 0d ;
			duration = 0d ;
			
			count = 0 ;
			timestamp = 0 ;
			
			dstIPSet.clear() ;
			// a string buffer to store dstIPs
			StringBuffer dstIPStr = new StringBuffer() ;
			
			
			for ( Text val : values )
			{
				count ++ ;
				line = val.toString().split("\t") ;
				
				// add dstIPs into hashset
				dstIPSet.add( line[1] ) ;
				
				feature = line[2].split(",") ;
				
				srcToDst_NumOfPkts += Double.parseDouble(feature[0]) ;
				srcToDst_NumOfBytes += Double.parseDouble(feature[1]) ;
				srcToDst_Byte_Max += Double.parseDouble(feature[2]) ;
				srcToDst_Byte_Min += Double.parseDouble(feature[3]) ;
				srcToDst_Byte_Mean += Double.parseDouble(feature[4]) ;
				
				dstToSrc_NumOfPkts += Double.parseDouble(feature[5]) ;
				dstToSrc_NumOfBytes += Double.parseDouble(feature[6]) ;
				dstToSrc_Byte_Max += Double.parseDouble(feature[7]) ;
				dstToSrc_Byte_Min += Double.parseDouble(feature[8]) ;
				dstToSrc_Byte_Mean += Double.parseDouble(feature[9]) ;
				
				total_NumOfPkts += Double.parseDouble(feature[10]) ;
				total_NumOfBytes += Double.parseDouble(feature[11]) ;
				total_Byte_Max += Double.parseDouble(feature[12]) ;
				total_Byte_Min += Double.parseDouble(feature[13]) ;
				total_Byte_Mean += Double.parseDouble(feature[14]) ;
				total_Byte_STD += Double.parseDouble(feature[15]) ;
				
				total_PktsRate += Double.parseDouble(feature[16]) ;
				total_BytesRate += Double.parseDouble(feature[17]) ;
				total_BytesTransferRatio += Double.parseDouble(feature[18]) ;
				duration += Double.parseDouble(feature[19]) ;
				
				if ( count == 1 )
				{
					timestamp = Long.parseLong(line[0]) ;
				}
				
				// test
				//System.out.println( "count = " + count ) ;
				//System.out.println( "timestamp = " + timestamp ) ;
			}
			
			// Get average of each features
			srcToDst_NumOfPkts = round( srcToDst_NumOfPkts/count , 5 ) ;
			srcToDst_NumOfBytes = round( srcToDst_NumOfBytes/count , 5 ) ;
			srcToDst_Byte_Max = round( srcToDst_Byte_Max/count , 5 ) ;
			srcToDst_Byte_Min = round( srcToDst_Byte_Min/count , 5 ) ;
			srcToDst_Byte_Mean = round( srcToDst_Byte_Mean/count , 5 ) ;
			
			dstToSrc_NumOfPkts = round( dstToSrc_NumOfPkts/count , 5 ) ;
			dstToSrc_NumOfBytes = round( dstToSrc_NumOfBytes/count , 5 ) ;
			dstToSrc_Byte_Max = round( dstToSrc_Byte_Max/count , 5 ) ;
			dstToSrc_Byte_Min = round( dstToSrc_Byte_Min/count , 5 ) ;
			dstToSrc_Byte_Mean = round( dstToSrc_Byte_Mean/count , 5 ) ;
			
			total_NumOfPkts = round( total_NumOfPkts/count , 5 ) ;
			total_NumOfBytes = round( total_NumOfBytes/count , 5 ) ;
			total_Byte_Max = round( total_Byte_Max/count , 5 ) ;
			total_Byte_Min = round( total_Byte_Min/count , 5 ) ;
			total_Byte_Mean = round( total_Byte_Mean/count , 5 ) ;
			total_Byte_STD = round( total_Byte_STD/count , 5 ) ;
			
			total_PktsRate = round( total_PktsRate/count , 5 ) ;
			total_BytesRate = round( total_BytesRate/count , 5 ) ;
			total_BytesTransferRatio = round( total_BytesTransferRatio/count , 5 ) ;
			duration = round( duration/count , 5 ) ;
			
			
			// append dstIP to the String buffer
			for ( String dIP : dstIPSet )
			{
				dstIPStr.append( dIP + "," ) ;
			}
			
			outputKey.set( key.k1toString() ) ;
			outputValue.set(	timestamp + "\t" + dstIPStr.toString() + "\t" + 
								srcToDst_NumOfPkts + "," + srcToDst_NumOfBytes + "," + srcToDst_Byte_Max + "," + srcToDst_Byte_Min + "," + srcToDst_Byte_Mean + "," + 
								dstToSrc_NumOfPkts + "," + dstToSrc_NumOfBytes + "," + dstToSrc_Byte_Max + "," + dstToSrc_Byte_Min + "," + dstToSrc_Byte_Mean + "," + 
								total_NumOfPkts + "," + total_NumOfBytes + "," + total_Byte_Max + "," + total_Byte_Min + "," + total_Byte_Mean + "," + total_Byte_STD + "," + 
								total_PktsRate + "," + total_BytesRate + "," + total_BytesTransferRatio + "," + duration ) ;
			context.write( outputKey, outputValue ) ;
			// test
			//System.out.println( "key = " + key.toString() );
			//System.out.println( "outValue = " + outputValue.toString() );
		}
		
		public double round ( double value, int places )
		{
			if ( places < 0 ) throw new IllegalArgumentException() ;
			
			BigDecimal bd = new BigDecimal( value ) ;
			bd = bd.setScale( places, RoundingMode.HALF_UP ) ;
			return bd.doubleValue() ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		Job job2 = Job.getInstance(conf, "Group 2 - job2");
		job2.setJarByClass(GroupPhase2MR_2.class);

		job2.setMapperClass(GetAverageFVOfLevelTwoClusterMapper.class);
		job2.setReducerClass(GetAverageFVOfLevelTwoClusterReducer.class);

		job2.setMapOutputKeyClass(CompositeKey.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setPartitionerClass( KeyPartitioner.class ) ;
		job2.setSortComparatorClass( CompositeKeyComparator.class ) ;
		job2.setGroupingComparatorClass( KeyGroupingComparator.class ) ;

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_GetAverageFVOfLevelTwoCluster"));
		job2.setKeyValueReduceClass(CompositeKey.class, Text.class, userDefineReducer.class);
		job2.setKeyValueMapClass(CompositeKey.class, Text.class, userDefineMapper.class);

		return job2.waitForCompletion(true) ? 0 : 1 ;
	}

	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GroupPhase2MR_2(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}
	public static class userDefineMapper extends GenericProxyMapper <CompositeKey,Text>{
		public userDefineMapper() throws Exception { super(CompositeKey.class,Text.class); }
		 @Override
			public void stringToKey(String in, CompositeKey key){
			 	System.out.println("Key:"+ in );
				String k[] = in.split(",");
				key.setTuples(k[0] +","+ k[1]);
				key.setTime(Long.parseLong(k[2]));
			}
	}
	public static class userDefineReducer extends GenericProxyReducer <CompositeKey,Text>{
		public userDefineReducer() throws Exception { super(CompositeKey.class,Text.class); }
	   
	}
}
