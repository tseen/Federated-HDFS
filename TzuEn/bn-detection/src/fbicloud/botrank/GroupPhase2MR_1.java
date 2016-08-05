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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fbicloud.botrank.KeyComparator.CompositeKey;
import fbicloud.botrank.KeyComparator.CompositeKeyComparator;
import fbicloud.botrank.KeyComparator.KeyGroupingComparator;
import fbicloud.botrank.KeyComparator.KeyPartitioner;
import fbicloud.utils.Host2HostBehaviorFS;
import fbicloud.utils.SimilarityFunction;


public class GroupPhase2MR_1 extends Configured implements Tool
{
	public static class LevelTwoClusterMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// length of line
		private final static int lengthOfLine = 22 ;
		
		// protocol and src and dst IP
		private String protocol ;
		private String ipPair ;
		private String srcIP, dstIP ;
		
		
		// map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																												index
			// Protocol,srcIP>dstIP    timestamp																					0~1
			// srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean,					2~6
			// dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean,					7~11
			// total_NumOfPkts,    total_NumOfBytes,    total_Byte_Max,    total_Byte_Min,    total_Byte_Mean,    total_Byte_STD,	12~17
			// total_PktsRate,     total_BytesRate,     total_BytesTransferRatio,   duration										18~21
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			System.out.println("inMapline:"+ line );

			String timestamp = subLine[1] ;
			
			String srcToDst_NumOfPkts = subLine[2] ;
			String srcToDst_NumOfBytes = subLine[3] ;
			String srcToDst_Byte_Max = subLine[4] ;
			String srcToDst_Byte_Min = subLine[5] ;
			String srcToDst_Byte_Mean = subLine[6] ;
			
			String dstToSrc_NumOfPkts = subLine[7] ;
			String dstToSrc_NumOfBytes = subLine[8] ;
			String dstToSrc_Byte_Max = subLine[9] ;
			String dstToSrc_Byte_Min = subLine[10] ;
			String dstToSrc_Byte_Mean = subLine[11] ;
			
			String total_NumOfPkts = subLine[12] ;
			String total_NumOfBytes = subLine[13] ;
			String total_Byte_Max = subLine[14] ;
			String total_Byte_Min = subLine[15] ;
			String total_Byte_Mean = subLine[16] ;
			String total_Byte_STD = subLine[17] ;
			
			String total_PktsRate = subLine[18] ;
			String total_BytesRate = subLine[19] ;
			String total_BytesTransferRatio = subLine[20] ;
			String duration = subLine[21] ;
			
			
			if ( subLine.length == lengthOfLine )
			{
				protocol = subLine[0].split(",")[0] ;
				ipPair = subLine[0].split(",")[1] ;
				srcIP = ipPair.split(">")[0] ;
				dstIP = ipPair.split(">")[1] ;
				
				comKey.setTuples( srcIP + "," + protocol ) ;
				comKey.setTime( Long.parseLong( timestamp ) ) ;
				
				interValue.set(	timestamp + "\t" + dstIP + "\t" + 
								srcToDst_NumOfPkts + "," + srcToDst_NumOfBytes + "," + srcToDst_Byte_Max + "," + srcToDst_Byte_Min + "," + srcToDst_Byte_Mean + "," + 
								dstToSrc_NumOfPkts + "," + dstToSrc_NumOfBytes + "," + dstToSrc_Byte_Max + "," + dstToSrc_Byte_Min + "," + dstToSrc_Byte_Mean + "," + 
								total_NumOfPkts + "," + total_NumOfBytes + "," + total_Byte_Max + "," + total_Byte_Min + "," + total_Byte_Mean + "," + total_Byte_STD + "," + 
								total_PktsRate + "," + total_BytesRate + "," + total_BytesTransferRatio + "," + duration ) ;
				context.write( comKey, interValue ) ;
				System.out.println("inMap:"+ comKey +"?"+interValue );
				
				// test
				//System.out.println( "interKey = " + interKey.toString() ) ;
				//System.out.println( "interValue = " + interValue.toString() ) ;
			}
		}	
	}
	
	public static class LevelTwoClusterReducer extends Reducer <CompositeKey, Text, Text, Text>
	{
		// values from command line
		private int minPts ;
		private double distance ;
		
		
		// list of flows with the same srcIP
		private ArrayList<Host2HostBehaviorFS> flowList = new ArrayList<Host2HostBehaviorFS>();
		// list of neighbors of p
		private ArrayList<Integer> neighborList = new ArrayList<Integer>();
		// cluster ID
		private int clusterID = 0 ;
		// index of flow in the flow list
		private int index ;
		
		// count the number of different destination IPs
		private Set<String> dstIPSet = new HashSet<String>() ;
		
		// input value reader
		private String[] line ;
		private String[] feature ;
		
		// number of features
		private final static int numOfFeatures = 20 ;
		
		// normalize variable : feature vector max, min
		private double[] fvMax = new double[numOfFeatures] ;
		private double[] fvMin = new double[numOfFeatures] ;
		// calculate similarity
		private double calculateSimularity ;
		private SimilarityFunction sf = new SimilarityFunction() ;
		
		
		// reduce output key and value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			minPts = Integer.parseInt( config.get( "similarBehaviorThreshold", "3" ) ) ;
			distance = Double.parseDouble( config.get( "distance", "4" ) ) ;
			// test
			//System.out.println( "minPts = " + minPts ) ;
			System.out.println( "distance = " + distance ) ;
			
			// load FV max, min
			String line ;
			String[] subLine ;
			FileSystem fs = FileSystem.get( config ) ;
			FileStatus[] status = fs.listStatus( new Path( config.get("MaxMinFV") ) ) ;
			int offset = 1 ;
			
			for ( int j = 0 ; j < status.length ; j ++ )
			{
				BufferedReader br = new BufferedReader( new InputStreamReader( fs.open(status[j].getPath()) ) ) ;
				// input format		index
				// max/min			0
				// 20 FV			1~20
				
				line = br.readLine() ;
				
				while ( line != null )
				{
					subLine = line.split("\t") ;
					for ( int i = 0 ; i < numOfFeatures ; i ++ )
					{
						if ( subLine[0].equals("max") )
						{
							fvMax[i] = Double.parseDouble(subLine[i+offset]) ;
						}
						if ( subLine[0].equals("min") )
						{
							fvMin[i] = Double.parseDouble(subLine[i+offset]) ;
						}
					}
					line = br.readLine() ;
				}
			}
		}
		
		public void reduce ( CompositeKey key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// srcIP,protocol
			// value :																												index
			// timestamp    dstIP																									0~1		split by "\t"
			// srcToDst_NumOfPkts  srcToDst_NumOfBytes  srcToDst_Byte_Max  srcToDst_Byte_Min  srcToDst_Byte_Mean					0~4		split by ","
			// dstToSrc_NumOfPkts  dstToSrc_NumOfBytes  dstToSrc_Byte_Max  dstToSrc_Byte_Min  dstToSrc_Byte_Mean					5~9
			// total_NumOfPkts     total_NumOfBytes     total_Byte_Max     total_Byte_Min     total_Byte_Mean     total_Byte_STD	10~15
			// total_PktsRate      total_BytesRate      total_BytesTransferRatio    duration										16~19
			
			
			// construct a list to store the flows with the same srcIP
			flowList.clear() ;
			for ( Text val : values )
			{
				line = val.toString().split("\t") ;
				feature = line[2].split(",") ;
				
				
				String timestamp = line[0] ;
				String dstIP = line[1] ;
				
				String srcToDst_NumOfPkts = feature[0] ;
				String srcToDst_NumOfBytes = feature[1] ;
				String srcToDst_Byte_Max = feature[2] ;
				String srcToDst_Byte_Min = feature[3] ;
				String srcToDst_Byte_Mean = feature[4] ;
				
				String dstToSrc_NumOfPkts = feature[5] ;
				String dstToSrc_NumOfBytes = feature[6] ;
				String dstToSrc_Byte_Max = feature[7] ;
				String dstToSrc_Byte_Min = feature[8] ;
				String dstToSrc_Byte_Mean = feature[9] ;
				
				String total_NumOfPkts = feature[10] ;
				String total_NumOfBytes = feature[11] ;
				String total_Byte_Max = feature[12] ;
				String total_Byte_Min = feature[13] ;
				String total_Byte_Mean = feature[14] ;
				String total_Byte_STD = feature[15] ;
				
				String total_PktsRate = feature[16] ;
				String total_BytesRate = feature[17] ;
				String total_BytesTransferRatio = feature[18] ;
				String duration = feature[19] ;
				
				
				flowList.add ( new Host2HostBehaviorFS ( timestamp, dstIP,
														srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean,
														dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean,
														total_NumOfPkts, total_NumOfBytes, total_Byte_Max, total_Byte_Min, total_Byte_Mean, total_Byte_STD,
														total_PktsRate, total_BytesRate, total_BytesTransferRatio, duration ) ) ;
			}
			// test
			//System.out.println( "key = " + key.toString() ) ;
			//System.out.println( "flowList size = " + flowList.size() ) ;
			//System.out.println( "==================================" ) ;
			
			
			// group similar flows in the flow list
			for ( Host2HostBehaviorFS p : flowList )
			{
				// check if p is visited
				if ( p.isVisited() )
				{
					continue ;
				}
				
				
				// get the neighbors of p
				neighborList.clear() ;
				getNeighbors( p, flowList, neighborList ) ;
				// test
				//System.out.println( "neighborList.size() = " + neighborList.size() ) ;
				// initialize
				dstIPSet.clear() ;
				
				
				// p is a noise, not have enough neighbors
				if ( neighborList.size() < minPts )
				{
					// set p visited
					p.setVisited( true ) ;
					
					if ( p.getCid() == 0 )
					{
						p.setCid( -1 ) ;
						// test
						//System.out.println( "p is a NOISE" ) ;
					}
				}
				// p belong to a cluster
				else
				{
					clusterID ++ ;
					
					// set neighbors of p (include p) : visited, cluster ID
					for ( Integer i : neighborList )
					{
						// set visited
						flowList.get(i).setVisited( true ) ;
						// set cluster ID
						if ( flowList.get(i).getCid() == 0 )
						{
							flowList.get(i).setCid( clusterID ) ;
						}
						
						// add to dst IP set
						dstIPSet.add ( flowList.get(i).getDstIP() ) ;
					}
				}
				// test
				//System.out.println( "key = " + key.toString() ) ;
				//System.out.println( "dstIPSet.size() = " + dstIPSet.size() ) ;
				
				// write reduce output
				if ( neighborList.size() >= minPts && dstIPSet.size() >= minPts )
				{
					for ( Integer i : neighborList )
					{
						outputKey.set ( key.k1toString() + ",G" + flowList.get(i).getCid() ) ;
						outputValue.set ( flowList.get(i).getFVString() ) ;
						context.write( outputKey, outputValue ) ;
						
						// test
						//System.out.println( "outKey = " + outputKey.toString() );
						//System.out.println( "outValue = " + outputValue.toString() );
					}
				}
			}
		}
		
		// get the neighbors of p
		public void getNeighbors ( Host2HostBehaviorFS p, ArrayList<Host2HostBehaviorFS> list, ArrayList<Integer> neighbors )
		{
			// feature vectors of p, q
			double[] pFeatures = p.getFeatures() ;
			double[] qFeatures ;
			// copy the feature vectors of p, q
			double[] pFV = Arrays.copyOf ( pFeatures, pFeatures.length ) ;
			double[] qFV ;
			// normalize
			normalize ( pFV ) ;
			
			
			// index of flow in the flow list
			index = -1 ;
			// calculate cosine similarity with all flows in the flow list
			for ( Host2HostBehaviorFS q : list )
			{
				index ++ ;
				
				// if q is visited, don't need to calculate again
				if ( q.isVisited() )
				{
					// do nothing
				}
				else
				{
					// features of flow q
					qFeatures = q.getFeatures() ;
					// copy the q feature array
					qFV = Arrays.copyOf ( qFeatures, qFeatures.length ) ;
					// normalize
					normalize ( qFV ) ;
					
					
					// calculate similarity
					int flag = sf.euclideanDistanceSelectedFV( pFV, qFV, distance ) ;
					
					// flag = 1 -> similar ; flag = 0 -> not similar
					if ( flag > 0 )
					{
						// add index in the neighbor list
						neighbors.add( index ) ;
						
						// test
						//System.out.println( "sim index = " + index ) ;
					}
				}
			}
		}
		
		// normalize feature vectors
		public void normalize ( double[] normalFV )
		{
			for ( int i = 0 ; i < numOfFeatures ; i ++ )
			{
				normalFV[i] = (normalFV[i] - fvMin[i]) / (fvMax[i] - fvMin[i]) * 100 ;
			}
		}
	}
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		/*if ( otherArgs.length < 2 )
		{
			System.out.println("GroupPhase2MR: <in> <out>") ;
			System.exit(2);
		}
		*/
		
		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	DBScan clustering algorithm										*
		 *------------------------------------------------------------------*/
		// set a name for the FV max, min output
		conf.set( "MaxMinFV", args[2]) ;
		
		Job job1 = Job.getInstance(conf, "Group 2 - job1");
		job1.setJarByClass(GroupPhase2MR_1.class);

		job1.setMapperClass(LevelTwoClusterMapper.class);
		job1.setReducerClass(LevelTwoClusterReducer.class);

		job1.setMapOutputKeyClass( CompositeKey.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setPartitionerClass( KeyPartitioner.class ) ;
		job1.setSortComparatorClass( CompositeKeyComparator.class ) ;
		job1.setGroupingComparatorClass( KeyGroupingComparator.class ) ;

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_LevelTwoCluster"));
		job1.setKeyValueReduceClass(CompositeKey.class, Text.class, userDefineReducer.class);
		job1.setKeyValueMapClass(CompositeKey.class, Text.class, userDefineMapper.class);

		return job1.waitForCompletion(true) ? 0 : 1;	
		}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GroupPhase2MR_1(), args ) ;
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
