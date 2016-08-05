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
import fbicloud.utils.HostBehaviorFS;
import fbicloud.utils.SimilarityFunction;


public class GroupPhase2MR_3 extends Configured implements Tool
{

	public static class LevelThreeClusterMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		private String[] tmp ;
		
		// length of line
		private final static int lengthOfLine = 4 ;
		
		// map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :															index
			// srcIP,protocol,G#    timestamp    dstIPs    20 features			0~3
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			tmp = subLine[0].split(",") ; // srcIP,protocol,G#
			
			
			String srcIP = tmp[0] ;
			String protocol = tmp[1] ;
			String timestamp = subLine[1] ;
			String dstIPs = subLine[2] ;
			String features = subLine[3] ;
			
			
			if ( subLine.length == lengthOfLine )
			{
				comKey.setTuples( protocol ) ;
				comKey.setTime( Long.parseLong( timestamp ) ) ;
				
				interValue.set( timestamp + "\t" + srcIP + "\t" + dstIPs + "\t" + features ) ;
				context.write( comKey, interValue ) ;
			}
		}
	}
	
	public static class LevelThreeClusterReducer extends Reducer <CompositeKey, Text, Text, Text>
	{
		// values from command line
		private int minPts ;
		private double distance ;
		
		// list of flows with the same protocol
		private ArrayList<HostBehaviorFS> flowList = new ArrayList<HostBehaviorFS>();
		// list of neighbors of p
		private ArrayList<Integer> neighborList = new ArrayList<Integer>();
		// cluster ID
		private int clusterID = 0 ;
		// index of flow in the flow list
		private int index ;
		
		
		// input value reader
		private String[] mLine ;
		private String[] feature ;
		
		// number of features
		private final static int numOfFeatures = 20 ;
		// feature vector max, min
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
			
			minPts = Integer.parseInt( config.get( "similarBehaviorThresholdBH", "3" ) ) ;
			distance = Double.parseDouble( config.get( "distanceBH", "1.3" ) ) ;
			// test
			//System.out.println( "minPts = " + minPts ) ;
			System.out.println( "distance = " + distance ) ;
			
			
			// load FV max, min
			String line ;
			String[] subLine ;
			FileSystem fs = FileSystem.get( config ) ;
			FileStatus[] status = fs.listStatus( new Path( config.get("MaxMinFV2") ) ) ;
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
			// protocol
			// value :
			// timestamp    srcIP    dstIPs    20 features
			
			
			//int ii = 0 ;
			
			// construct a list to store the flows with the same protocol
			flowList.clear() ;
			for ( Text val : values )
			{
				mLine = val.toString().split("\t") ;
				feature = mLine[3].split(",") ;
				
				String timestamp = mLine[0] ;
				String srcIP = mLine[1] ;
				String dstIPs = mLine[2] ;
				
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
				
				flowList.add ( new HostBehaviorFS (	timestamp, srcIP, dstIPs,
													srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean,
													dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean,
													total_NumOfPkts, total_NumOfBytes, total_Byte_Max, total_Byte_Min, total_Byte_Mean, total_Byte_STD,
													total_PktsRate, total_BytesRate, total_BytesTransferRatio, duration ) ) ;
				// test
				//System.out.println( ii + " : val = " + val.toString() ) ;
				//ii ++ ;
			}
			// test
			//System.out.println( "flowList size = " + flowList.size() ) ;
			
			
			// group similar flows in the flow list
			for ( HostBehaviorFS p : flowList )
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
				/*System.out.println( "========== neighborList ==========" ) ;
				System.out.println( "neighborList.size() = " + neighborList.size() ) ;
				for ( Integer i : neighborList )
				{
					System.out.println( "neighborList i = " + i ) ;
				}*/
				
				// count the number of different source IPs
				Set<String> srcIPSet = new HashSet<String>() ;
				
				
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
					
					// expand cluster of neighbors of p
					//expandCluster( p, flowList, neighborList ) ;
					
					
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
						
						// add to src IP set
						srcIPSet.add ( flowList.get(i).getSrcIP() ) ;
					}
				}
				
				// test
				System.out.println( "key = " + key.toString() ) ;
				System.out.println( "srcIPSet.size() = " + srcIPSet.size() ) ;
				
				// write reduce output
				if ( neighborList.size() >= minPts && srcIPSet.size() >= minPts )
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
		public void getNeighbors ( HostBehaviorFS p, ArrayList<HostBehaviorFS> list, ArrayList<Integer> neighbors )
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
			for ( HostBehaviorFS q : list )
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
						//System.out.println( "index = " + index ) ;
						//System.out.println( "cosine Sim = " + cosineSim ) ;
					}
				}
			}
		}
		
		// expand the cluster
		public void expandCluster ( HostBehaviorFS p, ArrayList<HostBehaviorFS> list, ArrayList<Integer> neighbors )
		{
			// clone the list of neighbors of p
			ArrayList<Integer> pNBList = (ArrayList) neighbors.clone() ;
			
			// let p' = neighbors of p
			// list of neighbors of p'
			ArrayList<Integer> pNBNBList = new ArrayList<Integer>();
			
			// find neighbors of p'
			for ( Integer i : pNBList )
			{
				// don't need to check p again
				if ( i != list.indexOf(p) )
				{
					// test
					//System.out.println( "list.indexOf(p) = " + list.indexOf(p) ) ;
					
					// get neighbors of p'
					pNBNBList.clear() ;
					getNeighbors( list.get(i), list, pNBNBList ) ;
					// test
					//System.out.println( "NB Integer i = " + i ) ;
					//System.out.println( "pNBNBList.size() = " + pNBNBList.size() ) ;
					
					if ( pNBNBList.size() >= minPts )
					{
						// set attributes of neighbors of p'
						for ( Integer j : pNBNBList )
						{
							// neighbor of p' is not visited
							if ( ! list.get(j).isVisited() )
							{
								if ( ! neighbors.contains(j) )
								{
									// add neighbors of p' into neighbors of p
									neighbors.add(j) ;
								}
							}
						}
					}
				}
			}
			
			// test
			/*System.out.println( "========== expand neighbors ==========" ) ;
			System.out.println( "neighbors.size() = " + neighbors.size() ) ;
			for ( Integer i : neighbors )
			{
				System.out.println( "neighbors i = " + i ) ;
			}
			System.out.println( "=========================================" ) ;
			*/
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
		/*------------------------------------------------------------------*
		 *								Job 3		 						*
		 *	DBScan clustering algorithm													*
		 *------------------------------------------------------------------*/
		// set a name for the FV max, min output
		conf.set( "MaxMinFV2", args[2] ) ;
		
		Job job3 = Job.getInstance(conf, "Group 2 - job3");  
		job3.setJarByClass(GroupPhase2MR_3.class);

		job3.setMapperClass( LevelThreeClusterMapper.class);
		job3.setReducerClass( LevelThreeClusterReducer.class);

		job3.setMapOutputKeyClass(CompositeKey.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		job3.setPartitionerClass( KeyPartitioner.class ) ;
		job3.setSortComparatorClass( CompositeKeyComparator.class ) ;
		job3.setGroupingComparatorClass( KeyGroupingComparator.class ) ;

		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"_LevelThreeCluster"));
		
		job3.setKeyValueReduceClass(CompositeKey.class, Text.class, userDefineReducer.class);
		job3.setKeyValueMapClass(CompositeKey.class, Text.class, userDefineMapper.class);

		return job3.waitForCompletion(true)? 0 :1;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GroupPhase2MR_3(), args ) ;
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
