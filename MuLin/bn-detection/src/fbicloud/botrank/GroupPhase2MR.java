package fbicloud.botrank ;

import fbicloud.botrank.KeyComparator.* ;
import fbicloud.utils.Host2HostBehaviorFS ;
import fbicloud.utils.HostBehaviorFS ;
import fbicloud.utils.SimilarityFunction ;

import java.io.* ;
import java.math.BigDecimal ;
import java.math.RoundingMode ;
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


public class GroupPhase2MR extends Configured implements Tool
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
			
			minPts = Integer.parseInt( config.get( "dstMinPts", "3" ) ) ;
			distance = Double.parseDouble( config.get( "dstDistance", "4" ) ) ;
			// test
			System.out.println( "dstMinPts = " + minPts ) ;
			System.out.println( "dstDistance = " + distance ) ;
			
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
			
			minPts = Integer.parseInt( config.get( "srcMinPts", "3" ) ) ;
			distance = Double.parseDouble( config.get( "srcDistance", "1.3" ) ) ;
			// test
			System.out.println( "srcMinPts = " + minPts ) ;
			System.out.println( "srcDistance = " + distance ) ;
			
			
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
				//System.out.println( "key = " + key.toString() ) ;
				//System.out.println( "srcIPSet.size() = " + srcIPSet.size() ) ;
				
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
	
	
	public static class GetAverageFVOfLevelThreeClusterMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// length of line
		private final static int lengthOfLine = 5 ;
		
		// map intermediate key
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// LongWritable
			// value :															index
			// protocol,G#    timestamp    srcIP    dstIPs    20 features		0~4
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			String protocol_gid = subLine[0] ;
			String srcIP = subLine[2] ;
			String dstIPs = subLine[3] ;
			String features = subLine[4] ;
			
			
			if ( subLine.length == lengthOfLine )
			{
				interKey.set( protocol_gid ) ; // protocol,G#
				interValue.set( srcIP + "\t" + dstIPs + "\t" + features ) ;
				context.write( interKey, interValue ) ;
			}
		}
	}
	
	public static class GetAverageFVOfLevelThreeClusterReducer extends Reducer <Text, Text, Text, Text>
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
		
		// a HashSet to store srcIPs
		private Set<String> srcIPSet = new TreeSet<String>() ;
		// a HashSet to store dstIPs
		private Set<String> dstIPSet = new TreeSet<String>() ;
		
		
		// reduce output value
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// protocol,G#
			// value :									index
			// srcIP    dstIPs    20 features			0~2
			
			
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
			
			srcIPSet.clear() ;
			dstIPSet.clear() ;
			// string buffer to store srcIPs and dstIPs
			StringBuffer srcIPStr = new StringBuffer() ;
			StringBuffer dstIPStr = new StringBuffer() ;
			
			
			for ( Text val : values )
			{
				count ++ ;
				line = val.toString().split("\t") ;
				
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
				
				// add srcIP into hashset
				srcIPSet.add( line[0] ) ;
				
				// add dstIPs into hashset
				String[] dstIParray = line[1].split(",") ;
				for ( int i = 0 ; i < dstIParray.length ; i ++ )
				{
					dstIPSet.add( dstIParray[i] ) ;
				}
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
			
			// append srcIP to the String buffer
			for ( String sIP : srcIPSet )
			{
				srcIPStr.append( sIP + "," ) ;
			}
			// append dstIP to the String buffer
			for ( String dIP : dstIPSet )
			{
				dstIPStr.append( dIP + "," ) ;
			}
			
			outputValue.set(	srcIPStr.toString() + "\t" + dstIPStr.toString() + "\t" + 
								srcToDst_NumOfPkts + "," + srcToDst_NumOfBytes + "," + srcToDst_Byte_Max + "," + srcToDst_Byte_Min + "," + srcToDst_Byte_Mean + "," + 
								dstToSrc_NumOfPkts + "," + dstToSrc_NumOfBytes + "," + dstToSrc_Byte_Max + "," + dstToSrc_Byte_Min + "," + dstToSrc_Byte_Mean + "," + 
								total_NumOfPkts + "," + total_NumOfBytes + "," + total_Byte_Max + "," + total_Byte_Min + "," + total_Byte_Mean + "," + total_Byte_STD + "," + 
								total_PktsRate + "," + total_BytesRate + "," + total_BytesTransferRatio + "," + duration ) ;
			context.write( key, outputValue ) ;
			// test
			//System.out.println( "key = " + key.toString() );
			//System.out.println( "srcIPStr.toString() = " + srcIPStr.toString() );
			//System.out.println( "dstIPStr.toString() = " + dstIPStr.toString() );
		}
		
		public double round ( double value, int places )
		{
			if ( places < 0 ) throw new IllegalArgumentException() ;
			
			BigDecimal bd = new BigDecimal( value ) ;
			bd = bd.setScale( places, RoundingMode.HALF_UP ) ;
			return bd.doubleValue() ;
		}
	}
	
	
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
			
			// ### A part ###
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
			// ### A part end ###
		}
	}
	
	
	public static class GetFVIDMappingMapper extends Mapper <LongWritable, Text, IntWritable, Text>
	{
		private FSDataOutputStream out ;
		
		private int numOfReducer = 0 ;
		private int count = 0 ;
		
		// length of line
		private final static int lengthOfLine = 5 ;
		
		// map inter key, value
		private IntWritable interKey = new IntWritable() ;
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			numOfReducer = Integer.parseInt ( config.get ( "mapreduce.job.reduces" ) ) ;
			String hdfsPath = config.get( "HDFSPath", "/user/hpds/" ) ;
			String mappingListDir = config.get( "FVID_IP_MappingList" ) ;
			
			FileSystem fs = FileSystem.get ( config ) ;
			fs.mkdirs ( new Path ( hdfsPath + mappingListDir ) ) ;
			out = fs.create ( new Path ( hdfsPath + mappingListDir + "/fvidIPMapping-" + context.getTaskAttemptID().getTaskID().getId())) ;
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// value :														index
			// FVID    protocol,G#    srcIPs    dstIPs    20 features		0~4
			
			
			String outputMappingList ;
			
			
			String line = value.toString() ;
			String[] subLine = line.split ("\t") ;
			
			String fvID = subLine[0] ;
			String protocol_gid = subLine[1] ;
			String srcIPs = subLine[2] ;
			String dstIPs = subLine[3] ;
			String features = subLine[4] ;
			
			if ( subLine.length == lengthOfLine )
			{
				interKey.set ( ++count % numOfReducer ) ;
				interValue.set ( fvID + "\t" + protocol_gid + "\t" + features ) ;
				context.write ( interKey, interValue ) ;
				
				
				// ### B part ###
				// Write the " FVID, Source IPs " to Mapping list
				outputMappingList = fvID + "\t" + srcIPs + "\n" ;
				out.write ( outputMappingList.getBytes(), 0, outputMappingList.length() ) ;
			}
		}
		
		public void cleanup ( Context context ) throws IOException, InterruptedException
		{
			super.cleanup ( context ) ;
			out.close() ;
		}
	}
	
	public static class GetFVIDMappingReducer extends Reducer <IntWritable, Text, NullWritable, Text>
	{
		// reduce output value
		private Text outputValue = new Text();
		
		
		public void reduce ( IntWritable key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// value :										index
			// FVID    protocol,G#    20 features			0~2
			
			
			// ### C part ###
			for ( Text val : values )
			{
				String[] subLine = val.toString().split ("\t") ;
				
				String fvID = subLine[0] ;
				String protocol_gid = subLine[1] ;
				String features = subLine[2] ;
				
				outputValue.set ( fvID + "\t" + protocol_gid + "\t" + features ) ;
				context.write ( NullWritable.get(), outputValue ) ;
			}
			// ### C part end ###
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		if ( otherArgs.length != 3 )
		{
			System.out.println("GroupPhase2MR: <in> <out> <fvidipmapping>") ;
			System.exit(2);
		}
		
		
		/*********************************************************/
		// job 1
		// cluster sessions with the same source IP
		/*********************************************************/
		
		conf.set( "MaxMinFV", args[0] + "_MaxMinFV" ) ;
		
		Job levelTwoCluster = Job.getInstance( conf, "Group 2 - LevelTwoCluster" ) ;
		levelTwoCluster.setJarByClass( GroupPhase2MR.class ) ;
		
		levelTwoCluster.setMapperClass( LevelTwoClusterMapper.class ) ;
		levelTwoCluster.setReducerClass( LevelTwoClusterReducer.class ) ;
		
		levelTwoCluster.setMapOutputKeyClass( CompositeKey.class ) ;
		levelTwoCluster.setMapOutputValueClass( Text.class ) ;
		
		levelTwoCluster.setOutputKeyClass( Text.class ) ;
		levelTwoCluster.setOutputValueClass( Text.class ) ;
		
		levelTwoCluster.setPartitionerClass( KeyPartitioner.class ) ;
		levelTwoCluster.setSortComparatorClass( CompositeKeyComparator.class ) ;
		levelTwoCluster.setGroupingComparatorClass( KeyGroupingComparator.class ) ;
		
		FileInputFormat.addInputPath( levelTwoCluster, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath( levelTwoCluster, new Path(args[1] + "_LevelTwoCluster") ) ;
		
		levelTwoCluster.waitForCompletion(true);
		
		/*********************************************************/
		// job 2
		// calculate average FV value of the clusters
		/*********************************************************/
		
		Job getAverageFVOfLevelTwoCluster = Job.getInstance( conf, "Group 2 - GetAverageFVOfLevelTwoCluster" ) ;
		getAverageFVOfLevelTwoCluster.setJarByClass(GroupPhase2MR.class);
		
		getAverageFVOfLevelTwoCluster.setMapperClass( GetAverageFVOfLevelTwoClusterMapper.class ) ;
		getAverageFVOfLevelTwoCluster.setReducerClass( GetAverageFVOfLevelTwoClusterReducer.class ) ;
		
		getAverageFVOfLevelTwoCluster.setMapOutputKeyClass( CompositeKey.class ) ;
		getAverageFVOfLevelTwoCluster.setMapOutputValueClass( Text.class ) ;
		
		getAverageFVOfLevelTwoCluster.setOutputKeyClass( Text.class ) ;
		getAverageFVOfLevelTwoCluster.setOutputValueClass( Text.class ) ;
		
		getAverageFVOfLevelTwoCluster.setPartitionerClass( KeyPartitioner.class ) ;
		getAverageFVOfLevelTwoCluster.setSortComparatorClass( CompositeKeyComparator.class ) ;
		getAverageFVOfLevelTwoCluster.setGroupingComparatorClass( KeyGroupingComparator.class ) ;
		
		FileInputFormat.addInputPath( getAverageFVOfLevelTwoCluster, new Path(args[1] + "_LevelTwoCluster") ) ;
		FileOutputFormat.setOutputPath( getAverageFVOfLevelTwoCluster, new Path(args[1] + "_GetAverageFVOfLevelTwoCluster") ) ;
		
		getAverageFVOfLevelTwoCluster.waitForCompletion(true) ;
		
		/*********************************************************/
		// job 3
		// cluster sessions with the same protocol
		/*********************************************************/
		
		conf.set( "MaxMinFV2", args[0] + "_MaxMinFV" ) ;
		
		Job levelThreeCluster = Job.getInstance( conf, "Group 2 - LevelThreeCluster" ) ;
		levelThreeCluster.setJarByClass( GroupPhase2MR.class ) ;
		
		levelThreeCluster.setMapperClass( LevelThreeClusterMapper.class ) ;
		levelThreeCluster.setReducerClass( LevelThreeClusterReducer.class ) ;
		
		levelThreeCluster.setMapOutputKeyClass( CompositeKey.class ) ;
		levelThreeCluster.setMapOutputValueClass( Text.class ) ;
		
		levelThreeCluster.setOutputKeyClass( Text.class ) ;
		levelThreeCluster.setOutputValueClass( Text.class ) ;
		
		levelThreeCluster.setPartitionerClass( KeyPartitioner.class ) ;
		levelThreeCluster.setSortComparatorClass( CompositeKeyComparator.class ) ;
		levelThreeCluster.setGroupingComparatorClass( KeyGroupingComparator.class ) ;
		
		FileInputFormat.addInputPath( levelThreeCluster, new Path(args[1] + "_GetAverageFVOfLevelTwoCluster") ) ;
		FileOutputFormat.setOutputPath( levelThreeCluster, new Path(args[1] + "_LevelThreeCluster") ) ;
		
		levelThreeCluster.waitForCompletion(true);
		
		/*********************************************************/
		// job 4
		// calculate average FV value of the clusters
		/*********************************************************/
		
		Job getAverageFVOfLevelThreeCluster = Job.getInstance(conf, "Group 2 - GetAverageFVOfLevelThreeCluster");
		getAverageFVOfLevelThreeCluster.setJarByClass(GroupPhase2MR.class);
		
		getAverageFVOfLevelThreeCluster.setMapperClass(GetAverageFVOfLevelThreeClusterMapper.class);
		getAverageFVOfLevelThreeCluster.setReducerClass(GetAverageFVOfLevelThreeClusterReducer.class);
		
		getAverageFVOfLevelThreeCluster.setMapOutputKeyClass(Text.class);
		getAverageFVOfLevelThreeCluster.setMapOutputValueClass(Text.class);
		
		getAverageFVOfLevelThreeCluster.setOutputKeyClass(Text.class);
		getAverageFVOfLevelThreeCluster.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(getAverageFVOfLevelThreeCluster, new Path(args[1]+"_LevelThreeCluster"));
		FileOutputFormat.setOutputPath(getAverageFVOfLevelThreeCluster, new Path(args[1]+"_GetAverageFVOfLevelThreeCluster"));
		
		getAverageFVOfLevelThreeCluster.waitForCompletion(true);
		
		/*********************************************************/
		// job 5
		// assign Feature Vector ID
		/*********************************************************/
		
		Job assignFVID = Job.getInstance(conf, "Group 2 - AssignFVID");
		assignFVID.setJarByClass(GroupPhase2MR.class);
		
		assignFVID.setMapperClass(AssignFVIDMapper.class);
		assignFVID.setReducerClass(AssignFVIDReducer.class);
		
		assignFVID.setMapOutputKeyClass(Text.class);
		assignFVID.setMapOutputValueClass(Text.class);
		
		assignFVID.setOutputKeyClass(NullWritable.class);
		assignFVID.setOutputValueClass(Text.class);
		
		assignFVID.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(assignFVID, new Path(args[1]+"_GetAverageFVOfLevelThreeCluster"));
		FileOutputFormat.setOutputPath(assignFVID, new Path(args[1]+"_assignFVID"));
		
		assignFVID.waitForCompletion(true);
		
		/*********************************************************/
		// job 6
		// get Feature Vector ID mapping
		/*********************************************************/
		
		conf.set("FVID_IP_MappingList",args[2]);
		
		Job getFVIDMapping = Job.getInstance(conf, "Group 2 - GetFVIDMapping");
		getFVIDMapping.setJarByClass(GroupPhase2MR.class);
		
		getFVIDMapping.setMapperClass(GetFVIDMappingMapper.class);
		getFVIDMapping.setReducerClass(GetFVIDMappingReducer.class);
		
		getFVIDMapping.setMapOutputKeyClass(IntWritable.class);
		getFVIDMapping.setMapOutputValueClass(Text.class);
		
		getFVIDMapping.setOutputKeyClass(NullWritable.class);
		getFVIDMapping.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(getFVIDMapping, new Path(args[1]+"_assignFVID"));
		FileOutputFormat.setOutputPath(getFVIDMapping, new Path(args[1]));
		
		return getFVIDMapping.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new GroupPhase2MR(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}
}
