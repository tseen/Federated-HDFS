package fbicloud.botrank ;

import fbicloud.botrank.KeyComparator.* ;
import fbicloud.utils.FlowGroupFS ;
import fbicloud.utils.DataDistribution ;
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
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.NullWritable ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.util.Tool ;
import org.apache.hadoop.util.ToolRunner ;


public class GroupPhase1MR extends Configured implements Tool
{
	public static class FindMaxMinFVMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		private int offset = 3 ;
		// counter
		private int lineNum = 0 ;
		
		// number of features
		private final static int numOfFeatures = 20 ;
		// feature vector max, min value
		private double[] fvMax = new double[numOfFeatures] ;
		private double[] fvMin = new double[numOfFeatures] ;
		
		// map intermediate value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																															index
			// timestamp    Protocol    SrcIP:SrcPort>DstIP:DstPort																					0~2
			// srcToDst_NumOfPkts    srcToDst_NumOfBytes    srcToDst_Byte_Max    srcToDst_Byte_Min    srcToDst_Byte_Mean						3~7
			// dstToSrc_NumOfPkts    dstToSrc_NumOfBytes    dstToSrc_Byte_Max    dstToSrc_Byte_Min    dstToSrc_Byte_Mean						8~12
			// total_NumOfPkts       total_NumOfBytes       total_Byte_Max       total_Byte_Min       total_Byte_Mean       total_Byte_STD		13~17
			// total_PktsRate        total_BytesRate        total_BytesTransferRatio      duration    Loss										18~23
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			lineNum ++ ;
			for ( int i = 0 ; i < numOfFeatures ; i ++ )
			{
				// assign first value
				if ( lineNum == 1 )
				{
					fvMax[i] = Double.parseDouble( subLine[i+offset] ) ;
					fvMin[i] = Double.parseDouble( subLine[i+offset] ) ;
				}
				else
				{
					// max comparison
					if ( Double.parseDouble(subLine[i+offset]) > fvMax[i] )
					{
						fvMax[i] = Double.parseDouble( subLine[i+offset] ) ;
					}
					// min comparison
					if ( Double.parseDouble(subLine[i+offset]) < fvMin[i] )
					{
						fvMin[i] = Double.parseDouble( subLine[i+offset] ) ;
					}
				}
			}
		}
		
		// do the cleanup at the end of the map job
		@Override
		protected void cleanup ( Context context ) throws IOException, InterruptedException
		{
			String maxLine = "" ;
			String minLine = "" ;
			for ( int i = 0 ; i < numOfFeatures ; i ++ )
			{
				maxLine += String.valueOf(fvMax[i]) + "\t" ;
				minLine += String.valueOf(fvMin[i]) + "\t" ;
			}
			
			interKey.set ( "max" ) ;
			interValue.set ( maxLine ) ;
			context.write ( interKey, interValue ) ;
			// test
			//System.out.println( "max " ) ;
			//System.out.println( "interKey = " + interKey.toString() ) ;
			//System.out.println( "interValue = " + interValue.toString() ) ;
			interKey.set ( "min" ) ;
			interValue.set ( minLine ) ;
			context.write ( interKey, interValue ) ;
			// test
			//System.out.println( "min " ) ;
			//System.out.println( "interKey = " + interKey.toString() ) ;
			//System.out.println( "interValue = " + interValue.toString() ) ;
		}
	}
	
	public static class FindMaxMinFVReducer extends Reducer <Text, Text, Text, Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		// counter
		private int lineNum ;
		
		// number of features
		private final static int numOfFeatures = 20 ;
		// feature vector max, min value
		private double[] fvMax = new double[numOfFeatures] ;
		private double[] fvMin = new double[numOfFeatures] ;
		
		// reduce output value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// max/min
			// value :																													index
			// srcToDst_NumOfPkts  srcToDst_NumOfBytes  srcToDst_Byte_Max  srcToDst_Byte_Min  srcToDst_Byte_Mean						0~4
			// dstToSrc_NumOfPkts  dstToSrc_NumOfBytes  dstToSrc_Byte_Max  dstToSrc_Byte_Min  dstToSrc_Byte_Mean						5~9
			// total_NumOfPkts     total_NumOfBytes     total_Byte_Max     total_Byte_Min     total_Byte_Mean     total_Byte_STD  		10~14
			// total_PktsRate      total_BytesRate      total_BytesTransferRatio    duration											15~19
			
			lineNum = 0 ;
			
			for ( Text val : values )
			{
				lineNum ++ ;
				
				line = val.toString() ;
				subLine = line.split("\t") ;
				
				// max
				if ( key.toString().equals("max") )
				{
					for ( int i = 0 ; i < numOfFeatures ; i ++ )
					{
						// assign first value
						if ( lineNum == 1 )
						{
							fvMax[i] = Double.parseDouble( subLine[i] ) ;
						}
						else
						{
							if ( Double.parseDouble(subLine[i]) > fvMax[i] )
							{
								fvMax[i] = Double.parseDouble( subLine[i] ) ;
							}
						}
					}
				}
				// min
				if ( key.toString().equals("min") )
				{
					for ( int i = 0 ; i < numOfFeatures ; i ++ )
					{
						// assign first value
						if ( lineNum == 1 )
						{
							fvMin[i] = Double.parseDouble( subLine[i] ) ;
						}
						else
						{
							if ( Double.parseDouble(subLine[i]) < fvMin[i] )
							{
								fvMin[i] = Double.parseDouble( subLine[i] ) ;
							}
						}
					}
				}
			}
		}
		
		@Override
		protected void cleanup ( Context context ) throws IOException, InterruptedException
		{
			String maxLine = "" ;
			String minLine = "" ;
			for ( int i = 0 ; i < numOfFeatures ; i ++ )
			{
				maxLine += String.valueOf(fvMax[i]) + "\t" ;
				minLine += String.valueOf(fvMin[i]) + "\t" ;
			}
			
			outputKey.set( "max" ) ;
			outputValue.set ( maxLine ) ;
			context.write ( outputKey, outputValue ) ;
			// test
			//System.out.println( "max " ) ;
			//System.out.println( "outputKey = " + outputKey.toString() ) ;
			//System.out.println( "outputValue = " + outputValue.toString() ) ;
			outputKey.set( "min" ) ;
			outputValue.set ( minLine ) ;
			context.write ( outputKey, outputValue ) ;
			// test
			//System.out.println( "min " ) ;
			//System.out.println( "outputKey = " + outputKey.toString() ) ;
			//System.out.println( "outputValue = " + outputValue.toString() ) ;
		}
	}
	
	
	public static class LevelOneClusterMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// src and dst IP
		private String srcInfo, srcIP ;
		private String dstInfo, dstIP ;
		
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// length of line
		private final static int lengthOfLine = 24 ;
		
		// map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																												index
			// timestamp	   Protocol    SrcIP:SrcPort>DstIP:DstPort																		0~2
			// srcToDst_NumOfPkts  srcToDst_NumOfBytes  srcToDst_Byte_Max  srcToDst_Byte_Min  srcToDst_Byte_Mean					3~7
			// dstToSrc_NumOfPkts  dstToSrc_NumOfBytes  dstToSrc_Byte_Max  dstToSrc_Byte_Min  dstToSrc_Byte_Mean					8~12
			// total_NumOfPkts     total_NumOfBytes     total_Byte_Max     total_Byte_Min     total_Byte_Mean     total_Byte_STD	13~18
			// total_PktsRate      total_BytesRate      total_BytesTransferRatio    duration  Loss									19~23
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			
			String timestamp = subLine[0] ;
			String protocol = subLine[1] ;
			
			String srcToDst_NumOfPkts = subLine[3] ;
			String srcToDst_NumOfBytes = subLine[4] ;
			String srcToDst_Byte_Max = subLine[5] ;
			String srcToDst_Byte_Min = subLine[6] ;
			String srcToDst_Byte_Mean = subLine[7] ;
			
			String dstToSrc_NumOfPkts = subLine[8] ;
			String dstToSrc_NumOfBytes = subLine[9] ;
			String dstToSrc_Byte_Max = subLine[10] ;
			String dstToSrc_Byte_Min = subLine[11] ;
			String dstToSrc_Byte_Mean = subLine[12] ;
			
			String total_NumOfPkts = subLine[13] ;
			String total_NumOfBytes = subLine[14] ;
			String total_Byte_Max = subLine[15] ;
			String total_Byte_Min = subLine[16] ;
			String total_Byte_Mean = subLine[17] ;
			String total_Byte_STD = subLine[18] ;
			
			String total_PktsRate = subLine[19] ;
			String total_BytesRate = subLine[20] ;
			String total_BytesTransferRatio = subLine[21] ;
			String duration = subLine[22] ;
			String loss = subLine[23] ;
			
			
			if ( subLine.length == lengthOfLine )
			{
				// subLine[2] = SrcIP:SrcPort>DstIP:DstPort
				srcInfo = subLine[2].split(">")[0] ;
				dstInfo = subLine[2].split(">")[1] ;
				srcIP = srcInfo.split(":")[0] ;
				dstIP = dstInfo.split(":")[0] ;
				
				
				comKey.setTuples( protocol + "," + srcIP + ">" + dstIP ) ;
				comKey.setTime( Long.parseLong( timestamp ) ) ;
				
				interValue.set(	timestamp + "\t" + srcIP + "\t" + dstIP + "\t" + 
								srcToDst_NumOfPkts + "\t" + srcToDst_NumOfBytes + "\t" + srcToDst_Byte_Max + "\t" + srcToDst_Byte_Min + "\t" + srcToDst_Byte_Mean + "\t" + 
								dstToSrc_NumOfPkts + "\t" + dstToSrc_NumOfBytes + "\t" + dstToSrc_Byte_Max + "\t" + dstToSrc_Byte_Min + "\t" + dstToSrc_Byte_Mean + "\t" + 
								total_NumOfPkts + "\t" + total_NumOfBytes + "\t" + total_Byte_Max + "\t" + total_Byte_Min + "\t" + total_Byte_Mean + "\t" + total_Byte_STD + "\t" + 
								total_PktsRate + "\t" + total_BytesRate + "\t" + total_BytesTransferRatio + "\t" + duration + "\t" + loss ) ;
				context.write ( comKey, interValue ) ;
				
				// test
				//System.out.println( "interKey = " + interKey.toString() ) ;
				//System.out.println( "interValue = " + interValue.toString() ) ;
			}
		}
	}
	
	public static class LevelOneClusterReducer extends Reducer <CompositeKey, Text, Text, Text>
	{
		// values from command line
		private int minPts ;
		private double distance ;
		
		
		// list of flows with the same 3-tuples
		private ArrayList<FlowGroupFS> flowList = new ArrayList<FlowGroupFS>();
		// list of neighbors of p
		private ArrayList<Integer> neighborList = new ArrayList<Integer>();
		// cluster ID
		private int clusterID = 0 ;
		// index of flow in the flow list
		private int index ;
		
		
		// input value reader
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
		
		// test : complexity
		//private int complexity = 0 ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			minPts = Integer.parseInt( config.get( "minPts", "3" ) ) ;
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
			// Protocol,srcIP>dstIP
			// value :																												index
			// timestamp    SrcIP    DstIP																								0~2
			// srcToDst_NumOfPkts  srcToDst_NumOfBytes  srcToDst_Byte_Max  srcToDst_Byte_Min  srcToDst_Byte_Mean					3~7
			// dstToSrc_NumOfPkts  dstToSrc_NumOfBytes  dstToSrc_Byte_Max  dstToSrc_Byte_Min  dstToSrc_Byte_Mean					8~12
			// total_NumOfPkts     total_NumOfBytes     total_Byte_Max     total_Byte_Min     total_Byte_Mean     total_Byte_STD	13~18
			// total_PktsRate      total_BytesRate      total_BytesTransferRatio    duration  Loss									19~23
			
			
			// construct a list to store the flows with the same 3-tuples
			flowList.clear() ;
			for ( Text val : values )
			{
				feature = val.toString().split("\t") ;
				
				String timestamp = feature[0] ;
				String srcIP = feature[1] ;
				String dstIP = feature[2] ;
				
				String srcToDst_NumOfPkts = feature[3] ;
				String srcToDst_NumOfBytes = feature[4] ;
				String srcToDst_Byte_Max = feature[5] ;
				String srcToDst_Byte_Min = feature[6] ;
				String srcToDst_Byte_Mean = feature[7] ;
				
				String dstToSrc_NumOfPkts = feature[8] ;
				String dstToSrc_NumOfBytes = feature[9] ;
				String dstToSrc_Byte_Max = feature[10] ;
				String dstToSrc_Byte_Min = feature[11] ;
				String dstToSrc_Byte_Mean = feature[12] ;
				
				String total_NumOfPkts = feature[13] ;
				String total_NumOfBytes = feature[14] ;
				String total_Byte_Max = feature[15] ;
				String total_Byte_Min = feature[16] ;
				String total_Byte_Mean = feature[17] ;
				String total_Byte_STD = feature[18] ;
				
				String total_PktsRate = feature[19] ;
				String total_BytesRate = feature[20] ;
				String total_BytesTransferRatio = feature[21] ;
				String duration = feature[22] ;
				String loss = feature[23] ;
				
				flowList.add ( new FlowGroupFS ( timestamp, srcIP, dstIP,
												srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean,
												dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean,
												total_NumOfPkts, total_NumOfBytes, total_Byte_Max, total_Byte_Min, total_Byte_Mean, total_Byte_STD,
												total_PktsRate, total_BytesRate, total_BytesTransferRatio, duration, loss ) ) ;
			}
			// test
			//System.out.println( "flowList size = " + flowList.size() ) ;
			
			
			// group similar flows in the flow list
			for ( FlowGroupFS p : flowList )
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
					}
				}
				
				// write reduce output
				if ( neighborList.size() >= minPts )
				{
					for ( Integer i : neighborList )
					{
						outputKey.set ( key.k1toString() + ",G" + flowList.get(i).getCid() ) ;
						outputValue.set ( flowList.get(i).getVector() ) ;
						context.write( outputKey, outputValue ) ;
						
						// test
						//System.out.println( "outKey = " + outputKey.toString() );
						//System.out.println( "outValue = " + outputValue.toString() );
					}
				}
			}
			
			// test : complexity
			//System.out.println( "complexity = " + complexity ) ;
		}
		
		// get the neighbors of p
		public void getNeighbors ( FlowGroupFS p, ArrayList<FlowGroupFS> list, ArrayList<Integer> neighbors )
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
			for ( FlowGroupFS q : list )
			{
				index ++ ;
				
				// if q is visited, don't need to calculate again
				if ( q.isVisited() )
				{
					// do nothing
				}
				else
				{
					// test : complexity
					//complexity ++ ;
					
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
	
	
	public static class GetAverageFVOfClusterMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// timestamp
		private long timestamp ;
		
		// map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// LongWritable
			// value :														index (split by tab)
			// Protocol,srcIP>dstIP											0
			// timestamp,														1
			// srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean,
			// dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean,
			// total_NumOfPkts,    total_NumOfBytes,    total_Byte_Max,    total_Byte_Min,    total_Byte_Mean,    total_Byte_STD,
			// total_PktsRate,     total_BytesRate,     total_BytesTransferRatio,   duration, Loss
			
			
			// test
			//System.out.println( "key = " + key.toString() ) ;
			//System.out.println( "value = " + value.toString() ) ;
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			if ( subLine.length == 2 )
			{
				timestamp = Long.parseLong( subLine[1].split(",")[0] ) ;
				
				comKey.setTuples( subLine[0] ) ; // Protocol,srcIP>dstIP
				comKey.setTime( timestamp ) ;
				interValue.set( subLine[1] ) ;
				context.write( comKey, interValue ) ;
			}
		}
	}
	
	public static class GetAverageFVOfClusterReducer extends Reducer <CompositeKey, Text, Text, Text>
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
		private String[] feature ;
		// counter
		private int count ;
		// timestamp
		private long timestamp ;
		
		
		// reduce output key and value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( CompositeKey key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// CompositeKey = ( Protocol,srcIP>dstIP ; timestamp )
			// value :																												index
			// timestamp,																												0
			// srcToDst_NumOfPkts, srcToDst_NumOfBytes, srcToDst_Byte_Max, srcToDst_Byte_Min, srcToDst_Byte_Mean,					1~5
			// dstToSrc_NumOfPkts, dstToSrc_NumOfBytes, dstToSrc_Byte_Max, dstToSrc_Byte_Min, dstToSrc_Byte_Mean,					6~10
			// total_NumOfPkts,    total_NumOfBytes,    total_Byte_Max,    total_Byte_Min,    total_Byte_Mean,    total_Byte_STD,	11~15
			// total_PktsRate,     total_BytesRate,     total_BytesTransferRatio,   duration, Loss									16~21
			
			
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
			
			
			Iterator<Text> val = values.iterator() ;
			while ( val.hasNext() )
			{
				count ++ ;
				
				feature = val.next().toString().split(",") ;
				
				srcToDst_NumOfPkts += Double.parseDouble(feature[1]) ;
				srcToDst_NumOfBytes += Double.parseDouble(feature[2]) ;
				srcToDst_Byte_Max += Double.parseDouble(feature[3]) ;
				srcToDst_Byte_Min += Double.parseDouble(feature[4]) ;
				srcToDst_Byte_Mean += Double.parseDouble(feature[5]) ;
				
				dstToSrc_NumOfPkts += Double.parseDouble(feature[6]) ;
				dstToSrc_NumOfBytes += Double.parseDouble(feature[7]) ;
				dstToSrc_Byte_Max += Double.parseDouble(feature[8]) ;
				dstToSrc_Byte_Min += Double.parseDouble(feature[9]) ;
				dstToSrc_Byte_Mean += Double.parseDouble(feature[10]) ;
				
				total_NumOfPkts += Double.parseDouble(feature[11]) ;
				total_NumOfBytes += Double.parseDouble(feature[12]) ;
				total_Byte_Max += Double.parseDouble(feature[13]) ;
				total_Byte_Min += Double.parseDouble(feature[14]) ;
				total_Byte_Mean += Double.parseDouble(feature[15]) ;
				total_Byte_STD += Double.parseDouble(feature[16]) ;
				
				total_PktsRate += Double.parseDouble(feature[17]) ;
				total_BytesRate += Double.parseDouble(feature[18]) ;
				total_BytesTransferRatio += Double.parseDouble(feature[19]) ;
				duration += Double.parseDouble(feature[20]) ;
				
				
				if ( count == 1 )
				{
					timestamp = Long.parseLong(feature[0]) ;
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
			
			
			outputKey.set( key.k1toString() ) ; // key : Protocol,srcIP>dstIP
			outputValue.set(	timestamp + "\t" + 
								srcToDst_NumOfPkts + "\t" + srcToDst_NumOfBytes + "\t" + srcToDst_Byte_Max + "\t" + srcToDst_Byte_Min + "\t" + srcToDst_Byte_Mean + "\t" + 
								dstToSrc_NumOfPkts + "\t" + dstToSrc_NumOfBytes + "\t" + dstToSrc_Byte_Max + "\t" + dstToSrc_Byte_Min + "\t" + dstToSrc_Byte_Mean + "\t" + 
								total_NumOfPkts + "\t" + total_NumOfBytes + "\t" + total_Byte_Max + "\t" + total_Byte_Min + "\t" + total_Byte_Mean + "\t" + total_Byte_STD + "\t" + 
								total_PktsRate + "\t" + total_BytesRate + "\t" + total_BytesTransferRatio + "\t" + duration ) ;
			context.write( outputKey, outputValue ) ;
			// test
			//System.out.println( "outKey = " + outputKey.toString() );
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
		if ( otherArgs.length != 2 )
		{
			System.out.println( "GroupPhase1MR: <in> <out>" ) ;
			System.exit(2);
		}
		
		/*********************************************************/
		// job 1
		// find max and min FV value
		/*********************************************************/
		
		Job findMaxMinFV = Job.getInstance( conf, "Group 1 - FindMaxMinFV" ) ;
		findMaxMinFV.setJarByClass( GroupPhase1MR.class ) ;
		
		findMaxMinFV.setMapperClass( FindMaxMinFVMapper.class ) ;
		findMaxMinFV.setReducerClass( FindMaxMinFVReducer.class ) ;
		
		findMaxMinFV.setMapOutputKeyClass( Text.class ) ;
		findMaxMinFV.setMapOutputValueClass( Text.class ) ;
		
		findMaxMinFV.setOutputKeyClass( Text.class ) ;
		findMaxMinFV.setOutputValueClass( Text.class ) ;
		
		FileInputFormat.addInputPath( findMaxMinFV, new Path( args[0] ) ) ;
		FileOutputFormat.setOutputPath( findMaxMinFV, new Path( args[1] + "_MaxMinFV" ) ) ;
		
		// compare every map output max and min, so it can set only one reduce
		findMaxMinFV.setNumReduceTasks(1) ;
		
		findMaxMinFV.waitForCompletion( true ) ;
		
		/*********************************************************/
		// job 2
		// cluster sessions with the same source-destination IP
		/*********************************************************/
		
		conf.set( "MaxMinFV", args[1] + "_MaxMinFV" ) ;
		
		Job levelOneCluster = Job.getInstance( conf, "Group 1 - LevelOneCluster" ) ;
		levelOneCluster.setJarByClass( GroupPhase1MR.class ) ;
		
		levelOneCluster.setMapperClass( LevelOneClusterMapper.class ) ;
		levelOneCluster.setReducerClass( LevelOneClusterReducer.class ) ;
		
		levelOneCluster.setMapOutputKeyClass( CompositeKey.class ) ;
		levelOneCluster.setMapOutputValueClass( Text.class ) ;
		
		levelOneCluster.setOutputKeyClass( Text.class ) ;
		levelOneCluster.setOutputValueClass( Text.class ) ;
		
		levelOneCluster.setPartitionerClass( KeyPartitioner.class ) ;
		levelOneCluster.setSortComparatorClass( CompositeKeyComparator.class ) ;
		levelOneCluster.setGroupingComparatorClass( KeyGroupingComparator.class ) ;
		
		FileInputFormat.addInputPath( levelOneCluster, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath( levelOneCluster, new Path(args[1] + "_LevelOneCluster") ) ;
		
		levelOneCluster.waitForCompletion(true) ;
		
		/*********************************************************/
		// job 3
		// calculate average FV value of the clusters
		/*********************************************************/
		
		Job getAverageFVOfCluster = Job.getInstance( conf, "Group 1 - GetAverageFVOfCluster" ) ;
		getAverageFVOfCluster.setJarByClass(GroupPhase1MR.class) ;
		
		getAverageFVOfCluster.setMapperClass(GetAverageFVOfClusterMapper.class) ;
		getAverageFVOfCluster.setReducerClass(GetAverageFVOfClusterReducer.class) ;
		
		getAverageFVOfCluster.setMapOutputKeyClass(CompositeKey.class) ;
		getAverageFVOfCluster.setMapOutputValueClass(Text.class) ;
		
		getAverageFVOfCluster.setOutputKeyClass(Text.class) ;
		getAverageFVOfCluster.setOutputValueClass(Text.class) ;
		
		// Partitions the key space
		getAverageFVOfCluster.setPartitionerClass(KeyPartitioner.class) ;
		// Define the comparator that controls how the keys are sorted before they are passed to the Reducer
		getAverageFVOfCluster.setSortComparatorClass(CompositeKeyComparator.class) ;
		// Define the comparator that controls which keys are grouped together for a single call to Reducer
		getAverageFVOfCluster.setGroupingComparatorClass(KeyGroupingComparator.class) ;
		
		FileInputFormat.addInputPath( getAverageFVOfCluster, new Path(args[1] + "_LevelOneCluster") ) ;
		FileOutputFormat.setOutputPath( getAverageFVOfCluster, new Path(args[1]) ) ;
		
		return getAverageFVOfCluster.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run( new Configuration(), new GroupPhase1MR(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}
