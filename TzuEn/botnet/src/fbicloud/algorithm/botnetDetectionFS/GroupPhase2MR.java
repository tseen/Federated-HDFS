package fbicloud.algorithm.botnetDetectionFS;

import fbicloud.algorithm.classes.host2hostBehaviorFS;
import fbicloud.algorithm.classes.hostBehaviorFS;
import fbicloud.algorithm.classes.SimilarityFunction ;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GroupPhase2MR extends Configured implements Tool
{
	public static class job1Mapper extends Mapper <LongWritable, Text, Text , Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// protocol and src and dst IP
		private String prot ;
		private String ipPair ;
		private String srcIP, dstIP ;
		
		// map intermediate key and value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
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
				prot = subLine[0].split(",")[0] ;
				ipPair = subLine[0].split(",")[1] ;
				srcIP = ipPair.split(">")[0] ;
				dstIP = ipPair.split(">")[1] ;
				
				interKey.set( srcIP + "," + prot + "," + subLine[1] ) ; // key : srcIP,protocol,wPER
				interValue.set(	dstIP+"\t"+
								subLine[2]+","+subLine[3]+","+subLine[4]+","+subLine[5]+","+subLine[6]+","+
								subLine[7]+","+subLine[8]+","+subLine[9]+","+subLine[10]+","+subLine[11]+","+
								subLine[12]+","+subLine[13]+","+subLine[14]+","+subLine[15]+","+subLine[16]+","+subLine[17]+","+
								subLine[18]+","+subLine[19]+","+subLine[20]+","+subLine[21]+","+
								subLine[22]+","+subLine[23]+","+subLine[24]+","+subLine[25] );
				context.write( interKey, interValue ) ;
				
				// test
				//System.out.println( "interKey = " + interKey.toString() ) ;
				//System.out.println( "interValue = " + interValue.toString() ) ;
			}
		}	
	}
	
	public static class job1Reducer extends Reducer <Text, Text, Text, Text>
	{
		// values from command line
		//private float similarity ;
		private int minPts ;
		private double distance ;
		
		
		// list of flows with the same srcIP
		private ArrayList<host2hostBehaviorFS> flowList = new ArrayList<host2hostBehaviorFS>();
		// list of neighbors of p
		private ArrayList<Integer> neighborList = new ArrayList<Integer>();
		// cluster ID
		private int clusterID = 0 ;
		// index of flow in the flow list
		private int index ;
		
		
		// input value reader
		private String[] line ;
		private String[] feature ;
		
		
		// features of p and q, use of compare two feature vectors
		//private double[] pFeatures ;
		//private double[] qFeatures ;
		// normalize variable : feature vector max, min
		private double[] fvMax = new double[20] ;
		private double[] fvMin = new double[20] ;
		// calculate similarity
		private double calculateSimularity ;
		private SimilarityFunction sf = new SimilarityFunction() ;
		
		
		// reduce output key and value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			// Get the parameter from -D command line option
			// Hadoop will parse the -D similarity=xxx and set the value, and then call run()
			//similarity = Float.parseFloat( config.get("similarity") ) ;
			minPts = Integer.parseInt( config.get("similarBehaviorThreshold") ) ;
			distance = Double.parseDouble( config.get( "distance" ) ) ;
			
			
			// load FV max, min
			String line ;
			String[] subLine ;
			FileSystem fs = FileSystem.get( config ) ;
			FileStatus[] status = fs.listStatus( new Path( config.get("FVMaxMin") ) ) ;
			
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
					for ( int i = 0 ; i < 20 ; i ++ )
					{
						if ( subLine[0].equals("max") )
						{
							fvMax[i] = Double.parseDouble(subLine[i+1]) ;
						}
						if ( subLine[0].equals("min") )
						{
							fvMin[i] = Double.parseDouble(subLine[i+1]) ;
						}
					}
					line = br.readLine() ;
				}
			}
		}
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// srcIP,protocol,wPER
			// value :																					index
			// dstIP																					0		split by tab
			// S2D_noP    S2D_noB    S2D_Byte_Max    S2D_Byte_Min    S2D_Byte_Mean						0~4		split by comma
			// D2S_noP    D2S_noB    D2S_Byte_Max    D2S_Byte_Min    D2S_Byte_Mean						5~9
			// ToT_noP    ToT_noB    ToT_Byte_Max    ToT_Byte_Min    ToT_Byte_Mean    ToT_Byte_STD		10~15
			// ToT_Prate    ToT_Brate    ToT_BTransferRatio    DUR										16~19
			// IAT_Max    IAT_Min    IAT_Mean    IAT_STD												20~23
			
			
			int lineNum = 0 ;
			
			// construct a list to store the flows with the same srcIP
			flowList.clear() ;
			for ( Text val : values )
			{
				lineNum ++ ;
				
				line = val.toString().split("\t") ;
				feature = line[1].split(",") ;
				
				flowList.add ( new host2hostBehaviorFS ( line[0],
														feature[0], feature[1], feature[2], feature[3], feature[4],
														feature[5], feature[6], feature[7], feature[8], feature[9],
														feature[10], feature[11], feature[12], feature[13], feature[14], feature[15],
														feature[16], feature[17], feature[18], feature[19],
														feature[20], feature[21], feature[22], feature[23] ) ) ;
			}
			// test
			//System.out.println( "key = " + key.toString() ) ;
			//System.out.println( "flowList size = " + flowList.size() ) ;
			//System.out.println( "==================================" ) ;
			
			
			// group similar flows in the flow list
			for ( host2hostBehaviorFS p : flowList )
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
						outputKey.set ( key + ",G" + flowList.get(i).getCid() ) ;
						outputValue.set ( flowList.get(i).getVector() ) ;
						context.write( outputKey, outputValue ) ;
						// output format
						// key :
						// srcIP,protocol,wPER,G#
						// value :
						// dstIP
						// S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean,
						// D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean,
						// ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean, ToT_Byte_STD,
						// ToT_Prate, ToT_Brate, ToT_BTransferRatio, DUR
						// IAT_Max, IAT_Min, IAT_Mean, IAT_STD
						
						// test
						//System.out.println( "outKey = " + outputKey.toString() );
						//System.out.println( "outValue = " + outputValue.toString() );
					}
				}
			}
		}
		
		// get the neighbors of p
		public void getNeighbors ( host2hostBehaviorFS p, ArrayList<host2hostBehaviorFS> list, ArrayList<Integer> neighbors )
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
			for ( host2hostBehaviorFS q : list )
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
			for ( int i = 0 ; i < 20 ; i ++ )
			{
				normalFV[i] = (normalFV[i] - fvMin[i]) / (fvMax[i] - fvMin[i]) * 100 ;
			}
		}
	}
	
	
	public static class job2Mapper extends Mapper <LongWritable, Text, Text , Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// map intermediate key
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// LongWritable
			// value :
			// srcIP,protocol,wPER,G#
			// dstIP
			// S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean,
			// D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean,
			// ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean, ToT_Byte_STD,
			// ToT_Prate, ToT_Brate, ToT_BTransferRatio, DUR
			// IAT_Max, IAT_Min, IAT_Mean, IAT_STD
			
			
			// test
			//System.out.println( "key = " + key.toString() ) ;
			//System.out.println( "value = " + value.toString() ) ;
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			if ( subLine.length == 3 )
			{
				interKey.set( subLine[0] ) ; // srcIP,protocol,wPER,G#
				interValue.set( subLine[1] + "\t" + subLine[2] ) ;
				context.write( interKey, interValue ) ;
			}
		}
	}
	
	public static class job2Reducer extends Reducer <Text, Text, Text, Text>
	{
		// feature vectors variable
		private double S2D_noP ;
		private double S2D_noB ;
		private double S2D_Byte_Max ;
		private double S2D_Byte_Min ;
		private double S2D_Byte_Mean ;
		
		private double D2S_noP ;
		private double D2S_noB ;
		private double D2S_Byte_Max ;
		private double D2S_Byte_Min ;
		private double D2S_Byte_Mean ;
		
		private double ToT_noP ;
		private double ToT_noB ;
		private double ToT_Byte_Max ;
		private double ToT_Byte_Min ;
		private double ToT_Byte_Mean ;
		private double ToT_Byte_STD ;
		
		private double ToT_Prate ;
		private double ToT_Brate ;
		private double ToT_BTransferRatio ;
		private double DUR ;
		
		private double IAT_Max ;
		private double IAT_Min ;
		private double IAT_Mean ;
		private double IAT_STD ;
		
		// input value reader
		private String[] line ;
		private String[] feature ;
		// counter
		private int count ;
		
		// a HashSet to store dstIPs
		Set<String> dstIPSet = new HashSet<String>() ;
		
		// reduce output value
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// srcIP,protocol,wPER,G#
			// value :																			index
			// dstIP																			0		split by tab
			// S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean,						0~4		split by comma
			// D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean,						5~9
			// ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean, ToT_Byte_STD,		10~15
			// ToT_Prate, ToT_Brate, ToT_BTransferRatio, DUR									16~19
			// IAT_Max, IAT_Min, IAT_Mean, IAT_STD												20~23
			
			
			// initialize
			S2D_noP = 0d ;
			S2D_noB = 0d ;
			S2D_Byte_Max = 0d ;
			S2D_Byte_Min = 0d ;
			S2D_Byte_Mean = 0d ;
			
			D2S_noP = 0d ;
			D2S_noB = 0d ;
			D2S_Byte_Max = 0d ;
			D2S_Byte_Min = 0d ;
			D2S_Byte_Mean = 0d ;
			
			ToT_noP = 0d ;
			ToT_noB = 0d ;
			ToT_Byte_Max = 0d ;
			ToT_Byte_Min = 0d ;
			ToT_Byte_Mean = 0d ;
			ToT_Byte_STD = 0d ;
			
			ToT_Prate = 0d ;
			ToT_Brate = 0d ;
			ToT_BTransferRatio = 0d ;
			DUR = 0d ;
			
			IAT_Max = 0d ;
			IAT_Min = 0d ;
			IAT_Mean = 0d ;
			IAT_STD = 0d ;
			
			count = 0 ;
			dstIPSet.clear() ;
			// a string buffer to store dstIPs
			StringBuffer dstIPStr = new StringBuffer() ;
			
			
			for ( Text val : values )
			{
				count ++ ;
				line = val.toString().split("\t") ;
				
				// add dstIPs into hashset
				dstIPSet.add( line[0] ) ;
				
				feature = line[1].split(",") ;
				
				S2D_noP += Double.parseDouble(feature[0]) ;
				S2D_noB += Double.parseDouble(feature[1]) ;
				S2D_Byte_Max += Double.parseDouble(feature[2]) ;
				S2D_Byte_Min += Double.parseDouble(feature[3]) ;
				S2D_Byte_Mean += Double.parseDouble(feature[4]) ;
				
				D2S_noP += Double.parseDouble(feature[5]) ;
				D2S_noB += Double.parseDouble(feature[6]) ;
				D2S_Byte_Max += Double.parseDouble(feature[7]) ;
				D2S_Byte_Min += Double.parseDouble(feature[8]) ;
				D2S_Byte_Mean += Double.parseDouble(feature[9]) ;
				
				ToT_noP += Double.parseDouble(feature[10]) ;
				ToT_noB += Double.parseDouble(feature[11]) ;
				ToT_Byte_Max += Double.parseDouble(feature[12]) ;
				ToT_Byte_Min += Double.parseDouble(feature[13]) ;
				ToT_Byte_Mean += Double.parseDouble(feature[14]) ;
				ToT_Byte_STD += Double.parseDouble(feature[15]) ;
				
				ToT_Prate += Double.parseDouble(feature[16]) ;
				ToT_Brate += Double.parseDouble(feature[17]) ;
				ToT_BTransferRatio += Double.parseDouble(feature[18]) ;
				DUR += Double.parseDouble(feature[19]) ;
				
				IAT_Max += Double.parseDouble(feature[20]) ;
				IAT_Min += Double.parseDouble(feature[21]) ;
				IAT_Mean += Double.parseDouble(feature[22]) ;
				IAT_STD += Double.parseDouble(feature[23]) ;
			}
			
			// Get average of each features
			S2D_noP = round( S2D_noP/count , 5 ) ;
			S2D_noB = round( S2D_noB/count , 5 ) ;
			S2D_Byte_Max = round( S2D_Byte_Max/count , 5 ) ;
			S2D_Byte_Min = round( S2D_Byte_Min/count , 5 ) ;
			S2D_Byte_Mean = round( S2D_Byte_Mean/count , 5 ) ;
			
			D2S_noP = round( D2S_noP/count , 5 ) ;
			D2S_noB = round( D2S_noB/count , 5 ) ;
			D2S_Byte_Max = round( D2S_Byte_Max/count , 5 ) ;
			D2S_Byte_Min = round( D2S_Byte_Min/count , 5 ) ;
			D2S_Byte_Mean = round( D2S_Byte_Mean/count , 5 ) ;
			
			ToT_noP = round( ToT_noP/count , 5 ) ;
			ToT_noB = round( ToT_noB/count , 5 ) ;
			ToT_Byte_Max = round( ToT_Byte_Max/count , 5 ) ;
			ToT_Byte_Min = round( ToT_Byte_Min/count , 5 ) ;
			ToT_Byte_Mean = round( ToT_Byte_Mean/count , 5 ) ;
			ToT_Byte_STD = round( ToT_Byte_STD/count , 5 ) ;
			
			ToT_Prate = round( ToT_Prate/count , 5 ) ;
			ToT_Brate = round( ToT_Brate/count , 5 ) ;
			ToT_BTransferRatio = round( ToT_BTransferRatio/count , 5 ) ;
			DUR = round( DUR/count , 5 ) ;
			
			IAT_Max = round( IAT_Max/count , 5 ) ;
			IAT_Min = round( IAT_Min/count , 5 ) ;
			IAT_Mean = round( IAT_Mean/count , 5 ) ;
			IAT_STD = round( IAT_STD/count , 5 ) ;
			
			// append dstIP to the String buffer
			for ( String dIP : dstIPSet )
			{
				dstIPStr.append( dIP + "," ) ;
			}
			
			outputValue.set(	S2D_noP+","+S2D_noB+","+S2D_Byte_Max+","+S2D_Byte_Min+","+S2D_Byte_Mean+","+
								D2S_noP+","+D2S_noB+","+D2S_Byte_Max+","+D2S_Byte_Min+","+D2S_Byte_Mean+","+
								ToT_noP+","+ToT_noB+","+ToT_Byte_Max+","+ToT_Byte_Min+","+ToT_Byte_Mean+","+ToT_Byte_STD+","+
								ToT_Prate+","+ToT_Brate+","+ToT_BTransferRatio+","+DUR+","+
								IAT_Max+","+IAT_Min+","+IAT_Mean+","+IAT_STD+"\t"+
								dstIPStr.toString() ) ;
			context.write( key, outputValue ) ; // key : srcIP,protocol,wPER,G#
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
	
	
	public static class job3Mapper extends Mapper <LongWritable, Text, Text , Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		private String[] tmp ;
		
		// map intermediate key and value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :
			// srcIP,protocol,wPER,G#    24 features    dstIPs
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			tmp = subLine[0].split(",") ; // srcIP   protocol   wPER   G#
			
			if ( subLine.length == 3 )
			{
				interKey.set( tmp[1] ) ;
				interValue.set( subLine[1] + "\t" + tmp[0] + "\t" + subLine[2] ) ;
				context.write( interKey, interValue ) ;
				// output format
				// key : protocol
				// value : 24 feature    srcIP    dstIPs
			}
		}
	}
	
	public static class job3Reducer extends Reducer <Text, Text, Text, Text>
	{
		// values from command line
		//private float similarity ;
		private int minPts ;
		private double distance ;
		
		// list of flows with the same protocol
		private ArrayList<hostBehaviorFS> flowList = new ArrayList<hostBehaviorFS>();
		// list of neighbors of p
		private ArrayList<Integer> neighborList = new ArrayList<Integer>();
		// cluster ID
		private int clusterID = 0 ;
		// index of flow in the flow list
		private int index ;
		
		
		// input value reader
		private String[] mLine ;
		private String[] feature ;
		
		
		// feature vector max, min
		private double[] fvMax = new double[20] ;
		private double[] fvMin = new double[20] ;
		// calculate similarity
		private double calculateSimularity ;
		private SimilarityFunction sf = new SimilarityFunction() ;
		
		
		// reduce output key and value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			// Get the parameter from -D command line option
			// Hadoop will parse the -D similarity=xxx and set the value, and then call run()
			//similarity = Float.parseFloat( config.get("similarityBH") ) ;
			minPts = Integer.parseInt( config.get("similarBehaviorThresholdBH") ) ;
			distance = Double.parseDouble( config.get( "distanceBH" ) ) ;
			
			
			// load FV max, min
			String line ;
			String[] subLine ;
			FileSystem fs = FileSystem.get( config ) ;
			FileStatus[] status = fs.listStatus( new Path( config.get("FVMaxMin2") ) ) ;
			
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
					for ( int i = 0 ; i < 20 ; i ++ )
					{
						if ( subLine[0].equals("max") )
						{
							fvMax[i] = Double.parseDouble(subLine[i+1]) ;
						}
						if ( subLine[0].equals("min") )
						{
							fvMin[i] = Double.parseDouble(subLine[i+1]) ;
						}
					}
					line = br.readLine() ;
				}
			}
		}
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// protocol
			// value :
			// 24 features    srcIP    dstIPs
			
			
			int lineNum = 0 ;
			
			// test
			System.out.println( "key = " + key.toString() ) ;
			int ii = 0 ;
			
			// construct a list to store the flows with the same protocol
			flowList.clear() ;
			for ( Text val : values )
			{
				mLine = val.toString().split("\t") ;
				feature = mLine[0].split(",") ;
				
				flowList.add ( new hostBehaviorFS (	mLine[1], mLine[2],
													feature[0], feature[1], feature[2], feature[3], feature[4],
													feature[5], feature[6], feature[7], feature[8], feature[9],
													feature[10], feature[11], feature[12], feature[13], feature[14], feature[15],
													feature[16], feature[17], feature[18], feature[19] ) ) ;
				// test
				System.out.println( ii + " : val = " + val.toString() ) ;
				ii ++ ;
			}
			// test
			//System.out.println( "flowList size = " + flowList.size() ) ;
			
			
			// group similar flows in the flow list
			for ( hostBehaviorFS p : flowList )
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
					}
				}
				
				// write reduce output
				if ( neighborList.size() >= minPts )
				{
					for ( Integer i : neighborList )
					{
						outputKey.set ( key + ",G" + flowList.get(i).getCid() ) ;
						outputValue.set ( flowList.get(i).getVector() ) ;
						context.write( outputKey, outputValue ) ;
						// output format
						// key :
						// protocol,G#
						// value :
						// feature vectors    srcIP    dstIPs
						
						// test
						System.out.println( "outKey = " + outputKey.toString() );
						System.out.println( "outValue = " + outputValue.toString() );
					}
				}
			}
		}
		
		// get the neighbors of p
		public void getNeighbors ( hostBehaviorFS p, ArrayList<hostBehaviorFS> list, ArrayList<Integer> neighbors )
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
			for ( hostBehaviorFS q : list )
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
						System.out.println( "index = " + index ) ;
						//System.out.println( "cosine Sim = " + cosineSim ) ;
					}
				}
			}
		}
		
		// expand the cluster
		public void expandCluster ( hostBehaviorFS p, ArrayList<hostBehaviorFS> list, ArrayList<Integer> neighbors )
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
			for ( int i = 0 ; i < 20 ; i ++ )
			{
				normalFV[i] = (normalFV[i] - fvMin[i]) / (fvMax[i] - fvMin[i]) * 100 ;
			}
		}
	}
	
	
	public static class job4Mapper extends Mapper <LongWritable, Text, Text , Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// map intermediate key
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// LongWritable
			// value :
			// protocol,G#    feature vectors    srcIP    dstIPs
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			if ( subLine.length == 4 )
			{
				interKey.set( subLine[0] ) ; // protocol,G#
				interValue.set( subLine[1] + "\t" + subLine[2] + "\t" + subLine[3] ) ; // feature vectors    srcIP    dstIPs
				context.write( interKey, interValue ) ;
			}
		}
	}
	
	public static class job4Reducer extends Reducer <Text, Text, Text, Text>
	{
		// feature vectors variable
		private double S2D_noP ;
		private double S2D_noB ;
		private double S2D_Byte_Max ;
		private double S2D_Byte_Min ;
		private double S2D_Byte_Mean ;
		
		private double D2S_noP ;
		private double D2S_noB ;
		private double D2S_Byte_Max ;
		private double D2S_Byte_Min ;
		private double D2S_Byte_Mean ;
		
		private double ToT_noP ;
		private double ToT_noB ;
		private double ToT_Byte_Max ;
		private double ToT_Byte_Min ;
		private double ToT_Byte_Mean ;
		private double ToT_Byte_STD ;
		
		private double ToT_Prate ;
		private double ToT_Brate ;
		private double ToT_BTransferRatio ;
		private double DUR ;
		
		// input value reader
		private String[] line ;
		private String[] feature ;
		// counter
		private int count ;
		
		// a HashSet to store srcIPs
		private Set<String> srcIPSet = new HashSet<String>() ;
		// a HashSet to store dstIPs
		private Set<String> dstIPSet = new HashSet<String>() ;
		
		// reduce output value
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// protocol,G#
			// value :
			// feature vectors    srcIP    dstIPs
			
			
			// initialize
			S2D_noP = 0d ;
			S2D_noB = 0d ;
			S2D_Byte_Max = 0d ;
			S2D_Byte_Min = 0d ;
			S2D_Byte_Mean = 0d ;
			
			D2S_noP = 0d ;
			D2S_noB = 0d ;
			D2S_Byte_Max = 0d ;
			D2S_Byte_Min = 0d ;
			D2S_Byte_Mean = 0d ;
			
			ToT_noP = 0d ;
			ToT_noB = 0d ;
			ToT_Byte_Max = 0d ;
			ToT_Byte_Min = 0d ;
			ToT_Byte_Mean = 0d ;
			ToT_Byte_STD = 0d ;
			
			ToT_Prate = 0d ;
			ToT_Brate = 0d ;
			ToT_BTransferRatio = 0d ;
			DUR = 0d ;
			
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
				
				feature = line[0].split(",") ;
				
				S2D_noP += Double.parseDouble(feature[0]) ;
				S2D_noB += Double.parseDouble(feature[1]) ;
				S2D_Byte_Max += Double.parseDouble(feature[2]) ;
				S2D_Byte_Min += Double.parseDouble(feature[3]) ;
				S2D_Byte_Mean += Double.parseDouble(feature[4]) ;
				
				D2S_noP += Double.parseDouble(feature[5]) ;
				D2S_noB += Double.parseDouble(feature[6]) ;
				D2S_Byte_Max += Double.parseDouble(feature[7]) ;
				D2S_Byte_Min += Double.parseDouble(feature[8]) ;
				D2S_Byte_Mean += Double.parseDouble(feature[9]) ;
				
				ToT_noP += Double.parseDouble(feature[10]) ;
				ToT_noB += Double.parseDouble(feature[11]) ;
				ToT_Byte_Max += Double.parseDouble(feature[12]) ;
				ToT_Byte_Min += Double.parseDouble(feature[13]) ;
				ToT_Byte_Mean += Double.parseDouble(feature[14]) ;
				ToT_Byte_STD += Double.parseDouble(feature[15]) ;
				
				ToT_Prate += Double.parseDouble(feature[16]) ;
				ToT_Brate += Double.parseDouble(feature[17]) ;
				ToT_BTransferRatio += Double.parseDouble(feature[18]) ;
				DUR += Double.parseDouble(feature[19]) ;
				
				// add srcIP into hashset
				srcIPSet.add( line[1] ) ;
				
				// add dstIPs into hashset
				String[] dstIParray = line[2].split(",") ;
				for ( int i = 0 ; i < dstIParray.length ; i ++ )
				{
					dstIPSet.add( dstIParray[i] ) ;
				}
			}
			
			// Get average of each features
			S2D_noP = round( S2D_noP/count , 5 ) ;
			S2D_noB = round( S2D_noB/count , 5 ) ;
			S2D_Byte_Max = round( S2D_Byte_Max/count , 5 ) ;
			S2D_Byte_Min = round( S2D_Byte_Min/count , 5 ) ;
			S2D_Byte_Mean = round( S2D_Byte_Mean/count , 5 ) ;
			
			D2S_noP = round( D2S_noP/count , 5 ) ;
			D2S_noB = round( D2S_noB/count , 5 ) ;
			D2S_Byte_Max = round( D2S_Byte_Max/count , 5 ) ;
			D2S_Byte_Min = round( D2S_Byte_Min/count , 5 ) ;
			D2S_Byte_Mean = round( D2S_Byte_Mean/count , 5 ) ;
			
			ToT_noP = round( ToT_noP/count , 5 ) ;
			ToT_noB = round( ToT_noB/count , 5 ) ;
			ToT_Byte_Max = round( ToT_Byte_Max/count , 5 ) ;
			ToT_Byte_Min = round( ToT_Byte_Min/count , 5 ) ;
			ToT_Byte_Mean = round( ToT_Byte_Mean/count , 5 ) ;
			ToT_Byte_STD = round( ToT_Byte_STD/count , 5 ) ;
			
			ToT_Prate = round( ToT_Prate/count , 5 ) ;
			ToT_Brate = round( ToT_Brate/count , 5 ) ;
			ToT_BTransferRatio = round( ToT_BTransferRatio/count , 5 ) ;
			DUR = round( DUR/count , 5 ) ;
			
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
			
			outputValue.set(	S2D_noP+","+S2D_noB+","+S2D_Byte_Max+","+S2D_Byte_Min+","+S2D_Byte_Mean+","+
								D2S_noP+","+D2S_noB+","+D2S_Byte_Max+","+D2S_Byte_Min+","+D2S_Byte_Mean+","+
								ToT_noP+","+ToT_noB+","+ToT_Byte_Max+","+ToT_Byte_Min+","+ToT_Byte_Mean+","+ToT_Byte_STD+","+
								ToT_Prate+","+ToT_Brate+","+ToT_BTransferRatio+","+DUR+"\t"+
								srcIPStr.toString()+"\t"+
								dstIPStr.toString() ) ;
			context.write( key, outputValue ) ; // key : protocol,G#
			// test
			System.out.println( "key = " + key.toString() );
			System.out.println( "srcIPStr.toString() = " + srcIPStr.toString() );
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
	
	
	public static class job5Mapper extends Mapper <LongWritable, Text, Text , Text>
	{
		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// value :								index
			// protocol,G#    feature vectors		0,1
			// srcIPs    dstIPs						2,3
			
			
			String line = value.toString() ;
			String[] subLine = line.split ("\t") ;
			
			if ( subLine.length == 4 )
			{
				interKey.set ( "Key" ) ;
				interValue.set ( subLine[0] + "\t" + subLine[1] + "\t" + subLine[2] + "\t" + subLine[3] ) ;
				context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class job5Reducer extends Reducer <Text, Text, NullWritable, Text>
	{
		// reduce output value
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			int FVID = 0 ;
			
			for ( Text val : values )
			{
				FVID ++ ;
				String[] subLine = val.toString().split ("\t") ;
				
				outputValue.set ( String.valueOf(FVID) + "\t" + subLine[0] + "\t" + subLine[1] + "\t" + 
								  subLine[2] + "\t" + subLine[3] ) ;
				context.write ( NullWritable.get(), outputValue ) ;
			}
		}
	}
	
	
	public static class job6Mapper extends Mapper <LongWritable, Text, IntWritable , Text>
	{
		private int noOfReducer = 0 ;
		private int count = 0 ;
		
		// map inter key, value
		private IntWritable interKey = new IntWritable() ;
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			//get the parameter from -d command line option
			noOfReducer = Integer.parseInt ( config.get ( "mapreduce.job.reduces" ) ) ;
			
			// test
			System.out.println ( "noOfReducer = " + noOfReducer ) ;
			/*System.out.println ( "context.getTaskAttemptID() = " + context.getTaskAttemptID() ) ;
			System.out.println ( "context.getTaskAttemptID().getTaskID() = " + context.getTaskAttemptID().getTaskID() ) ;
			System.out.println ( "context.getTaskAttemptID().getTaskID().getId() = " + context.getTaskAttemptID().getTaskID().getId() ) ;
			*/
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// value :										index
			// FVID    protocol,G#    feature vectors		0~2
			// srcIPs    dstIPs								3~4
			
			
			String line = value.toString() ;
			String[] subLine = line.split ("\t") ;
			
			if ( subLine.length == 5 )
			{
				interKey.set ( ++count % noOfReducer ) ;
				interValue.set ( subLine[0] + "\t" + subLine[1] + "\t" + subLine[2] + "\t" + 
								 subLine[3] + "\t" + subLine[4] ) ;
				context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class job6Reducer extends Reducer <IntWritable, Text, NullWritable, Text>
	{
		private FSDataOutputStream out ;
		
		// reduce output value
		private Text outputValue = new Text();
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			String MappingListDir = config.get ( "FVID_IP_MappingList" ) ;
			FileSystem fs = FileSystem.get ( config ) ;
			/****** path problem ******/
			fs.mkdirs ( new Path ( "/user/hpds/" + MappingListDir ) ) ;
			out = fs.create ( new Path ( "/user/hpds/" + MappingListDir + "/part-r-" + context.getTaskAttemptID().getTaskID().getId())) ;
		}
		
		public void cleanup ( Context context ) throws IOException, InterruptedException
		{
			super.cleanup ( context ) ;
			out.close() ;
		}
		
		public void reduce ( IntWritable key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			//String[] subLine = new String[5] ;
			String outputMappingList ;
			
			for ( Text val : values )
			{
				String[] subLine = val.toString().split ("\t") ;
				
				outputValue.set ( subLine[0] + "\t" + subLine[1] + "\t" + subLine[2] + "\t" + 
								  subLine[3] + "\t" + subLine[4] ) ;
				context.write ( NullWritable.get(), outputValue ) ;
				
				// Write the " FVID, Source IPs " to Mapping list
				outputMappingList = subLine[0] + "\t" + subLine[3] + "\n" ;
				out.write ( outputMappingList.getBytes(), 0, outputMappingList.length() ) ;
			}
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		if ( otherArgs.length < 2 )
		{
			System.out.println("GroupPhase2MR: <in> <out>") ;
			System.exit(2);
		}
		
		
		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	DBScan clustering algorithm										*
		 *------------------------------------------------------------------*/
		// set a name for the FV max, min output
		conf.set( "FVMaxMin", args[0] + "_FVMaxMin" ) ;
		
		Job job1 = Job.getInstance(conf, "Group 2 - job1");
		job1.setJarByClass(GroupPhase2MR.class);

		job1.setMapperClass(job1Mapper.class);
		job1.setReducerClass(job1Reducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_job1_DBScan"));

		job1.waitForCompletion(true);
		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Get average features of same group"								*
		 *------------------------------------------------------------------*/
		Job job2 = Job.getInstance(conf, "Group 2 - job2");
		job2.setJarByClass(GroupPhase2MR.class);

		job2.setMapperClass(job2Mapper.class);
		job2.setReducerClass(job2Reducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]+"_job1_DBScan"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_job2_average_Feature"));

		job2.waitForCompletion(true);
		/*------------------------------------------------------------------*
		 *								Job 3		 						*
		 *	DBScan clustering algorithm													*
		 *------------------------------------------------------------------*/
		// set a name for the FV max, min output
		conf.set( "FVMaxMin2", args[0] + "_FVMaxMin" ) ;
		
		Job job3 = Job.getInstance(conf, "Group 2 - job3");  
		job3.setJarByClass(GroupPhase2MR.class);

		job3.setMapperClass(job3Mapper.class);
		job3.setReducerClass(job3Reducer.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(args[1]+"_job2_average_Feature"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"_job3_DBScan_FV"));

		job3.waitForCompletion(true);
		
		/*------------------------------------------------------------------*
		 *								Job 4		 						*
		 *	Get average features of same group								*
		 *------------------------------------------------------------------*/
		Job job4 = Job.getInstance(conf, "Group 2 - job4");
		job4.setJarByClass(GroupPhase2MR.class);

		job4.setMapperClass(job4Mapper.class);
		job4.setReducerClass(job4Reducer.class);

		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job4, new Path(args[1]+"_job3_DBScan_FV"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]+"_job4_average_Feature"));
		
		job4.waitForCompletion(true);
		/*------------------------------------------------------------------*
		 *								Job 5		 						*
		 *	Assign Feature Vector ID										*
		 *------------------------------------------------------------------*/
		
		Job job5 = Job.getInstance(conf, "Group 2 - job5");
		job5.setJarByClass(GroupPhase2MR.class);
		
		job5.setMapperClass(job5Mapper.class);
		job5.setReducerClass(job5Reducer.class);
		
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);
		
		job5.setOutputKeyClass(NullWritable.class);
		job5.setOutputValueClass(Text.class);
		
		job5.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job5, new Path(args[1]+"_job4_average_Feature"));
		FileOutputFormat.setOutputPath(job5, new Path(args[1]+"_job5_assign_FVID"));
		
		job5.waitForCompletion(true);
		/*------------------------------------------------------------------*
		 *								Job 6		 						*
		 *	Read files and split into pieses							*
		 *------------------------------------------------------------------*/
		conf.set("FVID_IP_MappingList",args[2]);
		
		Job job6 = Job.getInstance(conf, "Group 2 - job6");
		job6.setJarByClass(GroupPhase2MR.class);

		job6.setMapperClass(job6Mapper.class);
		job6.setReducerClass(job6Reducer.class);

		job6.setMapOutputKeyClass(IntWritable.class);
		job6.setMapOutputValueClass(Text.class);

		job6.setOutputKeyClass(NullWritable.class);
		job6.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job6, new Path(args[1]+"_job5_assign_FVID"));
		FileOutputFormat.setOutputPath(job6, new Path(args[1]));

		return job6.waitForCompletion(true) ? 0 : 1;
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
