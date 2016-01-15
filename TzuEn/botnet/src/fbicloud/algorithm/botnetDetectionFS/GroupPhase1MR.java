package fbicloud.algorithm.botnetDetectionFS;

import fbicloud.algorithm.botnetDetectionFS.FilterPhaseSecondarySort.*;
import fbicloud.algorithm.classes.flowGroupFS;
import fbicloud.algorithm.classes.DataDistribution;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GroupPhase1MR extends Configured implements Tool
{
	public static class FindFVMaxMinMapper extends Mapper < LongWritable, Text, Text, Text >
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		// counter
		private int lineNum = 0 ;
		
		// feature vector max, min value
		private double[] fvMax = new double[20] ;
		private double[] fvMin = new double[20] ;
		
		// map intermediate value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																index
			// time    Protocol    SrcIP:SrcPort>DstIP:DstPort						0~2
			// S2D_noP  S2D_noB  S2D_Byte_Max  S2D_Byte_Min  S2D_Byte_Mean			3~7
			// D2S_noP  D2S_noB  D2S_Byte_Max  D2S_Byte_Min  D2S_Byte_Mean			8~12
			// ToT_noP  ToT_noB  ToT_Byte_Max  ToT_Byte_Min  ToT_Byte_Mean			13~17
			// ToT_Byte_STD  ToT_Prate  ToT_Brate  ToT_BTransferRatio  DUR  Loss	18~23
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			lineNum ++ ;
			for ( int i = 0 ; i < 20 ; i ++ )
			{
				// assign first value
				if ( lineNum == 1 )
				{
					fvMax[i] = Double.parseDouble( subLine[i+3] ) ;
					fvMin[i] = Double.parseDouble( subLine[i+3] ) ;
				}
				else
				{
					// max comparison
					if ( Double.parseDouble(subLine[i+3]) > fvMax[i] )
					{
						fvMax[i] = Double.parseDouble( subLine[i+3] ) ;
					}
					// min comparison
					if ( Double.parseDouble(subLine[i+3]) < fvMin[i] )
					{
						fvMin[i] = Double.parseDouble( subLine[i+3] ) ;
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
			for ( int i = 0 ; i < 20 ; i ++ )
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
	
	public static class FindFVMaxMinReducer extends Reducer < Text, Text, Text, Text >
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		// counter
		private int lineNum ;
		
		// feature vector max, min value
		private double[] fvMax = new double[20] ;
		private double[] fvMin = new double[20] ;
		
		// reduce output value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// max/min
			// value :																index
			// S2D_noP  S2D_noB  S2D_Byte_Max  S2D_Byte_Min  S2D_Byte_Mean			0~4
			// D2S_noP  D2S_noB  D2S_Byte_Max  D2S_Byte_Min  D2S_Byte_Mean			5~9
			// ToT_noP  ToT_noB  ToT_Byte_Max  ToT_Byte_Min  ToT_Byte_Mean			10~14
			// ToT_Byte_STD  ToT_Prate  ToT_Brate  ToT_BTransferRatio  DUR			15~19
			
			lineNum = 0 ;
			
			for ( Text val : values )
			{
				lineNum ++ ;
				
				line = val.toString() ;
				subLine = line.split("\t") ;
				
				// max
				if ( key.toString().equals("max") )
				{
					for ( int i = 0 ; i < 20 ; i ++ )
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
					for ( int i = 0 ; i < 20 ; i ++ )
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
			for ( int i = 0 ; i < 20 ; i ++ )
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
	
	
	// job 1 can be removed
	public static class job1Mapper extends Mapper <LongWritable, Text, Text, NullWritable>
	{
		// src and dst IP
		private String srcInfo, srcIP ;
		private String dstInfo, dstIP ;
		
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// map intermediate key
		private Text interKey = new Text() ;
		
		// test
		//private int lineNum = 0 ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																index
			// time    Protocol    SrcIP:SrcPort>DstIP:DstPort						0~2
			// S2D_noP  S2D_noB  S2D_Byte_Max  S2D_Byte_Min  S2D_Byte_Mean			3~7
			// D2S_noP  D2S_noB  D2S_Byte_Max  D2S_Byte_Min  D2S_Byte_Mean			8~12
			// ToT_noP  ToT_noB  ToT_Byte_Max  ToT_Byte_Min  ToT_Byte_Mean			13~17
			// ToT_Byte_STD  ToT_Prate  ToT_Brate  ToT_BTransferRatio  DUR  Loss	18~23
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			// test
			//System.out.println( "line number = " + lineNum ) ;
			//System.out.println( "key = " + key.toString() ) ;
			//System.out.println( "value = " + value.toString() ) ;
			//lineNum ++ ;
			
			if ( subLine.length == 24 )
			{
				// subLine[2] = SrcIP:SrcPort>DstIP:DstPort
				srcInfo = subLine[2].split(">")[0] ;
				dstInfo = subLine[2].split(">")[1] ;
				srcIP = srcInfo.split(":")[0] ;
				dstIP = dstInfo.split(":")[0] ;
				
				// subLine[1] : Protocol
				interKey.set ( subLine[1] + "," + srcIP + ">" + dstIP ) ;
				context.write ( interKey, NullWritable.get() ) ;
				// output format
				// key :
				// Protocol,srcIP>dstIP
				// value :
				// (null)
				
				// test
				//System.out.println( "interKey = " + interKey.toString() ) ;
				//System.out.println( "interValue = " + NullWritable.get().toString() ) ;
			}
		}
	}
	
	public static class job1Reducer extends Reducer <Text, NullWritable, Text, LongWritable>
	{
		// reduce output key and value
		private Text outputKey = new Text() ;
		private LongWritable outputValue = new LongWritable() ;
		
		public void reduce ( Text key, Iterable<NullWritable> values, Context context ) throws IOException, InterruptedException
		{
			// number of flows with the same protocol, srcIP, dstIP
			long count = 0 ;
			for ( NullWritable val : values )
			{
				count ++ ;
				// test
				//System.out.println( "count = " + count ) ;
				//System.out.println( "key = " + key.toString() ) ;
				//System.out.println( "val = " + val.toString() ) ;
			}
			outputKey.set( key ) ;
			outputValue.set( count ) ;
			context.write( outputKey, outputValue ) ;
			// output format
			// key :
			// Protocol,srcIP>dstIP
			// value :
			// count
			
			// test
			//System.out.println( "===================================" ) ;
			//System.out.println( "outputKey = " + outputKey.toString() ) ;
			//System.out.println( "outputValue = " + outputValue.toString() ) ;
			//System.out.println( "===================================" ) ;
		}
	}
	
	
	public static class job2Mapper extends Mapper <LongWritable, Text, Text, Text>
	{
		// a HashMap to store 3-tuples ( protocol, srcIP, dstIP ) counts
		private HashMap <String, String> tuplesCount = new HashMap<>() ;
		
		// src and dst IP
		private String srcInfo, srcIP ;
		private String dstInfo, dstIP ;
		
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// map intermediate key and value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		// construct a HashMap to store 3-tuples ( protocol, srcIP, dstIP ) counts
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			String line ;
			
			Configuration config = context.getConfiguration() ;
			FileSystem fs = FileSystem.get( config ) ;
			FileStatus[] status = fs.listStatus( new Path( config.get( "3tuplesCount" ) ) ) ;
			
			for ( int i = 0 ; i < status.length ; i ++ )
			{
				// Open all files under the specific folder ( "3tuplesCount", job1 output )
				BufferedReader br = new BufferedReader( new InputStreamReader(fs.open(status[i].getPath())) ) ;
				// input format
				// Protocol,srcIP>dstIP    count
				
				line = br.readLine() ;
				// test
				//System.out.println( "line = " + line ) ;
				while ( line != null )
				{
					// HashMap<K,V> , K : Protocol,srcIP>dstIP , V : count
					tuplesCount.put( line.split("\t")[0], line.split("\t")[1] ) ;
					line = br.readLine() ;
					// test
					//System.out.println( "line = " + line ) ;
				}
			}
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																index
			// time	   Protocol    SrcIP:SrcPort>DstIP:DstPort						0~2
			// S2D_noP  S2D_noB  S2D_Byte_Max  S2D_Byte_Min  S2D_Byte_Mean			3~7
			// D2S_noP  D2S_noB  D2S_Byte_Max  D2S_Byte_Min  D2S_Byte_Mean			8~12
			// ToT_noP  ToT_noB  ToT_Byte_Max  ToT_Byte_Min  ToT_Byte_Mean			13~17
			// ToT_Byte_STD  ToT_Prate  ToT_Brate  ToT_BTransferRatio  DUR  Loss	18~23
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			if ( subLine.length == 24 )
			{
				// subLine[2] = SrcIP:SrcPort>DstIP:DstPort
				srcInfo = subLine[2].split(">")[0] ;
				dstInfo = subLine[2].split(">")[1] ;
				srcIP = srcInfo.split(":")[0] ;
				dstIP = dstInfo.split(":")[0] ;
				
				// subLine[1] : Protocol
				interKey.set (	subLine[1] + "," + srcIP + ">" + dstIP + ",FGN:" + 
								tuplesCount.get( subLine[1] + "," + srcIP + ">" + dstIP )	) ;
				interValue.set(	subLine[0] + "\t" + srcIP + "\t" + dstIP + "\t" +
								subLine[3] + "\t" + subLine[4] + "\t" + subLine[5] + "\t" + subLine[6] + "\t" + subLine[7] + "\t" +
								subLine[8] + "\t" + subLine[9] + "\t" + subLine[10]+ "\t" + subLine[11]+ "\t" + subLine[12] + "\t" +
								subLine[13]+ "\t" + subLine[14]+ "\t" + subLine[15]+ "\t" + subLine[16]+ "\t" + subLine[17] + "\t" +
								subLine[18]+ "\t" + subLine[19]+ "\t" + subLine[20]+ "\t" + subLine[21]+ "\t" + subLine[22] + "\t" +
								subLine[23] ) ;
				context.write ( interKey, interValue ) ;
				// output format
				// key :
				// Protocol,srcIP>dstIP,FGN:#
				// value :
				// time    SrcIP    DstIP
				// S2D_noP  S2D_noB  S2D_Byte_Max  S2D_Byte_Min  S2D_Byte_Mean
				// D2S_noP  D2S_noB  D2S_Byte_Max  D2S_Byte_Min  D2S_Byte_Mean
				// ToT_noP  ToT_noB  ToT_Byte_Max  ToT_Byte_Min  ToT_Byte_Mean
				// ToT_Byte_STD  ToT_Prate  ToT_Brate  ToT_BTransferRatio  DUR  Loss
				
				// test
				//System.out.println( "interKey = " + interKey.toString() ) ;
				//System.out.println( "interValue = " + interValue.toString() ) ;
			}
		}
	}
	
	public static class job2Reducer extends Reducer <Text, Text, Text, Text>
	{
		// values from command line
		//private float similarity ;
		private int minPts ;
		private double distance ;
		
		
		// list of flows with the same 3-tuples
		private ArrayList<flowGroupFS> flowList = new ArrayList<flowGroupFS>();
		// list of neighbors of p
		private ArrayList<Integer> neighborList = new ArrayList<Integer>();
		// cluster ID
		private int clusterID = 0 ;
		// index of flow in the flow list
		private int index ;
		
		
		// input value reader
		private String[] feature ;
		
		
		// normalize variable : feature vector max, min
		private double[] fvMax = new double[20] ;
		private double[] fvMin = new double[20] ;
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
			// Get the parameter from -D command line option
			// Hadoop will parse the -D similarity=xxx and set the value, and then call run()
			//similarity = Float.parseFloat( config.get("similarity") ) ;
			minPts = Integer.parseInt( config.get("repeatedlyConnectThreshold") ) ;
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
			// Protocol,srcIP>dstIP,FGN:#
			// value :																index
			// time    SrcIP    DstIP												0~2
			// S2D_noP  S2D_noB  S2D_Byte_Max  S2D_Byte_Min  S2D_Byte_Mean			3~7
			// D2S_noP  D2S_noB  D2S_Byte_Max  D2S_Byte_Min  D2S_Byte_Mean			8~12
			// ToT_noP  ToT_noB  ToT_Byte_Max  ToT_Byte_Min  ToT_Byte_Mean			13~17
			// ToT_Byte_STD  ToT_Prate  ToT_Brate  ToT_BTransferRatio  DUR  Loss	18~23
			
			
			// construct a list to store the flows with the same 3-tuples
			flowList.clear() ;
			for ( Text val : values )
			{
				feature = val.toString().split("\t") ;
				
				flowList.add ( new flowGroupFS ( feature[0], feature[1], feature[2],
												feature[3], feature[4], feature[5], feature[6], feature[7],
												feature[8], feature[9], feature[10], feature[11], feature[12],
												feature[13], feature[14], feature[15], feature[16], feature[17], feature[18],
												feature[19], feature[20], feature[21], feature[22], feature[23] ) ) ;
			}
			// test
			//System.out.println( "flowList size = " + flowList.size() ) ;
			
			
			// group similar flows in the flow list
			for ( flowGroupFS p : flowList )
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
						// Protocol,srcIP>dstIP,FGN:#,G#
						// value :
						// time,
						// S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean,
						// D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean,
						// ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean,
						// ToT_Byte_STD, ToT_Prate, ToT_Brate, ToT_BTransferRatio, DUR, Loss
						
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
		public void getNeighbors ( flowGroupFS p, ArrayList<flowGroupFS> list, ArrayList<Integer> neighbors )
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
			for ( flowGroupFS q : list )
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
			for ( int i = 0 ; i < 20 ; i ++ )
			{
				normalFV[i] = (normalFV[i] - fvMin[i]) / (fvMax[i] - fvMin[i]) * 100 ;
			}
		}
	}
	
	
	public static class job3Mapper extends Mapper <LongWritable, Text, compositeKey , Text>
	{
		// input value reader
		private String line ;
		private String[] subLine ;
		
		// timestamp
		private long time ;
		
		// map intermediate key
		private compositeKey comKey = new compositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// LongWritable
			// value :																index (split by tab)
			// Protocol,srcIP>dstIP,FGN:#,G#										0
			// time,																1
			// S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean,
			// D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean,
			// ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean,
			// ToT_Byte_STD, ToT_Prate, ToT_Brate, ToT_BTransferRatio, DUR, Loss
			
			
			// test
			//System.out.println( "key = " + key.toString() ) ;
			//System.out.println( "value = " + value.toString() ) ;
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			
			if ( subLine.length == 2 )
			{
				time = Long.parseLong( subLine[1].split(",")[0] ) ;
				
				comKey.setTuples( subLine[0] ) ; // Protocol,srcIP>dstIP,FGN:#,G#
				comKey.setTime( time ) ;
				interValue.set( subLine[1] ) ;
				context.write( comKey, interValue ) ;
			}
		}
	}
	
	public static class job3Reducer extends Reducer <compositeKey, Text, Text, Text>
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
		private String[] feature ;
		// counter
		private int count ;
		// timestamp
		private long time ;
		
		// IAT list : store the time interval between flow records
		// IAT = inter arrival time
		private ArrayList<Long> intervalList = new ArrayList<Long>();
		// IAT distribution variable
		private double IAT_Max ;
		private double IAT_Min ;
		private double IAT_Mean ;
		private double IAT_STD ;
		// check if IAT has periodicity
		private String checkPER ;
		
		// reduce output key and value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( compositeKey key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key :
			// compositeKey = ( Protocol,srcIP>dstIP,FGN:#,G# ; time )
			// value :																	index
			// time,																	0
			// S2D_noP, S2D_noB, S2D_Byte_Max, S2D_Byte_Min, S2D_Byte_Mean,				1~5
			// D2S_noP, D2S_noB, D2S_Byte_Max, D2S_Byte_Min, D2S_Byte_Mean,				6~10
			// ToT_noP, ToT_noB, ToT_Byte_Max, ToT_Byte_Min, ToT_Byte_Mean,				11~15
			// ToT_Byte_STD, ToT_Prate, ToT_Brate, ToT_BTransferRatio, DUR, Loss		16~21
			
			
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
			
			IAT_Max = Long.MIN_VALUE ;
			IAT_Min = Long.MAX_VALUE ;
			IAT_Mean = 0d ;
			IAT_STD = 0d ;
			
			count = 0 ;
			time = 0 ;
			intervalList.clear() ;
			
			
			Iterator<Text> val = values.iterator() ;
			while ( val.hasNext() )
			{
				count ++ ;
				
				feature = val.next().toString().split(",") ;
				
				S2D_noP += Double.parseDouble(feature[1]) ;
				S2D_noB += Double.parseDouble(feature[2]) ;
				S2D_Byte_Max += Double.parseDouble(feature[3]) ;
				S2D_Byte_Min += Double.parseDouble(feature[4]) ;
				S2D_Byte_Mean += Double.parseDouble(feature[5]) ;
				
				D2S_noP += Double.parseDouble(feature[6]) ;
				D2S_noB += Double.parseDouble(feature[7]) ;
				D2S_Byte_Max += Double.parseDouble(feature[8]) ;
				D2S_Byte_Min += Double.parseDouble(feature[9]) ;
				D2S_Byte_Mean += Double.parseDouble(feature[10]) ;
				
				ToT_noP += Double.parseDouble(feature[11]) ;
				ToT_noB += Double.parseDouble(feature[12]) ;
				ToT_Byte_Max += Double.parseDouble(feature[13]) ;
				ToT_Byte_Min += Double.parseDouble(feature[14]) ;
				ToT_Byte_Mean += Double.parseDouble(feature[15]) ;
				ToT_Byte_STD += Double.parseDouble(feature[16]) ;
				
				ToT_Prate += Double.parseDouble(feature[17]) ;
				ToT_Brate += Double.parseDouble(feature[18]) ;
				ToT_BTransferRatio += Double.parseDouble(feature[19]) ;
				DUR += Double.parseDouble(feature[20]) ;
				
				
				// Store time interval
				// current "time" (ms) minus previous "time" (ms)
				if ( time != 0 )
				{
					intervalList.add( Long.parseLong(feature[0]) - time ) ;
					// test
					//System.out.println( "interval = " + ( Long.parseLong(feature[0]) - time ) ) ;
				}
				time = Long.parseLong(feature[0]) ;
				// test
				//System.out.println( "time = " + time ) ;
			}
			// test
			//System.out.println( "count = " + count ) ;
			//System.out.println( "interval list size = " + intervalList.size() ) ;
			
			
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
			
			
			// check if the IAT with periodicity or not
			if ( intervalList.size() != 0 )
			{
				checkPER = "wPER" ; // wPER : with Periodicity
				// test
				//System.out.println( "with Periodicity" ) ;
			}
			else
			{
				checkPER = "woPER" ; // woPER : without Periodicity
				// test
				//System.out.println( "without Periodicity" ) ;
			}
			
			
			// calculate IAT distribution
			DataDistribution intervalList_D = new DataDistribution(intervalList) ;
			IAT_Max = intervalList_D.getMax() ;
			IAT_Min = intervalList_D.getMin() ;
			IAT_Mean = round( intervalList_D.getMean() , 5 ) ;
			IAT_STD = round( intervalList_D.getStd() , 5 ) ;
			// test
			//System.out.println( "IAT_Max = " + IAT_Max ) ;
			//System.out.println( "IAT_Min = " + IAT_Min ) ;
			//System.out.println( "IAT_Mean = " + IAT_Mean ) ;
			//System.out.println( "IAT_STD = " + IAT_STD ) ;
			
			
			outputKey.set( key.k1toString() ) ; // key : Protocol,srcIP>dstIP,FGN:#,G#
			outputValue.set(	checkPER+"\t"+
								S2D_noP+"\t"+S2D_noB+"\t"+S2D_Byte_Max+"\t"+S2D_Byte_Min+"\t"+S2D_Byte_Mean+"\t"+
								D2S_noP+"\t"+D2S_noB+"\t"+D2S_Byte_Max+"\t"+D2S_Byte_Min+"\t"+D2S_Byte_Mean+"\t"+
								ToT_noP+"\t"+ToT_noB+"\t"+ToT_Byte_Max+"\t"+ToT_Byte_Min+"\t"+ToT_Byte_Mean+"\t"+ToT_Byte_STD+"\t"+
								ToT_Prate+"\t"+ToT_Brate+"\t"+ToT_BTransferRatio+"\t"+DUR+"\t"+
								IAT_Max+"\t"+IAT_Min+"\t"+IAT_Mean+"\t"+IAT_STD		);
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
		if ( otherArgs.length < 2 )
		{
			System.out.println( "GroupPhase1MR: <in> <out>" ) ;
			System.exit(2);
		}
		
		// job 0
		// find max and min FV value
		Job job0 = Job.getInstance( conf, "Group 1 - job0" ) ;
		job0.setJarByClass( GroupPhase1MR.class ) ;
		
		job0.setMapperClass( FindFVMaxMinMapper.class ) ;
		job0.setReducerClass( FindFVMaxMinReducer.class ) ;
		
		job0.setMapOutputKeyClass( Text.class ) ;
		job0.setMapOutputValueClass( Text.class ) ;
		
		job0.setOutputKeyClass( Text.class ) ;
		job0.setOutputValueClass( Text.class ) ;
		
		FileInputFormat.addInputPath( job0, new Path( args[0] ) ) ;
		FileOutputFormat.setOutputPath( job0, new Path( args[1] + "_FVMaxMin" ) ) ;
		
		// compare every map output max and min, so it can set only one reduce
		job0.setNumReduceTasks(1) ;
		
		job0.waitForCompletion( true ) ;
		
		/*------------------------------------------------------------------------------------------*
		 *	Job 1		 																			*
		 *	Calculate the number of Flow Group with the same 3-tuples ( protocol, SrcIP, DstIP )	*
		 *------------------------------------------------------------------------------------------*/
		Job job1 = Job.getInstance( conf, "Group 1 - job1" ) ;
		job1.setJarByClass(GroupPhase1MR.class) ;
		
		job1.setMapperClass(job1Mapper.class) ;
		job1.setReducerClass(job1Reducer.class) ;
		
		job1.setMapOutputKeyClass(Text.class) ;
		job1.setMapOutputValueClass(NullWritable.class) ;
		
		job1.setOutputKeyClass(Text.class) ;
		job1.setOutputValueClass(LongWritable.class) ;
		
		FileInputFormat.addInputPath( job1, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath( job1, new Path(args[1] + "_job1_3tuplesCount") ) ;
		
		job1.waitForCompletion(true) ;
		/*------------------------------------------------------------------*
		 *	Job 2		 													*
		 *	group similar feature vectors with the same 3-tuples			*
		 *------------------------------------------------------------------*/
		// set a name for the output folder of the job1 output
		conf.set( "3tuplesCount", args[1] + "_job1_3tuplesCount") ;
		// set a name for the FV max, min output
		conf.set( "FVMaxMin", args[1] + "_FVMaxMin" ) ;
		
		Job job2 = Job.getInstance( conf, "Group 1 - job2" ) ;
		job2.setJarByClass(GroupPhase1MR.class) ;
		
		job2.setMapperClass(job2Mapper.class) ;
		job2.setReducerClass(job2Reducer.class) ;
		
		job2.setMapOutputKeyClass(Text.class) ;
		job2.setMapOutputValueClass(Text.class) ;
		
		job2.setOutputKeyClass(Text.class) ;
		job2.setOutputValueClass(Text.class) ;
		
		FileInputFormat.addInputPath( job2, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath( job2, new Path(args[1] + "_job2_DBScan") ) ;
		
		job2.waitForCompletion(true) ;
		/*----------------------------------------------------------------------*
		 *	Job 3		 														*
		 *	Calculate average FV of the grouped 3-tuples and get IAT features	*
		 *----------------------------------------------------------------------*/
		Job job3 = Job.getInstance( conf, "Group 1 - job3" ) ;
		job3.setJarByClass(GroupPhase1MR.class) ;
		
		job3.setMapperClass(job3Mapper.class) ;
		job3.setReducerClass(job3Reducer.class) ;
		
		// key class : compositeKey.class
		job3.setMapOutputKeyClass(compositeKey.class) ;
		job3.setMapOutputValueClass(Text.class) ;
		
		job3.setOutputKeyClass(Text.class) ;
		job3.setOutputValueClass(Text.class) ;
		
		// Partitions the key space
		job3.setPartitionerClass(keyPartitioner.class) ;
		// Define the comparator that controls how the keys are sorted before they are passed to the Reducer
		job3.setSortComparatorClass(compositeKeyComparator.class) ;
		// Define the comparator that controls which keys are grouped together for a single call to Reducer
		job3.setGroupingComparatorClass(keyGroupingComparator.class) ;
		
		FileInputFormat.addInputPath( job3, new Path(args[1] + "_job2_DBScan") ) ;
		FileOutputFormat.setOutputPath( job3, new Path(args[1]) ) ;
		
		return job3.waitForCompletion(true) ? 0 : 1 ;
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
