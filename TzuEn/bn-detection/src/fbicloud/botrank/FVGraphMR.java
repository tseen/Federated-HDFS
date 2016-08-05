package fbicloud.botrank ;

import fbicloud.utils.FGInfo ;
import fbicloud.utils.SimilarityFunction ;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileSystem ;
import org.apache.hadoop.fs.FileStatus ;
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.DoubleWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.util.Tool ;
import org.apache.hadoop.util.ToolRunner ;


public class FVGraphMR extends Configured implements Tool
{
	public static class job1Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable>
	{
		// values from command line
		private double distance ;
		
		// store all flow groups
		private ArrayList<FGInfo> allFVGroup = new ArrayList<FGInfo>() ;
		
		// calculate similarity
		private SimilarityFunction sf = new SimilarityFunction() ;
		
		// normalization variable : feature vector max, min
		private final int numOfFeature = 20 ;
		private double[] fvMax = new double[numOfFeature] ;
		private double[] fvMin = new double[numOfFeature] ;
		
		// map inter key, value
		private Text interKey = new Text() ;
		private DoubleWritable interValue = new DoubleWritable() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			distance = Double.parseDouble( config.get( "distance", "1.5" ) ) ;
			// test
			System.out.println( "distance = " + distance ) ;
			
			
			// input value reader
			String line ;
			String[] subLine ;
			// feature vectors
			String[] strFeatures ;
			
			FileSystem fs = FileSystem.get ( config ) ;
			FileStatus[] status = fs.listStatus ( new Path ( config.get("FGInfo") ) ) ;
			
			// store all flow groups info
			for ( int i = 0 ; i < status.length ; i++ )
			{
				BufferedReader br = new BufferedReader ( new InputStreamReader ( fs.open ( status[i].getPath() ) ) ) ;
				
				// input format							index
				// FVID    protocol,Gx					0,1
				// feature vectors						2
				
				line = br.readLine() ;
				while ( line != null )
				{
					subLine = line.split ( "\t" ) ;
					String fvid = subLine[0] ;
					strFeatures = subLine[2].split( "," ) ;
					double[] features = new double[numOfFeature] ;
					
					for ( int j = 0 ; j < numOfFeature ; j ++ )
					{
						features[j] = Double.parseDouble ( strFeatures[j] ) ;
					}
					allFVGroup.add ( new FGInfo ( fvid, features ) ) ;
					
					line = br.readLine() ;
				}
			}
			
			
			// load feature vectors max, min ( for normalization )
			FileStatus[] status2 = fs.listStatus( new Path( config.get("MaxMinFV") ) ) ;
			int offset = 1 ;
			
			for ( int j = 0 ; j < status2.length ; j ++ )
			{
				BufferedReader br = new BufferedReader( new InputStreamReader( fs.open(status2[j].getPath()) ) ) ;
				// input format		index
				// max/min			0
				// 20 FV			1~20
				
				line = br.readLine() ;
				
				while ( line != null )
				{
					subLine = line.split("\t") ;
					for ( int i = 0 ; i < numOfFeature ; i ++ )
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
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format						index
			// FVID    protocol,Gx				0,1
			// feature vectors					2
			
			
			// input value reader
			String line = value.toString() ;
			String[] subLine = line.split( "\t" ) ;
			
			// flow group ID
			String currentID = subLine[0] ;
			// feature vectors
			String[] strFeatures = subLine[2].split( "," ) ;
			double[] features = new double[numOfFeature] ;
			
			for ( int j = 0 ; j < strFeatures.length ; j ++ )
			{
				features[j] = Double.parseDouble ( strFeatures[j] ) ;
			}
			
			// copy the feature vectors for normalization
			double[] pFV = Arrays.copyOf ( features, features.length ) ;
			double[] qFV ;
			// normalize
			normalize ( pFV ) ;
			
			
			// calculate similarity with other flow groups
			int flag ;
			for ( int i = 0 ; i < allFVGroup.size() ; i ++ )
			{
				// test
				//System.out.println( "compare = " + currentID.compareTo( allFVGroup.get(i).getID() ) ) ;
				if ( currentID.compareTo( allFVGroup.get(i).getID() ) != 0 )
				{
					// test
					//System.out.println( currentID + "," + allFVGroup.get(i).getID() ) ;
					//System.out.println( "features[0] = " + features[0] ) ;
					//System.out.println( "allFVGroup.get(i).getFeatures()[0] = " + allFVGroup.get(i).getFeatures()[0] ) ;
					
					// copy the feature vectors for normalization
					qFV = Arrays.copyOf ( allFVGroup.get(i).getFeatures(), allFVGroup.get(i).getFeatures().length ) ;
					// normalize
					normalize ( qFV ) ;
					
					flag = sf.euclideanDistanceSelectedFV ( pFV, qFV, distance ) ;
					
					// flag = 1 -> similar ; flag = 0 -> not similar
					if ( flag > 0 )
					{
						//interKey.set ( String.valueOf ( currentID ) + "," + String.valueOf ( allFVGroup.get(i).getID() ) ) ;
						interKey.set ( currentID + "," + allFVGroup.get(i).getID() ) ;
						interValue.set ( 0 ) ;
						context.write ( interKey, interValue ) ;
					}
				}
			}
		}
		
		// normalize feature vectors
		public void normalize ( double[] normalFV )
		{
			for ( int i = 0 ; i < numOfFeature ; i ++ )
			{
				normalFV[i] = (normalFV[i] - fvMin[i]) / (fvMax[i] - fvMin[i]) * 100 ;
			}
		}
	}
	
	
	public static class job2Mapper extends Mapper <LongWritable, Text, Text , Text>
	{
		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// FVID1,FVID2    [initial score]
			
			String line = value.toString() ;
			String[] subLine = line.split ( "\t" ) ;
			String[] fvid = subLine[0].split(",");
			
			if ( subLine.length == 2 )
			{
				interKey.set ( fvid[0] ) ;
				interValue.set ( fvid[1] ) ;
				context.write ( interKey, interValue ) ;
				
				//interKey.set ( fvid[1] ) ;
				//interValue.set ( fvid[0] ) ;
				//context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class job2Reducer extends Reducer <Text, Text, Text, Text>
	{
		// reduce output key, value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			//Input Format :
			//		Key		: 	FVID1
			//		Value	:	FVID2
			
			// TreeSet : ordered set
			Set<String> FVSet = new TreeSet<String>() ;
			StringBuffer outsb = new StringBuffer() ;
			
			for ( Text val : values )
			{
				FVSet.add ( val.toString() ) ;
			}
			for ( String FV : FVSet )
			{
				outsb.append ( FV + "," ) ;
			}
			
			outputKey.set ( key ) ;
			outputValue.set ( outsb.toString() ) ;
			context.write ( outputKey, outputValue ) ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		if ( otherArgs.length != 3 )
		{
			System.out.println ("FVGraphMR: <MaxMinFV> <in> <out>") ;
			System.exit(2) ;
		}
		
		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	Calculate Initial Score of two FVIDs							*
		 *------------------------------------------------------------------*/
		
		// feature vectors max, min
		conf.set ( "MaxMinFV", args[0] + "_MaxMinFV" ) ;
		// flow group info
		conf.set ( "FGInfo", args[1] ) ;
		
		
		Job job1 = Job.getInstance ( conf, "FVGraph - job1" ) ;
		job1.setJarByClass ( FVGraphMR.class ) ;
		
		job1.setMapperClass ( job1Mapper.class ) ;
		
		job1.setMapOutputKeyClass ( Text.class ) ;
		job1.setMapOutputValueClass ( DoubleWritable.class ) ;
		
		FileInputFormat.addInputPath ( job1, new Path ( args[1] ) ) ;
		FileOutputFormat.setOutputPath ( job1, new Path ( args[2] + "_job1_initialScore" ) ) ;
		
		// map only job
		job1.setNumReduceTasks(0) ;
		
		job1.waitForCompletion ( true ) ;
		//return job1.waitForCompletion(true) ? 0 : 1 ;
		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Get Adjacency list												*
		 *------------------------------------------------------------------*/
		
		Job job2 = Job.getInstance ( conf, "FVGraph - job2" ) ;
		job2.setJarByClass ( FVGraphMR.class ) ;
		
		job2.setMapperClass ( job2Mapper.class ) ;
		job2.setReducerClass ( job2Reducer.class ) ;
		
		job2.setMapOutputKeyClass ( Text.class ) ;
		job2.setMapOutputValueClass ( Text.class ) ;
		
		job2.setOutputKeyClass ( Text.class ) ;
		job2.setOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job2, new Path ( args[2] + "_job1_initialScore" ) ) ;
		FileOutputFormat.setOutputPath ( job2, new Path ( args[2] ) ) ;
		
		return job2.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new FVGraphMR(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}
