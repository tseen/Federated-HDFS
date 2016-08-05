package fbicloud.botrank;

import fbicloud.utils.FVInfo ;

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


public class FVGraphMR_IPoverlap extends Configured implements Tool
{
	public static class job1Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable>
	{
		private ArrayList<FVInfo> allFVGroup = new ArrayList<FVInfo>() ;
		
		// map inter key, value
		private Text interKey = new Text() ;
		private DoubleWritable interValue = new DoubleWritable() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			// input value reader
			String line ;
			String[] subLine ;
			//String[] srcIPsArray ;
			String[] dstIPsArray ;
			
			FileSystem fs = FileSystem.get ( config ) ;
			FileStatus[] status = fs.listStatus ( new Path ( config.get("FVInfo") ) ) ;
			
			for ( int i = 0 ; i < status.length ; i++ )
			{
				BufferedReader br = new BufferedReader ( new InputStreamReader ( fs.open ( status[i].getPath() ) ) ) ;
				
				// input format									index
				// FVID    protocol,Gx							0,1
				// [feature vectors]    srcIPs    dstIPs		2~4
				
				line = br.readLine() ;
				while ( line != null )
				{
					subLine = line.split ( "\t" ) ;
					//srcIPsArray = subLine[3].split ( "," ) ;
					dstIPsArray = subLine[4].split ( "," ) ;
					
					Collection<String> IPsCol = new HashSet<String>() ;
					/*for ( int j = 0 ; j < srcIPsArray.length ; j ++ )
					{
						IPsCol.add ( srcIPsArray[j] ) ;
					}*/
					for ( int j = 0 ; j < dstIPsArray.length ; j ++ )
					{
						IPsCol.add ( dstIPsArray[j] ) ;
					}
					allFVGroup.add ( new FVInfo ( Integer.parseInt ( subLine[0] ), IPsCol ) ) ;
					
					line = br.readLine() ;
				}
			}
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format									index
			// FVID    protocol,Gx							0,1
			// [feature vectors]    srcIPs    dstIPs		2~4
			
			int numOfAllIP ;
			double initialScore ;
			
			String[] subLine = value.toString().split("\t");
			int currentID = Integer.parseInt ( subLine[0] ) ;
			//String[] srcIPsArray = subLine[3].split ( "," ) ;
			String[] dstIPsArray = subLine[4].split ( "," ) ;
			
			Collection<String> currentIPsCol = new HashSet<String>() ;
			/*for ( i = 0 ; i < srcIPsArray.length ; i ++ )
			{
				currentIPsCol.add ( srcIPsArray[i] ) ;
			}*/
			for ( int i = 0 ; i < dstIPsArray.length ; i ++ )
			{
				currentIPsCol.add ( dstIPsArray[i] ) ;
			}
			
			
			for ( int i = 0 ; i < allFVGroup.size() ; i ++ )
			{
				if ( currentID < allFVGroup.get(i).getID() )
				{
					// copy collection
					Collection<String> tmpIPsCol = new HashSet<String> ( allFVGroup.get(i).getIPsCol() ) ;
					
					numOfAllIP = currentIPsCol.size() + tmpIPsCol.size() ;
					// get intersection
					tmpIPsCol.retainAll ( currentIPsCol ) ;
					
					// the 2 sets have intersection
					if ( tmpIPsCol.size() != 0 )
					{
						/*--------------------------------------------------*
						 *	Key		FVID1,FVID2								*
						 *	Value	2*(# of overlap IP) / (# of all IPs)	*
						 *--------------------------------------------------*
						 *	Example : 										*
						 *	FVID1 : A,B,C,D		/		FVID2 : C,D,M,N		*
						 *	-> Initial score = 2*(2) / (4+4) = 1/2			*
						 *--------------------------------------------------*/
						interKey.set ( String.valueOf ( currentID ) + "," + String.valueOf ( allFVGroup.get(i).getID() ) ) ;
						initialScore = ( double ) 2 * tmpIPsCol.size() / numOfAllIP ;
						
						interValue.set ( initialScore ) ;
						context.write ( interKey, interValue ) ;
						
						// test
						/*if ( interKey.toString().equals( "4,21" ) )
						{
							System.out.println( "4,21" ) ;
							
							for ( String ip : tmpIPsCol )
							{
								System.out.println( "ip = " + ip ) ;
							}
						}*/
					}
					tmpIPsCol.clear() ;
				}
			}
			currentIPsCol.clear() ;
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
				//If FVID1, FVID2 <FVID1, FVID2>
				//Key	protocol
				//Value	FeatureVector	sIP	dstIPs
				
				interKey.set ( fvid[0] ) ;
				interValue.set ( fvid[1] ) ;
				context.write ( interKey, interValue ) ;
				
				interKey.set ( fvid[1] ) ;
				interValue.set ( fvid[0] ) ;
				context.write ( interKey, interValue ) ;
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
			Set<String> FVSet = new TreeSet<>() ;
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
		if ( otherArgs.length < 2 )
		{
			System.out.println ("FVGraphMR_IPoverlap: <in> <out>") ;
			System.exit(2) ;
		}
		
		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	Calculate Initial Score of two FVIDs							*
		 *------------------------------------------------------------------*/
		
		conf.set ( "FVInfo", args[0] ) ;
		
		Job job1 = Job.getInstance ( conf, "FVGraph - job1" ) ;
		job1.setJarByClass ( FVGraphMR_IPoverlap.class ) ;
		
		job1.setMapperClass ( job1Mapper.class ) ;
		
		job1.setMapOutputKeyClass ( Text.class ) ;
		job1.setMapOutputValueClass ( DoubleWritable.class ) ;
		
		FileInputFormat.addInputPath ( job1, new Path ( args[0] ) ) ;
		FileOutputFormat.setOutputPath ( job1, new Path ( args[1] + "_job1_initialScore" ) ) ;
		
		job1.waitForCompletion ( true ) ;
		
		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Get Adjacency list												*
		 *------------------------------------------------------------------*/
		
		Job job2 = Job.getInstance ( conf, "FVGraph - job2" ) ;
		job2.setJarByClass ( FVGraphMR_IPoverlap.class ) ;
		
		job2.setMapperClass ( job2Mapper.class ) ;
		job2.setReducerClass ( job2Reducer.class ) ;
		
		job2.setMapOutputKeyClass ( Text.class ) ;
		job2.setMapOutputValueClass ( Text.class ) ;
		
		job2.setOutputKeyClass ( Text.class ) ;
		job2.setOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job2, new Path ( args[1] + "_job1_initialScore" ) ) ;
		FileOutputFormat.setOutputPath ( job2, new Path ( args[1] ) ) ;
		
		return job2.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new FVGraphMR_IPoverlap(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}
