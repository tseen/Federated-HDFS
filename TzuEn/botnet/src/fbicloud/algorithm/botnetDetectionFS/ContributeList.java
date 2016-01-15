package fbicloud.algorithm.botnetDetectionFS ;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileSystem ;
import org.apache.hadoop.fs.FileStatus ;
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.util.Tool ;
import org.apache.hadoop.util.ToolRunner ;


public class ContributeList extends Configured implements Tool
{
	public static class ContributeListMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
	    
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// FVID    FVID-A,FVID-B,FVID-C ...
			
			String[] subLine = value.toString().split ( "\t" ) ;
			
			int srcGroup = Integer.parseInt ( subLine[0] ) ;
			
			String[] dstGroupStr = subLine[1].split ( "," ) ;
			int[] dstGroup = new int [ dstGroupStr.length ] ;
			
			
			// output Value : neighbor list
			interValue.set ( subLine[1] ) ;
			
			// Case : s(A,C)
			for ( int i = 0 ; i < dstGroupStr.length ; i ++ )
			{
				dstGroup[i] = Integer.parseInt ( dstGroupStr[i] ) ;
				if ( srcGroup < dstGroup[i] )
				{
					interKey.set ( srcGroup + "," + dstGroup[i] ) ;
				}
				else
				{
					interKey.set ( dstGroup[i] + "," + srcGroup ) ;
				}
				context.write ( interKey, interValue ) ;
			}
			
			// Case : s(A,A)
			interKey.set ( srcGroup + "," + srcGroup ) ;
			context.write ( interKey, interValue ) ;
		}
	}
	
	public static class ContributeListReducer extends Reducer <Text, Text, Text, Text>
	{
		private HashMap<String,String> FVIDInitialScore  = new HashMap<>() ;
		
		// reduce output key, value
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			// input value reader
			String line ;
			String[] subLine ;
			
			Configuration config = context.getConfiguration() ;
			FileSystem fs = FileSystem.get ( config ) ;
			FileStatus[] status = fs.listStatus ( new Path ( config.get ( "FVInitialScore" ) ) ) ;
			
			for ( int i = 0 ; i < status.length ; i ++ )
			{
				// input format
				// FVID1,FVID2    [initial score]
				
				BufferedReader br = new BufferedReader ( new InputStreamReader ( fs.open ( status[i].getPath() ) ) ) ;
				line = br.readLine() ;
				
				while ( line != null )
				{
					subLine = line.split ( "\t" ) ;
					FVIDInitialScore.put ( subLine[0], subLine[1] ) ;
					line = br.readLine() ;
				}
			}
		}
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			int i, j ;
			String [] dstGroup1 = null ;
			String [] dstGroup2 = null ;
			StringBuffer outputStr = new StringBuffer() ;
			
			
			// key
			String[] pairStr = key.toString().split ( "," ) ;
			
			// Case : s(A,A) 1
			if ( pairStr[0].equals( pairStr[1] ) )
			{
				// set s(A, A) = 1
				outputStr.append ( "1\t" ) ;
				
				Iterator<Text> valueIterator = values.iterator() ;
				if ( valueIterator.hasNext() )
				{
					dstGroup1 = valueIterator.next().toString().split ( "," ) ;
				}
				
				// Update outputStr
				for ( i = 0 ; i < dstGroup1.length ; i ++ )
				{
					for ( j = i + 1 ; j < dstGroup1.length ; j ++ )
					{
						// GID1,GID2 -> GID1 < GID2
						if ( Integer.parseInt ( dstGroup1[i] ) < Integer.parseInt ( dstGroup1[j] ) )
						{
							outputStr.append ( dstGroup1[i] + "," + dstGroup1[j] + ";" ) ;
						}
						else if ( Integer.parseInt ( dstGroup1[i] ) > Integer.parseInt ( dstGroup1[j] ) )
						{
							outputStr.append ( dstGroup1[j] + "," + dstGroup1[i] + ";" ) ;
						}
					}
				}
			}
			// Case : s(A,B)
			else
			{
				// set s(A, B) initial score
				outputStr.append ( FVIDInitialScore.get ( key.toString() ) + "\t" ) ;
				
				Iterator<Text> valueIterator = values.iterator() ;
				if ( valueIterator.hasNext() )
				{
					dstGroup1 = valueIterator.next().toString().split ( "," ) ;
				}
				if ( valueIterator.hasNext() )
				{
					dstGroup2 = valueIterator.next().toString().split ( "," ) ;
				}
				
				// Update outputStr
				for ( i = 0 ; i < dstGroup1.length ; i ++ )
				{
					for ( j = 0 ; j < dstGroup2.length ; j ++ )
					{
						if ( ! dstGroup1[i].equals( dstGroup2[j] ) )
						{
							// The score will not contribute to his own self
							if ( ! ( dstGroup1[i].equals ( pairStr[0] ) && dstGroup2[j].equals ( pairStr[1] ) ) )
							{
								if ( ! ( dstGroup1[i].equals ( pairStr[1] ) && dstGroup2[j].equals ( pairStr[0] ) ) )
								{
									// GID1,GID2 -> GID1 < GID2 
									if ( Integer.parseInt ( dstGroup1[i] ) < Integer.parseInt ( dstGroup2[j] ) )
									{
										outputStr.append ( dstGroup1[i] + "," + dstGroup2[j] + ";" ) ;
									}
									else
									{
										outputStr.append ( dstGroup2[j] + "," + dstGroup1[i] + ";" ) ;
									}
								}
							}
						}
					}
				}
			}
			
			// Output <key,value>
			// 1001,1001		1	403,404;403,587;403,945;404,587...	
			outputKey.set ( key ) ;						//key		1001,1001
			outputValue.set ( outputStr.toString() ) ;	//value		1	403,404;403,587;403,945;404,587...;
			context.write ( outputKey, outputValue ) ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser ( conf, args ).getRemainingArgs() ;
		
		if ( otherArgs.length < 2 )
		{
			System.out.println ( "ContributeList <FVInitialScore> <in> <out>" ) ;
			System.exit(2) ;
		}
		
		conf.set ( "FVInitialScore", args[0] ) ;
		
		Job job = Job.getInstance ( conf, "Contribute List" ) ;
		job.setJarByClass ( ContributeList.class ) ;
		
		job.setMapperClass ( ContributeListMapper.class ) ;
		job.setReducerClass ( ContributeListReducer.class ) ;
		
		job.setMapOutputKeyClass ( Text.class ) ;
		job.setMapOutputValueClass ( Text.class ) ;
		
		job.setOutputKeyClass ( Text.class ) ;
		job.setOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job, new Path ( args[1] ) ) ;
		FileOutputFormat.setOutputPath ( job, new Path ( args[2] ) ) ;
		
		return job.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new ContributeList(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}
