package fbicloud.botrank;


import fbicloud.utils.FVGraphInfo ;

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
		private ArrayList<FVGraphInfo> fvGraph = new ArrayList<FVGraphInfo>() ;
		
		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
	    
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			// input value reader
			String line ;
			String[] subLine ;
			
			Configuration config = context.getConfiguration() ;
			FileSystem fs = FileSystem.get ( config ) ;
			FileStatus[] status = fs.listStatus ( new Path ( config.get ( "FVGraph" ) ) ) ;
			
			for ( int i = 0 ; i < status.length ; i ++ )
			{
				// input format
				// FVID    FVID-A,FVID-B,FVID-C ...
				
				BufferedReader br = new BufferedReader ( new InputStreamReader ( fs.open ( status[i].getPath() ) ) ) ;
				line = br.readLine() ;
				
				while ( line != null )
				{
					subLine = line.split ( "\t" ) ;
					String fvid = subLine[0] ;
					String neighbors = subLine[1] ;
					fvGraph.add ( new FVGraphInfo ( fvid, neighbors ) ) ;
					line = br.readLine() ;
				}
			}
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// FVID    FVID-A,FVID-B,FVID-C ...
			
			String[] subLine = value.toString().split ( "\t" ) ;
			
			String fvid = subLine[0] ;
			
			
			for ( int i = 0 ; i < fvGraph.size() ; i ++ )
			{
				String neighbor = fvGraph.get(i).getNeighbor() ;
				
				if ( fvid.compareTo( fvGraph.get(i).getID() ) < 0 )
				{
					interKey.set ( fvid + "," + fvGraph.get(i).getID() ) ;
				}
				else
				{
					interKey.set ( fvGraph.get(i).getID() + "," + fvid ) ;
				}
				/*else if ( fvid.compareTo( fvGraph.get(i).getID() ) > 0 )
				{
					interKey.set ( fvGraph.get(i).getID() + "," + fvid ) ;
				}
				else
				{
					interKey.set ( fvid + "," + fvid ) ;
				}*/
				interValue.set ( neighbor ) ;
				context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class ContributeListReducer extends Reducer <Text, Text, Text, Text>
	{
		// reduce output key, value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			String[] fvidNeighbor1 = null ;
			String[] fvidNeighbor2 = null ;
			StringBuffer outputStr = new StringBuffer() ;
			
			
			// key
			String[] fvidPair = key.toString().split ( "," ) ;
			
			// case : s(A, A)
			if ( fvidPair[0].equals( fvidPair[1] ) )
			{
				// set s(A, A) = 1
				outputStr.append ( "1" + "\t" ) ;
				
				
				// read neighbor list
				Iterator<Text> valueIterator = values.iterator() ;
				if ( valueIterator.hasNext() )
				{
					String fvidNeighborStr = valueIterator.next().toString() ;
					
					fvidNeighbor1 = fvidNeighborStr.split(",") ;
					fvidNeighbor2 = fvidNeighborStr.split(",") ;
				}
				
				
				Set<String> neighborSet = new TreeSet<String>() ;
				// get neighbor matrix
				for ( int i = 0 ; i < fvidNeighbor1.length ; i ++ )
				{
					for ( int j = 0 ; j < fvidNeighbor2.length ; j ++ )
					{
						if ( ! fvidNeighbor1[i].equals( fvidNeighbor2[j] ) )
						{
							if ( fvidNeighbor1[i].compareTo( fvidNeighbor2[j] ) < 0 )
							{
								neighborSet.add ( fvidNeighbor1[i] + "," + fvidNeighbor2[j] + ";" ) ;
							}
							else
							{
								neighborSet.add ( fvidNeighbor2[j] + "," + fvidNeighbor1[i] + ";" ) ;
							}
						}
					}
				}
				
				// get neighbor set member
				for ( String neighbor : neighborSet )
				{
					outputStr.append ( neighbor ) ;
				}
			}
			// case : s(A, B)
			else
			{
				// set s(A, B) initial score
				outputStr.append ( "0" + "\t" ) ;
				
				
				// read neighbor list
				Iterator<Text> valueIterator = values.iterator() ;
				if ( valueIterator.hasNext() )
				{
					fvidNeighbor1 = valueIterator.next().toString().split ( "," ) ;
				}
				if ( valueIterator.hasNext() )
				{
					fvidNeighbor2 = valueIterator.next().toString().split ( "," ) ;
				}
				
				
				Set<String> neighborSet = new TreeSet<String>() ;
				// get neighbor matrix
				for ( int i = 0 ; i < fvidNeighbor1.length ; i ++ )
				{
					for ( int j = 0 ; j < fvidNeighbor2.length ; j ++ )
					{
						if ( ! fvidNeighbor1[i].equals( fvidNeighbor2[j] ) )
						{
							// The score will not contribute to his own self
							if ( ! ( fvidNeighbor1[i].equals ( fvidPair[0] ) && fvidNeighbor2[j].equals ( fvidPair[1] ) ) )
							{
								if ( ! ( fvidNeighbor1[i].equals ( fvidPair[1] ) && fvidNeighbor2[j].equals ( fvidPair[0] ) ) )
								{
									if ( fvidNeighbor1[i].compareTo( fvidNeighbor2[j] ) < 0 )
									{
										neighborSet.add ( fvidNeighbor1[i] + "," + fvidNeighbor2[j] + ";" ) ;
									}
									else
									{
										neighborSet.add ( fvidNeighbor2[j] + "," + fvidNeighbor1[i] + ";" ) ;
									}
								}
							}
						}
					}
				}
				
				// get neighbor set member
				for ( String neighbor : neighborSet )
				{
					outputStr.append ( neighbor ) ;
				}
			}
			
			outputKey.set ( key ) ;
			outputValue.set ( outputStr.toString() ) ;
			context.write ( outputKey, outputValue ) ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser ( conf, args ).getRemainingArgs() ;
		
		
		
		conf.set ( "FVGraph", args[0] ) ;
		
		Job job = Job.getInstance ( conf, "Contribute List" ) ;
		job.setJarByClass ( ContributeList.class ) ;
		
		job.setMapperClass ( ContributeListMapper.class ) ;
		job.setReducerClass ( ContributeListReducer.class ) ;
		
		job.setMapOutputKeyClass ( Text.class ) ;
		job.setMapOutputValueClass ( Text.class ) ;
		
		job.setOutputKeyClass ( Text.class ) ;
		job.setOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job, new Path ( args[0] ) ) ;
		FileOutputFormat.setOutputPath ( job, new Path ( args[1] ) ) ;
		
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