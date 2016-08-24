package fbicloud.botrank;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileSystem ;
import org.apache.hadoop.fs.FileStatus ;
import org.apache.hadoop.fs.FSDataOutputStream ;
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


public class MultiDimSimRankMR extends Configured implements Tool
{
	public static class SimRankIterationMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		// length of line
		private final static int lengthOfLine = 3 ;


		// map inter key, value
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;


	    public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format
			// (2 groups)    score    (2 groups);(2 groups);(2 groups);...


			String[] subLine = value.toString().split("\t") ;

			// ### A part ###
			if ( subLine.length == lengthOfLine )
			{
				String fvidPairStr = subLine[0] ;
				String score = subLine[1] ;
				String neighborStr = subLine[2] ;

				String[] fvidPair = fvidPairStr.split(",") ;
				String[] neighbor = neighborStr.split(";") ;


				// case : s(A, A)
				if ( fvidPair[0].equals( fvidPair[1] ) )
				{
					for ( String nei : neighbor )
					{
						interKey.set ( nei ) ;
						interValue.set ( score ) ;
						context.write ( interKey, interValue ) ;
					}
				}
				// case : s(A, B)
				else
				{
					for ( String nei : neighbor )
					{
						interKey.set ( nei ) ;
						interValue.set ( score + "\t" + fvidPairStr ) ;
						context.write ( interKey, interValue ) ;
					}
				}
			}
			// ### A part end ###
		}
	}

	public static class SimRankIterationReducer extends Reducer <Text, Text, Text, Text>
	{
		// reduce output key, value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;


		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			// input format
			// key : fvid pair
			// value
			// score    neighbor


			// test
			//System.out.println( "key = " + key.toString() ) ;

			// ### B part ###
			int count = 0 ;
			double sum = 0.0 ;

			Set<String> neighborSet = new TreeSet<String>() ;
			String neighborStr = "" ;

			for ( Text val : values )
			{
				count ++ ;

				String line = val.toString() ;

				String[] subLine = line.split("\t") ;

				double score = Double.parseDouble( subLine[0] ) ;
				sum += score ;
				// test
				//System.out.println( "score = " + score ) ;

				// neighbor set
				if ( subLine.length == 2 )
				{
					neighborSet.add( subLine[1] ) ;
				}
			}

			// get neighbor set member
			for ( String neighbor : neighborSet )
			{
				neighborStr += neighbor + ";" ;
			}

			if ( count > 0 )
			{
				double simrankScore = 0.6 * sum / count ;
				// test
				//System.out.println( "sum = " + sum ) ;
				//System.out.println( "count = " + count ) ;
				//System.out.println( "simrankScore = " + simrankScore ) ;

				outputKey.set ( key ) ;
				outputValue.set ( simrankScore + "\t" + neighborStr ) ;
				context.write ( outputKey, outputValue ) ;
			}
			// ### B part end ###
		}
	}


	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;

		String[] otherArgs = new GenericOptionsParser ( conf, args ).getRemainingArgs() ;

		if ( otherArgs.length != 2 )
		{
			System.out.println ( "MultiDimSimRankMR <contributeList> <out>" ) ;
			System.exit(2) ;
		}

		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	SimRank First Iteration											*
		 *------------------------------------------------------------------*/


		// Need to set 'conf' before creates a new Job (job.getInstance)
		Job job1 = Job.getInstance ( conf, "Multidimensional SimRank iteration 1" ) ;
		job1.setJarByClass ( MultiDimSimRankMR.class ) ;

		// Set Mapper, Reducer class
		job1.setMapperClass ( SimRankIterationMapper.class ) ;
		job1.setReducerClass ( SimRankIterationReducer.class ) ;

		job1.setMapOutputKeyClass ( Text.class ) ;
		job1.setMapOutputValueClass ( Text.class ) ;

		job1.setOutputKeyClass ( Text.class ) ;
		job1.setOutputValueClass ( Text.class ) ;

		// Set input, output path
		FileInputFormat.addInputPath ( job1, new Path ( args[0] ) ) ;
		FileOutputFormat.setOutputPath ( job1, new Path ( args[1] ) ) ;

		return job1.waitForCompletion ( true ) ? 0 : 1 ;
	}

	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new MultiDimSimRankMR(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}
