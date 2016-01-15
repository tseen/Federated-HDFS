package fbicloud.algorithm.botnetDetectionFS;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FilterPhase2MR_3 extends Configured implements Tool
{
	
	
	public static class job3Mapper extends Mapper <LongWritable, Text, NullWritable, Text>
	{
		private ArrayList<String> IPList = new ArrayList<String>() ;
		
		// map intermediate value
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			String line ;
			Configuration config = context.getConfiguration() ;
			
			FileSystem fs = FileSystem.get ( config ) ;
			FileStatus[] status = fs.listStatus ( new Path ( config.get("HighFCIP") ) ) ;
			for ( int i = 0 ; i < status.length ; i ++ )
			{
				BufferedReader br = new BufferedReader ( new InputStreamReader(fs.open(status[i].getPath())) ) ;
				line = br.readLine() ;
				while ( line != null )
				{
					IPList.add ( line.split("\t")[0] ) ;
					line = br.readLine() ;
				}
			}
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																		index
			// time    Protocol    SrcIP:SrcPort>DstIP:DstPort								0~2
			// S2D_noP  S2D_noB  S2D_Byte_Max  S2D_Byte_Min  S2D_Byte_Mean					3~7
			// D2S_noP  D2S_noB  D2S_Byte_Max  D2S_Byte_Min  D2S_Byte_Mean					8~12
			// ToT_noP  ToT_noB  ToT_Byte_Max  ToT_Byte_Min  ToT_Byte_Mean  ToT_Byte_STD	13~18
			// ToT_Prate  ToT_Brate  ToT_BTransferRatio  DUR  Loss							19~23
			
			
			String srcIP ;
			String line = value.toString() ;
			String[] str = line.split("\t") ;
			if ( str.length == 24 )
			{
				srcIP = str[2].split(":")[0] ;
				
				
				if ( IPList.contains(srcIP) && str[23].equals("0") )
				{
					interValue.set(	str[0]+ "\t"+str[1]+ "\t"+str[2]+ "\t"+str[3]+ "\t"+
									str[4]+ "\t"+str[5]+ "\t"+str[6]+ "\t"+str[7]+ "\t"+
									str[8]+ "\t"+str[9]+ "\t"+str[10]+"\t"+str[11]+"\t"+
									str[12]+"\t"+str[13]+"\t"+str[14]+"\t"+str[15]+"\t"+
									str[16]+"\t"+str[17]+"\t"+str[18]+"\t"+str[19]+"\t"+
									str[20]+"\t"+str[21]+"\t"+str[22]+"\t"+str[23]);
					
					context.write ( NullWritable.get(), interValue ) ;
				}
			}
		}
	}
	
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2 ) {
			System.out.println("FilterPhase2MR_3: <in> <out>");
			System.out.println("otherArgs.length = "+otherArgs.length);
			System.exit(2);
		}
		
		
		/*----------------------------------------------------------------------------------------------*
		 *								Job 3		 													*
		 *	Remove low failed connection hosts and divide into Unidirectional/Bidirectional flow Group	*
		 *----------------------------------------------------------------------------------------------*/
		
		conf.set ( "HighFCIP", args[0] + "/HighFCIP" ) ;
		
		Job job3 = Job.getInstance ( conf, "Filter 2 - job 3" ) ;
		job3.setJarByClass ( FilterPhase2MR.class ) ;
		
		job3.setMapperClass ( job3Mapper.class ) ;
		
		job3.setMapOutputKeyClass ( NullWritable.class ) ;
		job3.setMapOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job3, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath( job3, new Path(args[1]) ) ;
		
		// map only job
		job3.setNumReduceTasks(0) ;
		
		return job3.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new FilterPhase2MR(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit ( res ) ;
	}
}

