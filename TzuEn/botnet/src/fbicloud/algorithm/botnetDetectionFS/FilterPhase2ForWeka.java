package fbicloud.algorithm.botnetDetectionFS ;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileSystem ;
import org.apache.hadoop.fs.FileStatus ;
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.IntWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.io.DoubleWritable ;
import org.apache.hadoop.io.NullWritable ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs ;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.util.Tool ;
import org.apache.hadoop.util.ToolRunner ;


public class FilterPhase2ForWeka extends Configured implements Tool
{
	public static class job1Mapper extends Mapper <LongWritable, Text, Text, IntWritable>
	{
		// map intermediate key and value
		private Text interKey = new Text() ;
		private IntWritable interValue = new IntWritable() ;
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// value :																	index
			// timestamp    Protocol    SrcIP:SrcPort>DstIP:DstPort						0~2
			// S2D_noP    S2D_noB														3~4
			// S2D_Byte_Max    S2D_Byte_Min    S2D_Byte_Mean    S2D_Byte_STD			5~8
			// S2D_IAT_Max    S2D_IAT_Min    S2D_IAT_Mean    S2D_IAT_STD				9~12
			// D2S_noP    D2S_noB														13~14
			// D2S_Byte_Max    D2S_Byte_Min    D2S_Byte_Mean    D2S_Byte_STD			15~18
			// D2S_IAT_Max    D2S_IAT_Min    D2S_IAT_Mean    D2S_IAT_STD				19~22
			// ToT_noP    ToT_noB														23~24
			// ToT_Byte_Max    ToT_Byte_Min    ToT_Byte_Mean    ToT_Byte_STD			25~28
			// ToT_IAT_Max    ToT_IAT_Min    ToT_IAT_Mean    ToT_IAT_STD				29~32
			// ToT_Prate    ToT_Brate    ToT_PTransferRatio    ToT_BTransferRatio		33~36
			// DUR    Loss																37~38
			
			
			String srcInfo, srcIP ;
			String dstInfo, dstIP ;
			String line = value.toString() ;
			String[] str = line.split("\t") ;
			
			if ( str.length == 39 )
			{
				srcInfo = str[2].split(">")[0] ; // SrcIP:SrcPort
				dstInfo = str[2].split(">")[1] ; // DstIP:DstPort
				srcIP = srcInfo.split(":")[0] ; // SrcIP
				dstIP = dstInfo.split(":")[0] ; // DstIP
				
				interKey.set ( srcIP + ">" + dstIP ) ;
				interValue.set ( Integer.parseInt(str[38]) ) ;
				context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class job1Reducer extends Reducer <Text, IntWritable, Text, DoubleWritable>
	{
		private DoubleWritable outputValue = new DoubleWritable() ;
		
		public void reduce ( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
		{
			long noOfFG = 0 ;				// number of Flow Group(FG) 
			long noOfFGWOResponsed = 0 ;	// number of Flow Group(FG) without response
			
			for ( IntWritable val : values )
			{
				noOfFG ++ ;
				noOfFGWOResponsed += val.get();
			}
			outputValue.set ( (double) noOfFGWOResponsed / noOfFG ) ;
			context.write ( key, outputValue ) ;
		}
	}
	
	
	public static class job2Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable>
	{
		private Text interKey = new Text() ;
		private DoubleWritable interValue = new DoubleWritable() ;
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// SrcIP>DstIP FCBH (Failed Connection between hosts)
			
			
			String srcIP ;
			String line = value.toString() ;
			String[] str = line.split("\t") ;
			
			if ( str.length == 2 )
			{
				srcIP = str[0].split(">")[0] ;
				interKey.set ( srcIP ) ;
				interValue.set ( Double.parseDouble(str[1]) ) ;
				context.write ( interKey, interValue ) ;
			}
		}
	}
	
	public static class job2Reducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable>
	{
		private double flowlossratio = 0 ;
		private MultipleOutputs<Text, DoubleWritable> mos ;
		private DoubleWritable outputValue = new DoubleWritable() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			// Multiple output Directory
			mos = new MultipleOutputs<Text, DoubleWritable>(context) ;
			flowlossratio = Double.parseDouble ( config.get ( "flowlossratio" ) ) ;
		}
		
		public void cleanup ( Context context ) throws IOException, InterruptedException
		{
			mos.close() ;
		}
		
		public void reduce ( Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException
		{
			// Set output folder
			// output
			//		LowFCIP
			//				IP-m-00000
			//				IP-m-00001
			//		HighFCIP
			//				IP-m-00000
			//				IP-m-00001
			
			String LowFCIP_OutFileName = "LowFCIP/IP" ;
			String HighFCIP_OutFileName = "HighFCIP/IP" ;
			
			long noOfdstIPs = 0;		// number of destination IPs
			double sumOfFC = 0;			// Total Flow Loss Rate
			
			for ( DoubleWritable val : values )
			{
				noOfdstIPs ++ ;
				sumOfFC += val.get() ;
			}
			
			outputValue.set ( sumOfFC / noOfdstIPs ) ;
			
			if ( sumOfFC/noOfdstIPs > flowlossratio )
			{
				mos.write ( "HighFCIP", key, outputValue, HighFCIP_OutFileName ) ;
			}
			else
			{
				mos.write ( "LowFCIP", key, outputValue, LowFCIP_OutFileName ) ;
			}
		}
	}
	
	
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
			// value :																	index
			// timestamp    Protocol    SrcIP:SrcPort>DstIP:DstPort						0~2
			// S2D_noP    S2D_noB														3~4
			// S2D_Byte_Max    S2D_Byte_Min    S2D_Byte_Mean    S2D_Byte_STD			5~8
			// S2D_IAT_Max    S2D_IAT_Min    S2D_IAT_Mean    S2D_IAT_STD				9~12
			// D2S_noP    D2S_noB														13~14
			// D2S_Byte_Max    D2S_Byte_Min    D2S_Byte_Mean    D2S_Byte_STD			15~18
			// D2S_IAT_Max    D2S_IAT_Min    D2S_IAT_Mean    D2S_IAT_STD				19~22
			// ToT_noP    ToT_noB														23~24
			// ToT_Byte_Max    ToT_Byte_Min    ToT_Byte_Mean    ToT_Byte_STD			25~28
			// ToT_IAT_Max    ToT_IAT_Min    ToT_IAT_Mean    ToT_IAT_STD				29~32
			// ToT_Prate    ToT_Brate    ToT_PTransferRatio    ToT_BTransferRatio		33~36
			// DUR    Loss																37~38
			
			
			String srcIP ;
			String line = value.toString() ;
			String[] str = line.split("\t") ;
			
			if ( str.length == 39 )
			{
				srcIP = str[2].split(":")[0] ;
				
				if ( IPList.contains(srcIP) && str[38].equals("0") )
				{
					String botnetID = "" ;
					botnetID = getBotnetID ( str[1], str[2] ) ;
					
					interValue.set ( 	str[3] + "," + str[4] + "," + 
										str[5] + "," + str[6] + "," + str[7] + "," + str[8] + "," + 
										//str[9] + "," + str[10] + "," + str[11] + "," + str[12] + "," + 
										str[13] + "," + str[14] + "," + 
										str[15] + "," + str[16] + "," + str[17] + "," + str[18] + "," + 
										//str[19] + "," + str[20] + "," + str[21] + "," + str[22] + "," + 
										str[23] + "," + str[24] + "," + 
										str[25] + "," + str[26] + "," + str[27] + "," + str[28] + "," + 
										//str[29] + "," + str[30] + "," + str[31] + "," + str[32] + "," + 
										str[33] + "," + str[34] + "," + str[35] + "," + str[36] + "," + 
										str[37] + "," + botnetID ) ;
					context.write ( NullWritable.get(), interValue ) ;
				}
			}
		}
		
		// for weka training
		// return botnet ID
		public String getBotnetID ( String protocol, String ip )
		{
			// string ip format
			// SrcIP:SrcPort>DstIP:DstPort
			
			String botnetID = "" ;
			
			String sip, dip ;
			int dot ;
			sip = ip.split( ">" )[0].split( ":" )[0] ;
			dip = ip.split( ">" )[1].split( ":" )[0] ;
			
			// sip, dip domain
			dot = sip.lastIndexOf( "." ) ;
			sip = sip.substring( 0, dot+1 ) ;
			dot = dip.lastIndexOf( "." ) ;
			dip = dip.substring( 0, dot+1 ) ;
			
			String[] domain = { "140.116.1.", "140.116.6.", "140.116.7.", "140.116.9." } ;
			String[] num = { "1", "6", "7", "9" } ;
			
			for ( int i = 0 ; i < 4 ; i ++ )
			{
				if ( sip.equals( domain[i] ) || dip.equals( domain[i] ) )
				{
					botnetID += num[i] ;
					
					if ( protocol.equals( "6" ) )
					{
						botnetID += "-6" ;
						break ;
					}
					if ( protocol.equals( "17" ) )
					{
						botnetID += "-17" ;
						break ;
					}
				}
			}
			
			return botnetID ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		if ( otherArgs.length != 2 )
		{
			System.out.println( "FilterPhase2MR: <in> <out>" ) ;
			System.out.println( "otherArgs.length = " + otherArgs.length ) ;
			System.exit(2) ;
		}
		
		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	Calculate Failed Connection between any two hosts(FCBH)			*
		 *------------------------------------------------------------------*/
		
		Job job1 = Job.getInstance ( conf, "Filter 2 for Weka - job 1" ) ;
		job1.setJarByClass ( FilterPhase2ForWeka.class ) ;
		
		job1.setMapperClass ( job1Mapper.class ) ;
		job1.setReducerClass ( job1Reducer.class ) ;
		
		job1.setMapOutputKeyClass ( Text.class ) ;
		job1.setMapOutputValueClass ( IntWritable.class ) ;
		job1.setOutputKeyClass ( Text.class ) ;
		job1.setOutputValueClass ( DoubleWritable.class ) ;
		
		FileInputFormat.addInputPath ( job1, new Path ( args[0] ) ) ;
		FileOutputFormat.setOutputPath ( job1, new Path ( args[1] + "_job1_FCBH" ) ) ;
		
		job1.waitForCompletion ( true ) ;
		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Calculate Failed Connection for each host						*
		 *------------------------------------------------------------------*/
		
		Job job2 = Job.getInstance ( conf, "Filter 2 for Weka - job 2" ) ;
		job2.setJarByClass ( FilterPhase2ForWeka.class ) ;
		
		job2.setMapperClass ( job2Mapper.class ) ;
		job2.setReducerClass ( job2Reducer.class ) ;
		
		job2.setMapOutputKeyClass ( Text.class ) ;
		job2.setMapOutputValueClass ( DoubleWritable.class ) ;
		job2.setOutputKeyClass ( Text.class ) ;
		job2.setOutputValueClass ( DoubleWritable.class ) ;
		
		FileInputFormat.addInputPath ( job2, new Path ( args[1] + "_job1_FCBH" ) ) ;
		FileOutputFormat.setOutputPath ( job2, new Path ( args[1] + "_job2_FC" ) ) ;
		MultipleOutputs.addNamedOutput ( job2, "HighFCIP", TextOutputFormat.class, Text.class, DoubleWritable.class ) ;
		MultipleOutputs.addNamedOutput ( job2, "LowFCIP", TextOutputFormat.class, Text.class, DoubleWritable.class ) ;
		
		job2.waitForCompletion ( true ) ;
		/*----------------------------------------------------------------------------------------------*
		 *								Job 3		 													*
		 *	Remove low failed connection hosts and divide into Unidirectional/Bidirectional flow Group	*
		 *----------------------------------------------------------------------------------------------*/
		
		conf.set ( "HighFCIP", args[1] + "_job2_FC/HighFCIP" ) ;
		
		Job job3 = Job.getInstance ( conf, "Filter 2 for Weka - job 3" ) ;
		job3.setJarByClass ( FilterPhase2ForWeka.class ) ;
		
		job3.setMapperClass ( job3Mapper.class ) ;
		
		job3.setMapOutputKeyClass ( NullWritable.class ) ;
		job3.setMapOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( job3, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath ( job3, new Path(args[1]) ) ;
		
		// map only job
		job3.setNumReduceTasks(0) ;
		
		return job3.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new FilterPhase2ForWeka(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit ( res ) ;
	}
}
