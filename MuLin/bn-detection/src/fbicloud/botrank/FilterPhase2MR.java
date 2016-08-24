package fbicloud.botrank ;

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


public class FilterPhase2MR extends Configured implements Tool
{
	public static class CalculateFLRBetweenHostsMapper extends Mapper <LongWritable, Text, Text, IntWritable>
	{
		// length of line
		private final static int lengthOfLine = 24 ;
		
		// input value reader
		private String srcInfo, srcIP ;
		private String dstInfo, dstIP ;
		private String line ;
		private String[] subLine ;
		private String loss ;
		
		// map intermediate key and value
		private Text interKey = new Text() ;
		private IntWritable interValue = new IntWritable() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			line = value.toString() ;
			subLine = line.split("\t") ;
			loss = subLine[23] ;
			
			if ( subLine.length == lengthOfLine )
			{
				srcInfo = subLine[2].split(">")[0] ; // SrcIP:SrcPort
				dstInfo = subLine[2].split(">")[1] ; // DstIP:DstPort
				srcIP = srcInfo.split(":")[0] ; // SrcIP
				dstIP = dstInfo.split(":")[0] ; // DstIP
				
				interKey.set( srcIP + ">" + dstIP ) ;
				interValue.set( Integer.parseInt( loss ) ) ;
				context.write( interKey, interValue ) ;
			}
		}
	}
	
	public static class CalculateFLRBetweenHostsReducer extends Reducer <Text, IntWritable, Text, DoubleWritable>
	{
		// counter
		private long numOfFG ;				// number of Flow Group(FG)
		private long numOfFGWOResponsed ;	// number of Flow Group(FG) without response
		
		// reduce output value
		private DoubleWritable outputValue = new DoubleWritable() ;
		
		
		public void reduce ( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
		{
			numOfFG = 0 ;				// number of Flow Group(FG)
			numOfFGWOResponsed = 0 ;	// number of Flow Group(FG) without response
			
			for ( IntWritable val : values )
			{
				numOfFG ++ ;
				numOfFGWOResponsed += val.get();
			}
			outputValue.set( (double)numOfFGWOResponsed/numOfFG ) ;
			context.write( key, outputValue ) ;
		}
	}
	
	
	public static class CalculateFLRForEachHostMapper extends Mapper <LongWritable, Text, Text, DoubleWritable>
	{
		// length of line
		private final static int lengthOfLine = 2 ;
		
		// input value reader
		private String srcIP ;
		private String line ;
		private String[] subLine ;
		private String flowLossRate ;
		
		// map intermediate key and value
		private Text interKey = new Text() ;
		private DoubleWritable interValue = new DoubleWritable() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// Input Format :
			//		SrcIP>DstIP FCBH(Failed Connection between hosts)
			// Input Example:
			//		140.116.1.1>61.64.19.192	0.6742
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			flowLossRate = subLine[1] ;
			
			if ( subLine.length == lengthOfLine )
			{
				srcIP = subLine[0].split(">")[0] ;
				
				interKey.set( srcIP ) ;
				interValue.set( Double.parseDouble( flowLossRate ) ) ;
				context.write( interKey, interValue ) ;
			}
		}
	}
	
	public static class CalculateFLRForEachHostReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable>
	{
		// command line option
		private double flowlossratio = 0 ;
		
		private MultipleOutputs<Text, DoubleWritable> mos ;
		// output directory name
		private String lowFLR_DirName = "LowFLR/IP" ;
		private String highFLR_DirName = "HighFLR/IP" ;
		
		// counter
		private long numOfDstIPs ;	// number of destination IPs
		private double sumOfFLR ;	// sum of Flow Loss Rate
		
		// reduce output value
		private DoubleWritable outputValue = new DoubleWritable() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			// Multiple output Directory
			mos = new MultipleOutputs<Text, DoubleWritable>(context) ;
			
			flowlossratio = Double.parseDouble( config.get( "flowlossratio", "0.225" ) ) ;
			// test
			//System.out.println( "flowlossratio = " + flowlossratio ) ;
		}
		
		public void reduce ( Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException
		{
			//Set output folder
			//output
			//		LowFLR
			//				IP-m-00000
			//				IP-m-00001
			//		HighFLR
			//				IP-m-00000
			//				IP-m-00001
			
			
			numOfDstIPs = 0 ;	// number of destination IPs
			sumOfFLR = 0 ;	// sum of Flow Loss Rate
			
			for ( DoubleWritable val : values )
			{
				numOfDstIPs ++ ;
				sumOfFLR += val.get() ;
			}
			outputValue.set ( sumOfFLR / numOfDstIPs ) ;
			
			if ( sumOfFLR / numOfDstIPs > flowlossratio )
				mos.write ( "HighFLR", key, outputValue, highFLR_DirName ) ;
			else
				mos.write ( "LowFLR", key, outputValue, lowFLR_DirName ) ;
		}
		
		public void cleanup ( Context context ) throws IOException, InterruptedException
		{
			mos.close() ;
		}
	}
	
	
	public static class RemoveLowFLRMapper extends Mapper <LongWritable, Text, NullWritable, Text>
	{
		// HashSet to store high flow loss rate IP
		private Set<String> ipSet = new HashSet<String>() ;
		
		// length of line
		private final static int lengthOfLine = 24 ;
		
		// input value reader
		private String srcIP ;
		private String line ;
		private String[] subLine ;
		private String loss ;
		
		// map intermediate value
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			String line ;
			Configuration config = context.getConfiguration() ;
			
			FileSystem fs = FileSystem.get ( config ) ;
			FileStatus[] status = fs.listStatus ( new Path ( config.get("HighFLR") ) ) ;
			for ( int i = 0 ; i < status.length ; i ++ )
			{
				BufferedReader br = new BufferedReader ( new InputStreamReader(fs.open(status[i].getPath())) ) ;
				line = br.readLine() ;
				while ( line != null )
				{
					ipSet.add ( line.split("\t")[0] ) ;
					line = br.readLine() ;
				}
			}
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :																															index
			// time    Protocol    SrcIP:SrcPort>DstIP:DstPort																					0~2
			// srcToDst_NumOfPkts    srcToDst_NumOfBytes    srcToDst_Byte_Max    srcToDst_Byte_Min    srcToDst_Byte_Mean						3~7
			// dstToSrc_NumOfPkts    dstToSrc_NumOfBytes    dstToSrc_Byte_Max    dstToSrc_Byte_Min    dstToSrc_Byte_Mean						8~12
			// total_NumOfPkts       total_NumOfBytes       total_Byte_Max       total_Byte_Min       total_Byte_Mean       total_Byte_STD		13~17
			// total_PktsRate        total_BytesRate        total_BytesTransferRatio      duration    Loss										18~23
			
			
			line = value.toString() ;
			subLine = line.split("\t") ;
			loss = subLine[23] ;
			
			
			String timestamp = subLine[0] ;
			String protocol = subLine[1] ;
			String srcDstPair = subLine[2] ; // SrcIP:SrcPort>DstIP:DstPort
			
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
				srcIP = subLine[2].split(":")[0] ;
				
				if ( ipSet.contains(srcIP) && loss.equals("0") )
				{
					interValue.set(	timestamp + "\t" + protocol + "\t" + srcDstPair + "\t" + 
									srcToDst_NumOfPkts + "\t" + srcToDst_NumOfBytes + "\t" + srcToDst_Byte_Max + "\t" + srcToDst_Byte_Min + "\t" + srcToDst_Byte_Mean + "\t" + 
									dstToSrc_NumOfPkts + "\t" + dstToSrc_NumOfBytes + "\t" + dstToSrc_Byte_Max + "\t" + dstToSrc_Byte_Min + "\t" + dstToSrc_Byte_Mean + "\t" + 
									total_NumOfPkts + "\t" + total_NumOfBytes + "\t" + total_Byte_Max + "\t" + total_Byte_Min + "\t" + total_Byte_Mean + "\t" + total_Byte_STD + "\t" + 
									total_PktsRate + "\t" + total_BytesRate + "\t" + total_BytesTransferRatio + "\t" + duration + "\t" + loss ) ;
					
					context.write ( NullWritable.get(), interValue ) ;
				}
			}
		}
	}
	
	
	public int run(String[] args) throws Exception
	{
		Configuration conf = this.getConf();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2 )
		{
			System.out.println("FilterPhase2MR: <in> <out>");
			System.exit(2);
		}
		
		
		/*********************************************************/
		// job 1
		// calculate Flow Loss Rate(FLR) between any two hosts
		/*********************************************************/
		
		Job calculateFLRBetweenHosts = Job.getInstance ( conf, "Filter 2 - CalculateFLRBetweenHosts" ) ;
		calculateFLRBetweenHosts.setJarByClass(FilterPhase2MR.class);
		
		calculateFLRBetweenHosts.setMapperClass(CalculateFLRBetweenHostsMapper.class);
		calculateFLRBetweenHosts.setReducerClass(CalculateFLRBetweenHostsReducer.class);
		
		calculateFLRBetweenHosts.setMapOutputKeyClass(Text.class);
		calculateFLRBetweenHosts.setMapOutputValueClass(IntWritable.class);
		calculateFLRBetweenHosts.setOutputKeyClass(Text.class);
		calculateFLRBetweenHosts.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(calculateFLRBetweenHosts, new Path(args[0]));
		FileOutputFormat.setOutputPath(calculateFLRBetweenHosts, new Path(args[1]+"_job1_FLR_BH"));
		
		calculateFLRBetweenHosts.waitForCompletion(true);
		
		/*********************************************************/
		// job 2
		// calculate Flow Loss Rate(FLR) for each host
		/*********************************************************/
		
		Job calculateFLRForEachHost = Job.getInstance ( conf, "Filter 2 - CalculateFLRForEachHost" ) ;
		calculateFLRForEachHost.setJarByClass(FilterPhase2MR.class);
		
		calculateFLRForEachHost.setMapperClass(CalculateFLRForEachHostMapper.class);
		calculateFLRForEachHost.setReducerClass(CalculateFLRForEachHostReducer.class);
		
		calculateFLRForEachHost.setMapOutputKeyClass(Text.class);
		calculateFLRForEachHost.setMapOutputValueClass(DoubleWritable.class);
		calculateFLRForEachHost.setOutputKeyClass(Text.class);
		calculateFLRForEachHost.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(calculateFLRForEachHost, new Path(args[1]+"_job1_FLR_BH"));
		FileOutputFormat.setOutputPath(calculateFLRForEachHost, new Path(args[1]+"_job2_FLR"));
		MultipleOutputs.addNamedOutput(calculateFLRForEachHost, "HighFLR", TextOutputFormat.class, Text.class, DoubleWritable.class);
		MultipleOutputs.addNamedOutput(calculateFLRForEachHost, "LowFLR", TextOutputFormat.class, Text.class, DoubleWritable.class);
		
		calculateFLRForEachHost.waitForCompletion(true);
		
		/*********************************************************/
		// job 3
		// remove low Flow Loss Rate(FLR) sessions
		/*********************************************************/
		
		conf.set ( "HighFLR", args[1] + "_job2_FLR/HighFLR" ) ;
		
		Job removeLowFLR = Job.getInstance ( conf, "Filter 2 - RemoveLowFLR" ) ;
		removeLowFLR.setJarByClass ( FilterPhase2MR.class ) ;
		
		removeLowFLR.setMapperClass ( RemoveLowFLRMapper.class ) ;
		
		removeLowFLR.setMapOutputKeyClass ( NullWritable.class ) ;
		removeLowFLR.setMapOutputValueClass ( Text.class ) ;
		
		FileInputFormat.addInputPath ( removeLowFLR, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath( removeLowFLR, new Path(args[1]) ) ;
		
		// map only job
		removeLowFLR.setNumReduceTasks(0) ;
		
		return removeLowFLR.waitForCompletion(true) ? 0 : 1 ;
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
