package fbicloud.botrank ;

import fbicloud.botrank.KeyComparator.* ;
import fbicloud.utils.NetflowRecord ;
import fbicloud.utils.DataDistribution ;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileSystem ;
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


public class FilterPhase1MR extends Configured implements Tool
{
	public static class FilterMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// HashSets to store white list, dns list
		private Set<String> white = new HashSet<String>() ;
		private Set<String> whiteDomain = new HashSet<String>() ;
		private Set<String> dns = new HashSet<String>() ;
		
		// HDFS file input value reader
		private	String line ;
		
		// netflow object
		private NetflowRecord nf = new NetflowRecord() ;
		
		// TCP, UDP protocol number
		private final static String tcpNum = "6" ;
		private final static String udpNum = "17" ;
		
		// input value reader
		private String[] srcIPArray ;
		private String[] dstIPArray ;
		private String subSrcIP ;
		private String subDstIP ;
		
		
		// map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			// Read File from HDFS -- white list
			Path pathWhite = new Path( config.get( "whitePath", "white" ) ) ;
			FileSystem fsWhite = pathWhite.getFileSystem( config ) ;
			BufferedReader brWhite = new BufferedReader( new InputStreamReader( fsWhite.open(pathWhite) ) ) ;
			line = brWhite.readLine() ;
			while ( line != null )
			{
				// domin part of IP address
				if ( line.split("\\.").length == 2 )
				{
					whiteDomain.add( line ) ;
				}
				// full IP address
				else
				{
					white.add( line ) ;
				}
				line = brWhite.readLine() ;
			}
			brWhite.close() ;
			/*// test
			for ( String ip : whiteDomain )
			{
				System.out.println( "whiteDomain = " + ip ) ;
			}
			for ( String ip : white )
			{
				System.out.println( "white = " + ip ) ;
			}*/
			
			// Read File from HDFS -- dns list
			Path pathDNS = new Path( config.get( "dnsPath", "dns" ) ) ;
			FileSystem fsDNS = pathDNS.getFileSystem( config ) ;
			BufferedReader brDNS = new BufferedReader( new InputStreamReader( fsDNS.open(pathDNS) ) ) ;
			line = brDNS.readLine() ;
			while ( line != null )
			{
				dns.add( line ) ;
				line = brDNS.readLine() ;
			}
			brDNS.close() ;
			/*// test
			for ( String ip : dns )
			{
				System.out.println( "dns = " + ip ) ;
			}*/
		}
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			// input format :
			// key : 
			// LongWritable
			// value :									index
			// timestamp    duration    protocol		0~2
			// srcIP    srcPort    dstIP    dstPort		3~6
			// packets    bytes							7~8
			
			
			// value to netflow object
			nf.parseRaw( value.toString() ) ;
			
			srcIPArray = nf.getSrcIP().split("\\.") ;
			dstIPArray = nf.getDstIP().split("\\.") ;
			subSrcIP = srcIPArray[0] + "." + srcIPArray[1] ; // domin part of IP address
			subDstIP = dstIPArray[0] + "." + dstIPArray[1] ;
			
			// Get the flow with protocol = TCP (6) or UDP (17)
			if ( nf.getProt().equals(tcpNum) || nf.getProt().equals(udpNum) )
			{
				// srcIP and dstIP are not in the white list and dns list
				if( ! white.contains(nf.getSrcIP()) && 
					! white.contains(nf.getDstIP()) && 
					! whiteDomain.contains(subSrcIP) && 
					! whiteDomain.contains(subDstIP) && 
					! dns.contains(nf.getSrcIP()) && 
					! dns.contains(nf.getDstIP())      )
				{
					// Set the composite key = < hash ,timestamp >
					comKey.setTuples(nf.getKey()) ; // Protocol + SrcIP + DstIP
					comKey.setTime(nf.getTimestamp()) ;
					
					// Set the value
					interValue.set(value.toString()) ;
					
					context.write( comKey, interValue ) ;
					
					// test
					//System.out.println( "nf.getKey() = " + nf.getKey() ) ;
					//System.out.println( "interValue = " + interValue.toString() ) ;
				}
			}
		}
	}
	
	
	// get the session of the flows with same Protocol, SrcIP, DstIP
	public static class FilterReducer extends Reducer <CompositeKey, Text, Text, Text>
	{
		// command line parameter
		private long tcpTimeOut = 0L ;
		private long udpTimeOut = 0L ;
		
		// input value reader
		private	String line ;
		
		// netflow object
		private NetflowRecord nf = new NetflowRecord();
		// TCP/UDP timeout value
		private long timeout = 0L ;
		
		// TCP, UDP protocol number
		private final static String tcpNum = "6" ;
		private final static String udpNum = "17" ;
		
		// feature vector variable
		private String srcIP ;
		private String dstIP ;
		private String srcPort ;
		private String dstPort ;
		private String protocol ;
		
		private long srcToDst_NumOfPkts ;
		private long srcToDst_NumOfBytes ;
		private ArrayList<Long> srcToDst_Byte_List = new ArrayList<Long>() ;
		
		private long dstToSrc_NumOfPkts ;
		private long dstToSrc_NumOfBytes ;
		private ArrayList<Long> dstToSrc_Byte_List = new ArrayList<Long>() ;
		
		private long total_NumOfPkts ;
		private long total_NumOfBytes ;
		private ArrayList<Long> total_Byte_List = new ArrayList<Long>() ;
		
		private double total_PktsRate ; // packets rate
		private double total_BytesRate ; // bytes rate
		private double total_BytesTransferRatio ; // dstToSrc_NumOfBytes/srcToDst_NumOfBytes
		
		private long first_timestamp ;
		private long timestamp ;
		private long endTime ;
		private long loss ;
		private boolean isBiDirectional ;
		
		
		// reduce output key and value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			tcpTimeOut = Long.parseLong( config.get( "tcpTimeOut", "21000" ) ) ;
			udpTimeOut = Long.parseLong( config.get( "udpTimeOut", "22000" ) ) ;
		}
		
		public void reduce ( CompositeKey key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			srcIP = "" ;
			dstIP = "" ;
			srcPort = "" ;
			dstPort = "" ;
			protocol = "" ;
			
			/*----------------------------------------------*
			 *	Source to Destination information			*
			 *----------------------------------------------*/
			srcToDst_NumOfPkts = 0 ;
			srcToDst_NumOfBytes = 0 ;
			srcToDst_Byte_List.clear() ;
			//ArrayList<Long> srcToDst_Byte_List = new ArrayList<Long>() ;
			
			/*----------------------------------------------*
			 *	Destination to Source information			*
			 *----------------------------------------------*/
			dstToSrc_NumOfPkts = 0 ;
			dstToSrc_NumOfBytes = 0 ;
			dstToSrc_Byte_List.clear() ;
			//ArrayList<Long> dstToSrc_Byte_List = new ArrayList<Long>() ;
			
			/*----------------------------------------------*
			 *	S2D and D2S total information				*
			 *----------------------------------------------*/
			total_NumOfPkts = 0 ;
			total_NumOfBytes = 0 ;
			total_Byte_List.clear() ;
			//ArrayList<Long> total_Byte_List = new ArrayList<Long>() ;
			
			total_PktsRate = 0 ; // packets rate
			total_BytesRate = 0 ; // bytes rate
			total_BytesTransferRatio = 0 ; // dstToSrc_NumOfBytes/srcToDst_NumOfBytes
			
			// time information
			first_timestamp = 0 ;
			timestamp = 0 ;
			endTime = 0 ;
			
			// flow loss or not, loss = 0 => flow has response ; loss = 1 => flow has no response
			loss = 0 ;
			
			isBiDirectional = false ;
			
			
			/*--------------------------------------------------------------------------------------*
			 *	Organize the Flow Group	by TCP/UDP timeout. Each Flow Group has same five-tuples		*
			 *	TCP -> Time interval between current flow and previous flow samller than 21000(ms)	*
			 *		-> Group as the same flow														*
			 *	UDP -> Time interval between current flow and previous flow samller than 22000(ms)	*
			 *		-> Group as the same flow														*
			 *--------------------------------------------------------------------------------------*/
			
			// test
			//int lineNum = 0 ;
			
			Iterator<Text> val = values.iterator() ;
			
			if ( val.hasNext() )
			{
				line = val.next().toString() ;
				// test
				//System.out.println( lineNum + " line = " + line ) ;
				//lineNum ++ ;
				
				nf.parseRaw( line ) ;
				
				protocol = nf.getProt() ;
				// Set the tcpTimeOut, udpTimeOut
				if ( protocol.equals(tcpNum) ) // TCP
				{
					timeout = tcpTimeOut ;
					// test
					//System.out.println( "TCP = " + timeout ) ;
				}
				if ( protocol.equals(udpNum) ) // UDP
				{
					timeout = udpTimeOut ;
					// test
					//System.out.println( "UDP = " + timeout ) ;
				}
				
				
				srcIP = nf.getSrcIP();
				dstIP = nf.getDstIP();
				srcPort = nf.getSrcPort();
				dstPort = nf.getDstPort();
				
				total_NumOfPkts = nf.getPkts();
				total_NumOfBytes = nf.getBytes();
				
				timestamp = nf.getTimestamp();
				first_timestamp = timestamp ; // session start time
				endTime = timestamp + nf.getDuration();
				
				
				dstToSrc_NumOfPkts = 0;
				srcToDst_NumOfPkts = total_NumOfPkts;
				dstToSrc_NumOfBytes = 0;
				srcToDst_NumOfBytes = total_NumOfBytes;
				
				// if srcToDst_NumOfPkts = 3 (packets), srcToDst_NumOfBytes = 150(bytes)
				// then add three packets each has 50 bytes
				for ( int i = 0 ; i < srcToDst_NumOfPkts ; i++ )
				{
					srcToDst_Byte_List.add( (long) ((double)srcToDst_NumOfBytes/srcToDst_NumOfPkts) ) ;
					total_Byte_List.add( (long) ((double)srcToDst_NumOfBytes/srcToDst_NumOfPkts) ) ;
				}
				
				//loss = 0 ;
				//isBiDirectional = false ;
			}
			while ( val.hasNext() )
			{
				line = val.next().toString() ;
				// test
				//System.out.println( lineNum + " line = " + line ) ;
				//lineNum ++ ;
				
				nf.parseRaw( line ) ;
				
				
				if ( nf.getTimestamp() - timestamp < timeout )
				{
					// Check Flow Group is uni-directional or bi-directional
					if ( ! nf.getSrcIP().equals(srcIP) )
					{
						isBiDirectional = true ;
					}
					
					// update timestamp
					timestamp = nf.getTimestamp() ;
					
					// update the finish time of the session
					endTime = nf.getTimestamp() + nf.getDuration() ;
					
					
					// Accumulate the Flow Group's Feature
					total_NumOfPkts = total_NumOfPkts + nf.getPkts() ;
					total_NumOfBytes = total_NumOfBytes + nf.getBytes() ;
					for ( int i = 0 ; i < nf.getPkts() ; i ++ )
					{
						total_Byte_List.add( (long) ( (double)nf.getBytes()/nf.getPkts() ) ) ;
					}
					
					// same direction flow
					if ( srcIP.equals(nf.getSrcIP()) && dstIP.equals(nf.getDstIP()) )
					{
						srcToDst_NumOfPkts = srcToDst_NumOfPkts + nf.getPkts() ;
						srcToDst_NumOfBytes = srcToDst_NumOfBytes + nf.getBytes() ;
						for ( int i = 0 ; i < nf.getPkts() ; i ++ )
						{
							srcToDst_Byte_List.add( (long) ((double)nf.getBytes()/nf.getPkts()) ) ;
						}
					}
					// counter direction flow
					else
					{
						dstToSrc_NumOfPkts = dstToSrc_NumOfPkts + nf.getPkts() ;
						dstToSrc_NumOfBytes = dstToSrc_NumOfBytes + nf.getBytes() ;
						for ( int i = 0 ; i < nf.getPkts() ; i ++ )
						{
							dstToSrc_Byte_List.add( (long) ((double)nf.getBytes()/nf.getPkts()) ) ;
						}
					}
				}
				else
				{
					if(!isBiDirectional)	//Only have one direction in Flow Group
						loss = 1;
					
					//Get Byte : Max,min,mean,STD 
					DataDistribution srcToDst_Byte_Distribution = new DataDistribution(srcToDst_Byte_List);
					DataDistribution dstToSrc_Byte_Distribution = new DataDistribution(dstToSrc_Byte_List);
					DataDistribution total_Byte_Distribution = new DataDistribution(total_Byte_List);
					
					//Calculate  packet/sec,bytes/sec
					if((endTime-first_timestamp) == 0){//Set duration as 0.5 millisecs
						total_PktsRate = (double) total_NumOfPkts/0.5;
						total_BytesRate = (double) total_NumOfBytes/0.5;
					}
					else{
						total_PktsRate = (double) total_NumOfPkts/(endTime-first_timestamp);
						total_BytesRate = (double) total_NumOfBytes/(endTime-first_timestamp);
					}
					
					//Calculate trnsfer rate. srcToDst_NumOfPkts != 0 for all situations
					//ToT_PTransferRatio = (double) dstToSrc_NumOfPkts/srcToDst_NumOfPkts;
					total_BytesTransferRatio = (double) dstToSrc_NumOfBytes/srcToDst_NumOfBytes;
					
					
					outputKey.set(String.valueOf(first_timestamp)+"\t"+protocol+"\t"+srcIP+":"+srcPort+">"+dstIP+":"+dstPort);
					outputValue.set(
							//Source -> Destination 
							String.valueOf(srcToDst_NumOfPkts)+"\t"+ String.valueOf(srcToDst_NumOfBytes)+"\t"+
							String.valueOf(srcToDst_Byte_Distribution.getMax())+"\t"+ String.valueOf(srcToDst_Byte_Distribution.getMin())+"\t"+ String.valueOf(srcToDst_Byte_Distribution.getMean())+"\t"+
							
							//Destination -> Source 
							String.valueOf(dstToSrc_NumOfPkts)+"\t"+ String.valueOf(dstToSrc_NumOfBytes)+"\t"+
							String.valueOf(dstToSrc_Byte_Distribution.getMax())+"\t"+ String.valueOf(dstToSrc_Byte_Distribution.getMin())+"\t"+ String.valueOf(dstToSrc_Byte_Distribution.getMean())+"\t"+
							
							//Total information
							String.valueOf(total_NumOfPkts)+"\t"+ String.valueOf(total_NumOfBytes)+"\t"+
							String.valueOf(total_Byte_Distribution.getMax())+"\t"+ String.valueOf(total_Byte_Distribution.getMin())+"\t"+ String.valueOf(total_Byte_Distribution.getMean())+"\t"+ String.valueOf(total_Byte_Distribution.getStd())+"\t"+
							String.valueOf(total_PktsRate)+"\t"+ String.valueOf(total_BytesRate)+"\t"+
							String.valueOf(total_BytesTransferRatio)+"\t"+
							String.valueOf(endTime-first_timestamp)+"\t"+
							String.valueOf(loss));
					context.write(outputKey, outputValue);
					// test
					//System.out.println( "else outputKey = " + outputKey.toString() ) ;
					//System.out.println( "else outputValue = " + outputValue.toString() ) ;
					
					
					// Reset Arraylist
					srcToDst_Byte_List.clear();
					dstToSrc_Byte_List.clear();
					total_Byte_List.clear();
					
					// Reset Flow Group
					srcIP = nf.getSrcIP();
					dstIP = nf.getDstIP();
					srcPort = nf.getSrcPort();
					dstPort = nf.getDstPort();
					protocol = nf.getProt();
					total_NumOfPkts = nf.getPkts();
					total_NumOfBytes = nf.getBytes();
					timestamp = nf.getTimestamp();
					first_timestamp = timestamp ; // session start time
					endTime = timestamp + nf.getDuration();
					
					loss = 0;
					isBiDirectional = false;
					
					dstToSrc_NumOfPkts = 0;
					srcToDst_NumOfPkts = total_NumOfPkts;
					dstToSrc_NumOfBytes = 0;
					srcToDst_NumOfBytes = total_NumOfBytes;
					
					for(int i=0;i<srcToDst_NumOfPkts;i++){
						srcToDst_Byte_List.add((long) ((double)srcToDst_NumOfBytes/srcToDst_NumOfPkts));
						total_Byte_List.add((long) ((double)srcToDst_NumOfBytes/srcToDst_NumOfPkts));
					}
				}
			}
			
			
			// Only have one direction in Flow Group
			if ( ! isBiDirectional )
			{
				loss = 1 ;
			}
			
			
			// Get Byte : max, min, mean, STD
			DataDistribution srcToDst_Byte_Distribution = new DataDistribution(srcToDst_Byte_List) ;
			DataDistribution dstToSrc_Byte_Distribution = new DataDistribution(dstToSrc_Byte_List) ;
			DataDistribution total_Byte_Distribution = new DataDistribution(total_Byte_List) ;
			
			// Calculate  packet/sec, bytes/sec
			if ( ( endTime - first_timestamp ) == 0 )
			{
				total_PktsRate = (double) total_NumOfPkts/0.5 ;
				total_BytesRate = (double) total_NumOfBytes/0.5 ;
			}
			else
			{
				total_PktsRate = (double) total_NumOfPkts/(endTime-first_timestamp) ;
				total_BytesRate = (double) total_NumOfBytes/(endTime-first_timestamp) ;
			}
			
			// Calculate trnsfer rate. srcToDst_NumOfPkts != 0 for all situations
			total_BytesTransferRatio = (double) dstToSrc_NumOfBytes/srcToDst_NumOfBytes ;
			
			
			outputKey.set( String.valueOf(first_timestamp) + "\t" +
						   protocol + "\t" + srcIP + ":" + srcPort + ">" + dstIP + ":" + dstPort ) ;
			outputValue.set(
					// Source -> Destination
					String.valueOf(srcToDst_NumOfPkts) + "\t" + String.valueOf(srcToDst_NumOfBytes) + "\t" +
					String.valueOf(srcToDst_Byte_Distribution.getMax()) + "\t" + String.valueOf(srcToDst_Byte_Distribution.getMin()) + "\t" +
					String.valueOf(srcToDst_Byte_Distribution.getMean()) + "\t" +
					
					// Destination -> Source
					String.valueOf(dstToSrc_NumOfPkts) + "\t" + String.valueOf(dstToSrc_NumOfBytes) + "\t" +
					String.valueOf(dstToSrc_Byte_Distribution.getMax()) + "\t" + String.valueOf(dstToSrc_Byte_Distribution.getMin()) + "\t" +
					String.valueOf(dstToSrc_Byte_Distribution.getMean()) + "\t" +
					
					// Total information
					String.valueOf(total_NumOfPkts) + "\t" + String.valueOf(total_NumOfBytes) + "\t" +
					String.valueOf(total_Byte_Distribution.getMax()) + "\t" + String.valueOf(total_Byte_Distribution.getMin()) + "\t" +
					String.valueOf(total_Byte_Distribution.getMean()) + "\t" + String.valueOf(total_Byte_Distribution.getStd()) + "\t" +
					String.valueOf(total_PktsRate) + "\t" + String.valueOf(total_BytesRate) + "\t" +
					String.valueOf(total_BytesTransferRatio) + "\t" +
					String.valueOf(endTime-first_timestamp) + "\t" +
					String.valueOf(loss) ) ;
			context.write( outputKey, outputValue ) ;
			// test
			//System.out.println( "outputKey = " + outputKey.toString() ) ;
			//System.out.println( "outputValue = " + outputValue.toString() ) ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		if ( otherArgs.length != 2 )
		{
			System.out.println( "FilterPhase1MR: <in> <out>" ) ;
			System.exit(2) ;
		}
		
		// 1. filter out white list and dns list
		// 2. group flow into session by TCP/UDP timeout
		// 3. extract features of session
		Job job = Job.getInstance ( conf, "Filter 1" ) ;
		job.setJarByClass(FilterPhase1MR.class) ;
		
		job.setMapperClass(FilterMapper.class) ;
		job.setReducerClass(FilterReducer.class) ;
		
		job.setMapOutputKeyClass(CompositeKey.class) ;
		job.setMapOutputValueClass(Text.class) ;
		job.setOutputKeyClass(Text.class) ;
		job.setOutputValueClass(Text.class) ;
		
		job.setPartitionerClass(KeyPartitioner.class) ;
		job.setSortComparatorClass(CompositeKeyComparator.class) ;
		job.setGroupingComparatorClass(KeyGroupingComparator.class) ;
		
		FileInputFormat.addInputPath( job, new Path(args[0]) ) ;
		FileOutputFormat.setOutputPath( job, new Path(args[1]) ) ;
		
		return job.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run( new Configuration(), new FilterPhase1MR(), args ) ;
		// Run the class FilterPhase1MR() after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}
