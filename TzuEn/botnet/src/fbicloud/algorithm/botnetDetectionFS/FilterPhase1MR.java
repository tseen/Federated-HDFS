package fbicloud.algorithm.botnetDetectionFS ;

import fbicloud.algorithm.botnetDetectionFS.FilterPhaseSecondarySort.* ;
import fbicloud.algorithm.classes.NetflowRecord ;
import fbicloud.algorithm.classes.DataDistribution ;

import java.io.* ;
import java.util.* ;

import ncku.hpds.fed.MRv2.proxy.GenericProxyMapper;
import ncku.hpds.fed.MRv2.proxy.GenericProxyReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FilterPhase1MR extends Configured implements Tool
{
	public static class FilterMapper extends Mapper <LongWritable, Text, CompositeKey, Text>
	{
		// HashSets to store white list, dns list, domain
		private Set<String> white = new HashSet<String>() ;
		private Set<String> whiteDomain = new HashSet<String>() ;
		private Set<String> dns = new HashSet<String>() ;
		private Set<String> domain = new HashSet<String>() ;
		
		// HDFS file input value reader
		private	String line ;
		// filter domain option
		private boolean filterDomain ;
		
		// netflow object
		private NetflowRecord nf = new NetflowRecord() ;
		
		// map intermediate key and value
		private CompositeKey comKey = new CompositeKey() ;
		private Text interValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			// Read File from HDFS -- white list
			Path pathWhite = new Path( "white" ) ;
			FileSystem fsWhite = pathWhite.getFileSystem( config ) ;
			BufferedReader brWhite = new BufferedReader( new InputStreamReader( fsWhite.open(pathWhite) ) ) ;
			line = brWhite.readLine() ;
			while ( line != null )
			{
				// domin part of IP address
				if ( line.split("\\.").length == 2 )
				{
					whiteDomain.add( line ) ;
					// test
					//System.out.println( "whiteDomain = " + line ) ;
				}
				// full IP address
				else
				{
					whiteDomain.add( line ) ;
					// test
					//System.out.println( "white = " + line ) ;
				}
				line = brWhite.readLine() ;
			}
			brWhite.close() ;
			
			// Read File from HDFS -- dns list
			Path pathDNS = new Path( "dns" ) ;
			FileSystem fsDNS = pathDNS.getFileSystem( config ) ;
			BufferedReader brDNS = new BufferedReader( new InputStreamReader( fsDNS.open(pathDNS) ) ) ;
			line = brDNS.readLine() ;
			while ( line != null )
			{
				dns.add( line ) ;
				// test
				//System.out.println( "dns = " + line ) ;
				line = brDNS.readLine() ;
			}
			brDNS.close() ;
			
			// Read File from HDFS -- domain
			Path pathDomain = new Path( "domain" ) ;
			FileSystem fsDomain = pathDomain.getFileSystem( config ) ;
			BufferedReader brDomain = new BufferedReader( new InputStreamReader( fsDomain.open(pathDomain) ) ) ;
			line = brDomain.readLine() ;
			while ( line != null )
			{
				domain.add( line ) ;
				// test
				//System.out.println( "domain = " + line ) ;
				line = brDomain.readLine() ;
			}
			brDomain.close() ;
			
			// Hadoop will parse the -D filterdomain = true/false and set the boolean
			String tmpStr = config.get( "filterdoman" ) ;
			if ( tmpStr.equals("false") )
			{
				filterDomain = false ;
			}
			else if ( tmpStr.equals("true") )
			{
				filterDomain = true ;
			}
			else
			{
				System.exit(1) ; // exit
			}
			// test
			//System.out.println( "filterDomain = " + filterDomain ) ;
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
			
			String[] srcIPArray = nf.getSrcIP().split("\\.") ;
			String[] dstIPArray = nf.getDstIP().split("\\.") ;
			String subSrcIP = srcIPArray[0] + "." + srcIPArray[1] ; // domin part of IP address
			String subDstIP = dstIPArray[0] + "." + dstIPArray[1] ;
			
			// Get the flow with protocol = TCP (6) or UDP (17)
			if ( nf.getProt().equals("6") || nf.getProt().equals("17") )
			{
				// srcIP and dstIP are not in the white list and dns list
				if( ! white.contains(nf.getSrcIP()) && 
					! white.contains(nf.getDstIP()) && 
					! whiteDomain.contains(subSrcIP) && 
					! whiteDomain.contains(subDstIP) && 
					! dns.contains(nf.getSrcIP()) && 
					! dns.contains(nf.getDstIP())      )
				{
					// Get the Flow which SrcIP or DstIP is 140.116
					if ( filterDomain )
					{
						if ( domain.contains(subSrcIP) || domain.contains(subDstIP) )
						{
							// Set the composite key = < hash ,timestamp >
							comKey.setTuples(nf.getKey()) ; // Protocol + SrcIP +DstIP
							comKey.setTime(nf.getTimestamp()) ;
							
							// Set the value
							interValue.set(value.toString()) ;
							
							context.write( comKey, interValue ) ;
						}
					}
					else{
						// Set the composite key = < hash ,timestamp >
						comKey.setTuples(nf.getKey()) ; // Protocol + SrcIP +DstIP
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
	}
	
	
	// get the session of the flows with same Protocol, SrcIP, DstIP
	public static class FilterReducer extends Reducer <CompositeKey, Text, Text, Text>
	{
		// command line parameter
		private long tcptime = 0L ;
		private long udptime = 0L ;
		
		// input value reader
		private	String line ;
		
		// netflow object
		private NetflowRecord nf = new NetflowRecord();
		// TCP/UDP timeout value
		private long timeout = 0L ;
		
		
		// reduce output key and value
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			// Get the parameter from -D command line option
			// Hadoop will parse the -D tcptime=xxx(ms) and set the tcptime value
			tcptime = Long.parseLong( config.get("tcptime") ) ;
			udptime = Long.parseLong( config.get("udptime") ) ;
		}
		
		public void reduce ( CompositeKey key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			
			String srcIP = "" ;
			String dstIP = "" ;
			String srcPort = "" ;
			String dstPort = "" ;
			String prot = "" ;
			
			/*----------------------------------------------*
			 *	Source to Destination information			*
			 *----------------------------------------------*/
			long S2D_noP = 0 ;
			long S2D_noB = 0 ;
			ArrayList<Long> S2D_Byte_List = new ArrayList<Long>() ;
			
			/*----------------------------------------------*
			 *	Destination to Source information			*
			 *----------------------------------------------*/
			long D2S_noP = 0 ;
			long D2S_noB = 0 ;
			ArrayList<Long> D2S_Byte_List = new ArrayList<Long>() ;
			
			/*----------------------------------------------*
			 *	S2D and D2S total information				*
			 *----------------------------------------------*/
			long ToT_noP = 0 ;
			long ToT_noB = 0 ;
			ArrayList<Long> ToT_Byte_List = new ArrayList<Long>() ;
			
			double ToT_Prate = 0 ; // packets rate
			double ToT_Brate = 0 ; // bytes rate
			double ToT_BTransferRatio = 0 ; // D2S_noB/S2D_noB
			
			// time information
			long timestamp = 0 ;
			long last = 0 ;
			
			// flow loss or not, loss = 0 => flow has response ; loss = 1 => flow has no response
			long loss = 0 ;
			
			boolean isBiDirectional = false ;
			
			
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
				prot = nf.getProt() ;
				
				// Set the tcptime, udptime
				if ( prot.equals("6") ) // TCP
				{
					timeout = tcptime ;
					// test
					//System.out.println( "TCP = " + timeout ) ;
				}
				if ( prot.equals("17") ) // UDP
				{
					timeout = udptime ;
					// test
					//System.out.println( "UDP = " + timeout ) ;
				}
				
				
				srcIP = nf.getSrcIP();
				dstIP = nf.getDstIP();
				srcPort = nf.getSrcPort();
				dstPort = nf.getDstPort();
				
				ToT_noP = nf.getPkts();
				ToT_noB = nf.getBytes();
				
				timestamp = nf.getTimestamp();
				last = timestamp + nf.getDuration();
				
				
				D2S_noP = 0;
				S2D_noP = ToT_noP;
				D2S_noB = 0;
				S2D_noB = ToT_noB;
				
				// if S2D_noP = 3 (packets), S2D_noB = 150(bytes)
				// then add three packets each has 50 bytes
				for ( int i = 0 ; i < S2D_noP ; i++ )
				{
					S2D_Byte_List.add( (long) ((double)S2D_noB/S2D_noP) ) ;
					ToT_Byte_List.add( (long) ((double)S2D_noB/S2D_noP) ) ;
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
					//-----------------Check time order-----------------
					if(nf.getTimestamp()<timestamp){
						outputKey.set("ERROR error");//CHECK
						outputValue.set("ERROR error");//CHECK
						context.write(outputKey, outputValue);
					}
					//--------------------------------------------------
					
					// test
					//System.out.println( " time slot = " + (nf.getTimestamp() - timestamp) ) ;
					
					// Check Flow Group is uni-directional or bi-directional
					if ( ! nf.getSrcIP().equals(srcIP) )
					{
						isBiDirectional = true ;
					}
					
					// update the finish time of the session
					last = nf.getTimestamp() + nf.getDuration() ;
					
					// Accumulate the Flow Group's Feature
					ToT_noP = ToT_noP + nf.getPkts() ;
					ToT_noB = ToT_noB + nf.getBytes() ;
					for ( int i = 0 ; i < nf.getPkts() ; i ++ )
					{
						ToT_Byte_List.add( (long) ( (double)nf.getBytes()/nf.getPkts() ) ) ;
					}
					
					// same direction flow
					if ( srcIP.equals(nf.getSrcIP()) && dstIP.equals(nf.getDstIP()) )
					{
						S2D_noP = S2D_noP + nf.getPkts() ;
						S2D_noB = S2D_noB + nf.getBytes() ;
						for ( int i = 0 ; i < nf.getPkts() ; i ++ )
						{
							S2D_Byte_List.add( (long) ((double)nf.getBytes()/nf.getPkts()) ) ;
						}
					}
					// counter direction flow
					else
					{
						D2S_noP = D2S_noP + nf.getPkts() ;
						D2S_noB = D2S_noB + nf.getBytes() ;
						for ( int i = 0 ; i < nf.getPkts() ; i ++ )
						{
							D2S_Byte_List.add( (long) ((double)nf.getBytes()/nf.getPkts()) ) ;
						}
					}
				}
				else
				{
					if(!isBiDirectional)	//Only have one direction in Flow Group
						loss = 1;
					
					//Get Byte : Max,min,mean,STD 
					DataDistribution S2D_Byte_D = new DataDistribution(S2D_Byte_List);
					DataDistribution D2S_Byte_D = new DataDistribution(D2S_Byte_List);
					DataDistribution ToT_Byte_D = new DataDistribution(ToT_Byte_List);
					
					//Calculate  packet/sec,bytes/sec
					if((last-timestamp) == 0){//Set duration as 0.5 millisecs
						ToT_Prate = (double) ToT_noP/0.5;
						ToT_Brate = (double) ToT_noB/0.5;
					}
					else{
						ToT_Prate = (double) ToT_noP/(last-timestamp);
						ToT_Brate = (double) ToT_noB/(last-timestamp);
					}
					
					//Calculate trnsfer rate. S2D_noP != 0 for all situations
					//ToT_PTransferRatio = (double) D2S_noP/S2D_noP;
					ToT_BTransferRatio = (double) D2S_noB/S2D_noB;
					
					
					//Ignore all IAT, S2D_Byte_STD, D2S_Byte_STD, ToT_PTransferRatio
					outputKey.set(String.valueOf(timestamp)+"\t"+prot+"\t"+srcIP+":"+srcPort+">"+dstIP+":"+dstPort);
					outputValue.set(
							//Source -> Destination 
							String.valueOf(S2D_noP)+"\t"+ String.valueOf(S2D_noB)+"\t"+
							String.valueOf(S2D_Byte_D.getMax())+"\t"+ String.valueOf(S2D_Byte_D.getMin())+"\t"+ String.valueOf(S2D_Byte_D.getMean())+"\t"+
							
							//Destination -> Source 
							String.valueOf(D2S_noP)+"\t"+ String.valueOf(D2S_noB)+"\t"+
							String.valueOf(D2S_Byte_D.getMax())+"\t"+ String.valueOf(D2S_Byte_D.getMin())+"\t"+ String.valueOf(D2S_Byte_D.getMean())+"\t"+
							
							//Total information
							String.valueOf(ToT_noP)+"\t"+ String.valueOf(ToT_noB)+"\t"+
							String.valueOf(ToT_Byte_D.getMax())+"\t"+ String.valueOf(ToT_Byte_D.getMin())+"\t"+ String.valueOf(ToT_Byte_D.getMean())+"\t"+ String.valueOf(ToT_Byte_D.getStd())+"\t"+
							String.valueOf(ToT_Prate)+"\t"+ String.valueOf(ToT_Brate)+"\t"+
							String.valueOf(ToT_BTransferRatio)+"\t"+
							String.valueOf(last-timestamp)+"\t"+
							String.valueOf(loss));
					context.write(outputKey, outputValue);
					// test
					//System.out.println( "else outputKey = " + outputKey.toString() ) ;
					//System.out.println( "else outputValue = " + outputValue.toString() ) ;
					
					
					// Reset Arraylist
					S2D_Byte_List.clear();
					D2S_Byte_List.clear();
					ToT_Byte_List.clear();
					
					// Reset Flow Group
					srcIP = nf.getSrcIP();
					dstIP = nf.getDstIP();
					srcPort = nf.getSrcPort();
					dstPort = nf.getDstPort();
					prot = nf.getProt();
					ToT_noP = nf.getPkts();
					ToT_noB = nf.getBytes();
					timestamp = nf.getTimestamp();
					last = timestamp + nf.getDuration();
					
					loss = 0;
					isBiDirectional = false;
					
					D2S_noP = 0;
					S2D_noP = ToT_noP;
					D2S_noB = 0;
					S2D_noB = ToT_noB;
					
					for(int i=0;i<S2D_noP;i++){
						S2D_Byte_List.add((long) ((double)S2D_noB/S2D_noP));
						ToT_Byte_List.add((long) ((double)S2D_noB/S2D_noP));
					}
				}
			}
			
			
			// Only have one direction in Flow Group
			if ( ! isBiDirectional )
			{
				loss = 1 ;
			}
			
			
			// Get Byte : max, min, mean, STD
			DataDistribution S2D_Byte_D = new DataDistribution(S2D_Byte_List) ;
			DataDistribution D2S_Byte_D = new DataDistribution(D2S_Byte_List) ;
			DataDistribution ToT_Byte_D = new DataDistribution(ToT_Byte_List) ;
			
			// Calculate  packet/sec, bytes/sec
			if ( ( last - timestamp ) == 0 )
			{
				ToT_Prate = (double) ToT_noP/0.5 ;
				ToT_Brate = (double) ToT_noB/0.5 ;
			}
			else
			{
				ToT_Prate = (double) ToT_noP/(last-timestamp) ;
				ToT_Brate = (double) ToT_noB/(last-timestamp) ;
			}
			
			// Calculate trnsfer rate. S2D_noP != 0 for all situations
			ToT_BTransferRatio = (double) D2S_noB/S2D_noB ;
			
			
			outputKey.set( String.valueOf(timestamp) + "\t" +
						   prot + "\t" + srcIP + ":" + srcPort + ">" + dstIP + ":" + dstPort ) ;
			outputValue.set(
					// Source -> Destination
					String.valueOf(S2D_noP) + "\t" + String.valueOf(S2D_noB) + "\t" +
					String.valueOf(S2D_Byte_D.getMax()) + "\t" + String.valueOf(S2D_Byte_D.getMin()) + "\t" +
					String.valueOf(S2D_Byte_D.getMean()) + "\t" +
					
					// Destination -> Source
					String.valueOf(D2S_noP) + "\t" + String.valueOf(D2S_noB) + "\t" +
					String.valueOf(D2S_Byte_D.getMax()) + "\t" + String.valueOf(D2S_Byte_D.getMin()) + "\t" +
					String.valueOf(D2S_Byte_D.getMean()) + "\t" +
					
					// Total information
					String.valueOf(ToT_noP) + "\t" + String.valueOf(ToT_noB) + "\t" +
					String.valueOf(ToT_Byte_D.getMax()) + "\t" + String.valueOf(ToT_Byte_D.getMin()) + "\t" +
					String.valueOf(ToT_Byte_D.getMean()) + "\t" + String.valueOf(ToT_Byte_D.getStd()) + "\t" +
					String.valueOf(ToT_Prate) + "\t" + String.valueOf(ToT_Brate) + "\t" +
					String.valueOf(ToT_BTransferRatio) + "\t" +
					String.valueOf(last-timestamp) + "\t" +
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
			System.out.println( "Usage: ./hadoop jar (jar path) (class path) -D tcptime=xxx(ms) -D udptime=xxx(ms) <log file> <out>  , <> : files in HDFS" ) ;
			System.out.println( "otherArgs.length = " + otherArgs.length ) ;
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
		
		job.setKeyValueReduceClass(CompositeKey.class, Text.class, userDefineReducer.class);
		job.setKeyValueMapClass(CompositeKey.class, Text.class, userDefineMapper.class);
		return job.waitForCompletion(true) ? 0 : 1 ;
	}
	
	public static class userDefineMapper extends GenericProxyMapper <CompositeKey,Text>{
		public userDefineMapper() throws Exception { super(CompositeKey.class,Text.class); }
		 @Override
			public void stringToKey(String in, CompositeKey key){
			 	System.out.println("Key:"+ in );
				String k[] = in.split(",");
				key.setTuples(k[0]);
				key.setTime(Long.parseLong(k[1]));
			}
	}
	public static class userDefineReducer extends GenericProxyReducer <CompositeKey,Text>{
		public userDefineReducer() throws Exception { super(CompositeKey.class,Text.class); }
	   
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
