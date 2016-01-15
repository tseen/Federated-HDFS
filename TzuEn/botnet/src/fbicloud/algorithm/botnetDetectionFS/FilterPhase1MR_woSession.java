package fbicloud.algorithm.botnetDetectionFS;

import fbicloud.algorithm.botnetDetection.FilterPhaseSecondarySort.*;
import fbicloud.algorithm.classes.netflowRecord;
import fbicloud.algorithm.classes.DataDistribution;

import java.io.*;
import java.util.*;

//org.apache.hadoop.mapreduce.x -> Map Reduce 2.0
//org.apache.hadoop.mapred.x -> Map Reduce 1.0
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

public class FilterPhase1MR_woSession extends Configured implements Tool{
	public static Set<String> white = new HashSet<String>();
	public static Set<String> whiteDomain = new HashSet<String>();
	public static Set<String> dns = new HashSet<String>();
	public static Set<String> domain = new HashSet<String>();
	public static class job1Mapper extends Mapper <LongWritable, Text, compositeKey, Text>{
		//private Text interKey = new Text();
		private netflowRecord r = new netflowRecord();	
		private	String line;
		private boolean filterdomain;
		private compositeKey comKey = new compositeKey();
		private Text interValue = new Text();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String tmpStr;

			//Read File from HDFS--white list
			Path pathWhite = new Path("white");
			FileSystem fsWhite = pathWhite.getFileSystem(config);
			BufferedReader brWhite = new BufferedReader(new InputStreamReader(fsWhite.open(pathWhite)));
			line=brWhite.readLine();
			while (line != null){
				FilterPhase1MR_woSession.white.add(line);
				if(line.split("\\.").length == 2){
					FilterPhase1MR_woSession.whiteDomain.add(line);
				}
				line=brWhite.readLine();
			}

			//Read File from HDFS--dns list
			Path pathDNS = new Path("dns");
			FileSystem fsDNS = pathDNS.getFileSystem(config);
			BufferedReader brDNS = new BufferedReader(new InputStreamReader(fsDNS.open(pathDNS)));
			line=brDNS.readLine();
			while (line != null){
				FilterPhase1MR_woSession.dns.add(line);
				line=brDNS.readLine();
			}

			//Read File from HDFS--domain
			Path pathDomain = new Path("domain");
			FileSystem fsDomain = pathDomain.getFileSystem(config);
			BufferedReader brDomain = new BufferedReader(new InputStreamReader(fsDomain.open(pathDomain)));
			line=brDomain.readLine();
			while (line != null){
				FilterPhase1MR_woSession.domain.add(line);
				line=brDomain.readLine();
			}
			tmpStr = config.get("filterdomain");//Hadoop will parse the -D tcptime=xxx(ms) and set the tcptime value ,and then call run()
			if(tmpStr.equals("false"))
				filterdomain = false;
			else if(tmpStr.equals("true"))
				filterdomain = true;
			else
				System.exit(1);//exit
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//Input Format:
			//		unix_secs	unix_nsecs	sysUptime	
			//		exaddr	srcaddr	dstaddr	nexthop	input_interface	output_interface
			//		dPkts	dOctets	First	Last	srcPort	dstPort	Prot
			//		tos	tcp_flags	engine_type	engine_id	src_mask	dst_mask	src_AS	dst_AS
			//Input Example:
			//		1411747201 523883430 3875437168
			//		211.79.56.1 132.163.4.102 140.116.213.214 211.79.56.26	90	41
			//		1 76 3875133168 3875133168 123 9139 17
			//		0 0 0 8 16 16 2648 18177
			long firstTime = 3195833370L;//(ms)
			long TimeWindoes = 600000L;//(600,000ms = 600s = 10mins )
			long noTW=0L;

			r.parseRaw(value.toString());
			String [] srcIPArray = r.getSrcIP().split("\\.");
			String [] dstIPArray = r.getDstIP().split("\\.");
			String subSrcIP = srcIPArray[0]+"."+srcIPArray[1];
			String subDstIP = dstIPArray[0]+"."+dstIPArray[1];
			
			//Get the flow with protocol=TCP or UDP 
			if( r.getProt().equals("6") || r.getProt().equals("17") ){//6 -> tcp ,7 -> udp
				//if Both srcIP and dstIP not in the white list & DNS list -> Don't context.write any <key,value> set
				if( !white.contains(r.getSrcIP()) && 
					!white.contains(r.getDstIP()) && 
					!whiteDomain.contains(subSrcIP) && 
					!whiteDomain.contains(subDstIP) && 
					!dns.contains(r.getSrcIP()) && 
					!dns.contains(r.getDstIP())){


					noTW = ( r.getFirst() - firstTime )/TimeWindoes;

					//Get the Flow which SrcIP or DstIP is 140.116
					if(filterdomain){
						if(domain.contains(subSrcIP) || domain.contains(subDstIP)){
							//Set the composite key = < hash ,first >
							comKey.setTuples(r.getKey() + String.valueOf(noTW));
							comKey.setTime(r.getFirst());

							//Set the value 
							interValue.set(value.toString());

							context.write(comKey, interValue);
						}
					}
					else{
						//Set the composite key = < hash ,first >
						comKey.setTuples(r.getKey() + String.valueOf(noTW));
						comKey.setTime(r.getFirst());

						//Set the value 
						interValue.set(value.toString());

						context.write(comKey, interValue);
					}
				}
			}
		}
	}
	public static class job1Reducer extends Reducer<compositeKey, Text, Text, Text> {
		private netflowRecord tmpR = new netflowRecord();
		//private long tcptime=0L;
		//private long udptime=0L;
		//private long time=0L;
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		/*
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			//Get the parameter from -D command line option
			tcptime = Long.parseLong(config.get("tcptime"));//Hadoop will parse the -D tcptime=xxx(ms) and set the tcptime value ,and then call run()
			udptime = Long.parseLong(config.get("udptime"));//Hadoop will parse the -D udptime=xxx(ms) and set the udptime value ,and then call run()
		}*/
		public void reduce(compositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			Iterator<Text> val = values.iterator();

			String srcIP="";
			String dstIP="";
			String srcPort="";
			String dstPort="";
			String prot="";//protocol

			/*----------------------------------------------*
			 *	Source to Destination information			*
			 *----------------------------------------------*/
			long S2D_noP=0;
			long S2D_noB=0;
			ArrayList<Long> S2D_Byte_List = new ArrayList<Long>();

			/*----------------------------------------------*
			 *	Destination to Source information			*
			 *----------------------------------------------*/
			long D2S_noP=0;
			long D2S_noB=0;
			ArrayList<Long> D2S_Byte_List = new ArrayList<Long>();

			/*----------------------------------------------*
			 *	S2D and D2S total information			*
			 *----------------------------------------------*/
			long ToT_noP=0;
			long ToT_noB=0;
			ArrayList<Long> ToT_Byte_List = new ArrayList<Long>();

			double ToT_Prate=0;
			double ToT_Brate=0;
			//double ToT_PTransferRatio=0;//	D2S_noP/S2D_noP
			double ToT_BTransferRatio=0;//	D2S_noB/S2D_noB
			
			long first=0;
			long last=0;
			//long duration=0;
			
			long loss=0;

			boolean isBiDirectional=false;
			
			String tmpStr="";
			/*--------------------------------------------------------------------------------------*
			 *	Organize the Flow Group	by TCP/UDP time. Each Flow Group has same five-tuples		*
			 *	TCP -> Time interval between current flow and previous flow samller than 21000(ms)	*
			 *		-> Group as the same flow														*
			 *	UDP -> Time interval between current flow and previous flow samller than 22000(ms)	*
			 *		-> Group as the same flow														*
			 *--------------------------------------------------------------------------------------*/

			if(val.hasNext()){//Get the first record and assign value
				tmpStr=val.next().toString();
				tmpR.parseRaw(tmpStr);
				prot = tmpR.getProt();
				
				/*
				//Set the tcptime, udptime
				if(Long.parseLong(prot)==6)	//TCP
					time = tcptime;
				else						//UDP
					time = udptime;
				*/

				srcIP = tmpR.getSrcIP();
				dstIP = tmpR.getDstIP();
				srcPort = tmpR.getSrcPort();
				dstPort = tmpR.getDstPort();
				ToT_noP = tmpR.getPkts();
				ToT_noB = tmpR.getBytes();
				first = tmpR.getFirst();
				last = tmpR.getLast();

				D2S_noP = 0;
				S2D_noP = ToT_noP;
				D2S_noB = 0;
				S2D_noB = ToT_noB;

				//if S2D_noP = 3 (packets), S2D_noB = 150(bytes)
				//then add three packets each has 50 bytes
				for(int i=0;i<S2D_noP;i++){
					S2D_Byte_List.add((long) ((double)S2D_noB/S2D_noP));				
					ToT_Byte_List.add((long) ((double)S2D_noB/S2D_noP));
				}

				loss = 0;
				isBiDirectional=false;
			}
			while(val.hasNext()){
				tmpStr=val.next().toString();
				tmpR.parseRaw(tmpStr);

				//if(tmpR.getFirst()-first < time){

					//-----------------Check time order-----------------
					if(tmpR.getFirst()<first){
						outputKey.set("ERROR error");//CHECK
						outputValue.set("ERROR error");//CHECK
						context.write(outputKey, outputValue);
					}
					//--------------------------------------------------

					if(!tmpR.getSrcIP().equals(srcIP))//Check Flow Group is uni-directional or bi-directional
						isBiDirectional = true;

					if(tmpR.getLast()>last)//Get the latter 'last'
						last = tmpR.getLast();

					//Accumulate the Flow Group's Feature
					ToT_noP = ToT_noP + tmpR.getPkts();
					ToT_noB = ToT_noB + tmpR.getBytes();
					for(int i=0;i<tmpR.getPkts();i++){
						ToT_Byte_List.add((long) ((double)tmpR.getBytes()/tmpR.getPkts()));
					}
					
					if(srcIP.equals(tmpR.getSrcIP()) && dstIP.equals(tmpR.getDstIP())){
						S2D_noP = S2D_noP + tmpR.getPkts();
						S2D_noB = S2D_noB + tmpR.getBytes();
						for(int i=0;i<tmpR.getPkts();i++){
							S2D_Byte_List.add((long) ((double)tmpR.getBytes()/tmpR.getPkts()));
						}
					}
					else{
						D2S_noP = D2S_noP + tmpR.getPkts();
						D2S_noB = D2S_noB + tmpR.getBytes();
						for(int i=0;i<tmpR.getPkts();i++){
							D2S_Byte_List.add((long) ((double)tmpR.getBytes()/tmpR.getPkts()));
						}
					}
				//}
				/*
				else{
					if(!isBiDirectional)	//Only have one direction in Flow Group
						loss = 1;
					
					//Get Byte : Max,min,mean,STD 
					DataDistribution S2D_Byte_D = new DataDistribution(S2D_Byte_List);
					DataDistribution D2S_Byte_D = new DataDistribution(D2S_Byte_List);
					DataDistribution ToT_Byte_D = new DataDistribution(ToT_Byte_List);

					//Calculate  packet/sec,bytes/sec
					if((last-first) == 0){//Set duration as 0.5 millisecs
						ToT_Prate = (double) ToT_noP/0.5;
						ToT_Brate = (double) ToT_noB/0.5;
					}
					else{
						ToT_Prate = (double) ToT_noP/(last-first);
						ToT_Brate = (double) ToT_noB/(last-first);
					}

					//Calculate trnsfer rate. S2D_noP != 0 for all situations
					ToT_PTransferRatio = (double) D2S_noP/S2D_noP;
					ToT_BTransferRatio = (double) D2S_noB/S2D_noB;

					//Ignore all IAT, S2D_Byte_STD, D2S_Byte_STD, ToT_PTransferRatio
					outputKey.set(String.valueOf(first)+"\t"+prot+"\t"+srcIP+":"+srcPort+">"+dstIP+":"+dstPort);
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
							String.valueOf(last-first)+"\t"+
							String.valueOf(loss));
					context.write(outputKey, outputValue);

					//Reset Arraylist
					S2D_Byte_List.clear();
					D2S_Byte_List.clear();
					ToT_Byte_List.clear();

					//Reset Flow Group
					srcIP = tmpR.getSrcIP();
					dstIP = tmpR.getDstIP();
					srcPort = tmpR.getSrcPort();
					dstPort = tmpR.getDstPort();
					prot = tmpR.getProt();
					ToT_noP = tmpR.getPkts();
					ToT_noB = tmpR.getBytes();
					first = tmpR.getFirst();
					last = tmpR.getLast();

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
				}*/
			}

			if(!isBiDirectional)	//Only have one direction in Flow Group
				loss = 1;

			//Get Byte : Max,min,mean,STD 
			DataDistribution S2D_Byte_D = new DataDistribution(S2D_Byte_List);
			DataDistribution D2S_Byte_D = new DataDistribution(D2S_Byte_List);
			DataDistribution ToT_Byte_D = new DataDistribution(ToT_Byte_List);

			//Calculate  packet/sec,bytes/sec
			if((last-first) == 0){
				ToT_Prate = (double) ToT_noP/0.5;
				ToT_Brate = (double) ToT_noB/0.5;
			}
			else{
				ToT_Prate = (double) ToT_noP/(last-first);
				ToT_Brate = (double) ToT_noB/(last-first);
			}

			//Calculate trnsfer rate. S2D_noP != 0 for all situations
			//ToT_PTransferRatio = (double) D2S_noP/S2D_noP;
			ToT_BTransferRatio = (double) D2S_noB/S2D_noB;

			//Ignore all IAT, S2D_Byte_STD, D2S_Byte_STD, ToT_PTransferRatio
			outputKey.set(String.valueOf(first)+"\t"+prot+"\t"+srcIP+":"+srcPort+">"+dstIP+":"+dstPort);
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
					String.valueOf(last-first)+"\t"+
					String.valueOf(loss));
			context.write(outputKey, outputValue);
		}
	}
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2 ) {
			//[Key point] '-D tcptime=10000' equal to 'conf.set("tcptime","10000")' so we can do conf.get("tcptime") to get the value
			System.out.println("Usage: ./hadoop jar (jar path) (class path) -D tcptime=xxx(ms) -D udptime=xxx(ms) <log file> <out>  , <> : files in HDFS");
			System.out.println("otherArgs.length = "+otherArgs.length);
			System.exit(2);
		}

		Job job1 = Job.getInstance(conf,"Filter out 'White list' and preserve 'TCP/UDP' flow and extract features from flow");
		job1.setJarByClass(FilterPhase1MR_woSession.class);
		
		job1.setMapperClass(job1Mapper.class);
		job1.setReducerClass(job1Reducer.class);
		
		job1.setMapOutputKeyClass(compositeKey.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setPartitionerClass(keyPartitioner.class);
		job1.setSortComparatorClass(compositeKeyComparator.class);
		job1.setGroupingComparatorClass(keyGroupingComparator.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		return job1.waitForCompletion(true) ? 0 : 1;

	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new FilterPhase1MR_woSession(), args);//Run the class FilterPhase1MR_woSession() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}
}
