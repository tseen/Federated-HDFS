package fbicloud.algorithm.botnetDetectionFS; 

import fbicloud.algorithm.botnetDetection.MergeLogSecondarySort.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

//org.apache.hadoop.mapreduce.x -> Map Reduce 2.0
//org.apache.hadoop.mapred.x -> Map Reduce 1.0
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MergeLog_old extends Configured implements Tool{
	public static class routerLogMapper extends Mapper <LongWritable, Text, compositeKey, Text>{
		private Set<String> removeIPlist = new HashSet<String>();
		private String line;
		private compositeKey comKey = new compositeKey();
		private Text interValue = new Text();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
		
			//Read File from HDFS--remove ip list
			Path path = new Path("removeIPlist");
			FileSystem fs = path.getFileSystem(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			line=br.readLine();                                                     
			while (line != null){
				removeIPlist.add(line);
				line=br.readLine();
			}
		}
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			long first = 0L;
			//Original Log format
			//1411747201 523883430 3875437168 211.79.56.1 8.8.8.8 120.114.100.1 211.79.56.42 90 25   1      124    3875134002 3875134002     53     57544    17   0 0 0 8 0 18 0 0

			String line = value.toString();
			String[] str = line.split("\\s+"); //split by one or more "space"
			//str[4] -> source IP 
			//str[5] -> Destination IP 
			if( !removeIPlist.contains(str[4]) && !removeIPlist.contains(str[5]) ){
				first = Long.parseLong(str[11]);
				comKey.setSameKey("netflow");
				comKey.setTime(first);
				interValue.set(value);
				context.write(comKey,interValue);
			}
		}
	}

	public static class collectLogMapper extends Mapper <LongWritable, Text, compositeKey, Text>{
		private compositeKey comKey = new compositeKey();
		private Text interValue = new Text();
		private long diffTime = 0L;
		public void setup(Context context) throws IOException, InterruptedException {
			String pcapInitialTime="";
			long unixTime = 0L;
			long sysTime = 0L;

			Configuration config = context.getConfiguration();
			//Get the parameter from -D command line option
			pcapInitialTime = config.get("pcapInitialTime");
			try{
				SimpleDateFormat pattern = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS");
				Date date = pattern.parse(pcapInitialTime);
				unixTime = date.getTime();

				sysTime = Long.parseLong(config.get("sysTime"));//Put the "first" value from first netflow record

				diffTime = unixTime - sysTime;
				System.out.println("Correct - diffTime = " + String.valueOf(diffTime));
			}catch(Exception e){
				System.out.println("Error:" + e.getMessage());
				System.out.println("pcapInitialTime = " + pcapInitialTime);
				diffTime=Long.MAX_VALUE;
			}
		}
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input Order
			//		Date flow start          Duration Proto      Src IP Addr:Port          Dst IP Addr:Port   (Flags Tos)  Packets    Bytes Flows
			//Input Format
			//		2015-06-14 20:50:50.929     2.011 6        93.113.108.29:80    ->      140.116.1.1:1217  (.A.R..   0)        3      138     1
			//		2015-06-14 20:50:50.929     2.011 6          140.116.1.1:1217  ->    93.113.108.29:80    (....S.   0)        3      144     1


			long unixTime = 0L;
			long first=0L;
			long last=0L;

			String line = value.toString();
			String[] str = line.split("\\s+"); //split by one or more "space"
			String[] srcInfo = str[4].split(":");
			String[] dstInfo = str[6].split(":");

			try{
				SimpleDateFormat pattern = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS");
				Date date = pattern.parse(str[0]+"_"+str[1]);
				unixTime = date.getTime();
				first = unixTime - diffTime;
				
				String [] tmp=str[2].split("\\.");
				last = first + Integer.parseInt(tmp[0])*1000+Integer.parseInt(tmp[1]);//Convert the 'second.millisecond' to 'millisecond'
			}catch(Exception e){
				System.out.println(key.toString() + "\t" +"Error:"+e.getMessage());
				System.out.println(key.toString() + "\t" +"date = "+ str[0]+"_"+str[1]);
			}
			//Original Log format
			//1411747201 523883430 3875437168 211.79.56.1 8.8.8.8 120.114.100.1 211.79.56.42 90 25   1      124    3875134002 3875134002     53     57544    17   0 0 0 8 0 18 0 0
			//xxxxxxxxxx xxxxxxxxx xxxxxxxxxx xxx.xx.xx.x srcaddr    dstaddr    xxx.xx.xx.xx xx xx dPkts  dOctets     first     last      srcPort  dstport  prot  Tos tcpflags x x x xx x x 
			comKey.setSameKey("netflow");
			comKey.setTime(first);
			interValue.set("xxxxxxxxxx" + " " +
						"xxxxxxxxx" + " " +
						"xxxxxxxxxx" + " " +
						"xxx.xx.xx.x" + " " +
						srcInfo[0] + " " +
						dstInfo[0] + " " +
						"xxx.xx.xx.xx" + " " +
						"xx" + " " +
						"xx" + " " +
						str[7] + " " +
						str[8] + " " +
						String.valueOf(first) + " " +
						String.valueOf(last) + " " +
						srcInfo[1] + " " +
						dstInfo[1] + " " +
						str[3] + " "+ 
						"xx" + " " +
						"xx" + " " +
						"x" + " " +
						"x" + " " +
						"x" + " " +
						"xxx" + " " +
						"x" + " " +
						"x" + " " );
			context.write(comKey,interValue);
		}
	}

	public static class MergeLog_oldReducer extends Reducer<compositeKey, Text, NullWritable, Text> {
		private Text outputValue = new Text();
		public void reduce(compositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			for(Text val : values){
				outputValue.set(val); 
				context.write(NullWritable.get(),outputValue);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3 ) {
			//[Key point] '-D tcptime=10000' equal to 'conf.set("tcptime","10000")' so we can do conf.get("tcptime") to get the value
			System.out.println("MergeLog_old: <routerLog> <collectLog> <MergeOut>");
			System.out.println("otherArgs.length = "+otherArgs.length);
			System.exit(2);
		}

		Job job = Job.getInstance(conf,"Merge the collect Log and router Log into file and sort by 'first' timestamp");
		job.setJarByClass(MergeLog_old.class);
		
		job.setMapOutputKeyClass(compositeKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, routerLogMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, collectLogMapper.class);
		job.setReducerClass(MergeLog_oldReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]) );

		job.setPartitionerClass(keyPartitioner.class);
		job.setSortComparatorClass(compositeKeyComparator.class);
		job.setGroupingComparatorClass(keyGroupingComparator.class);

		job.setNumReduceTasks(1);//Set only one reduce

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new MergeLog_old(), args);//Run the class MergeLog_old() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}
}
