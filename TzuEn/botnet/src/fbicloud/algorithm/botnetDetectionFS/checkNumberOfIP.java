package fbicloud.algorithm.botnetDetectionFS; 

import java.io.*;
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

public class checkNumberOfIP extends Configured implements Tool{
	public static class routerLogMapper extends Mapper <LongWritable, Text, Text, Text>{
		private Set<String> removeIPlist = new HashSet<String>();
		private String line;
		private Text interKey = new Text();
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
			//Original Log format
			//1411747201 523883430 3875437168 211.79.56.1 8.8.8.8 120.114.100.1 211.79.56.42 90 25   1      124    3875134002 3875134002     53     57544    17   0 0 0 8 0 18 0 0

			String line = value.toString();
			String[] str = line.split("\\s+"); //split by one or more "space"
			//str[4] -> source IP 
			//str[5] -> Destination IP 
			interKey.set("IP");
			interValue.set(str[4] + "\t" + str[5]);
			context.write(interKey,interValue);
		}
	}

	public static class collectLogMapper extends Mapper <LongWritable, Text, Text, Text>{
		private Text interKey = new Text();
		private Text interValue = new Text();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input Order
			//		Date flow start          Duration Proto      Src IP Addr:Port          Dst IP Addr:Port   Flags Tos  Packets    Bytes Flows
			//Input Format
			//		2015-06-14 20:50:50.929     2.011 6        93.113.108.29:80    ->      140.116.1.1:1217  .A.R..   0        3      138     1
			//		2015-06-14 20:50:50.929     2.011 6          140.116.1.1:1217  ->    93.113.108.29:80    ....S.   0        3      144     1

			String line = value.toString();
			String[] str = line.split("\\s+"); //split by one or more "space"
			String[] srcInfo = str[4].split(":");
			String[] dstInfo = str[6].split(":");
			
			interKey.set("IP");
			interValue.set(srcInfo[0] + "\t" + dstInfo[0]);
			context.write(interKey,interValue);
		}
	}

	public static class checkNumberOfIPReducer extends Reducer<Text, Text, NullWritable, Text> {
		private Text outputValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			String[] IP = new String[2];
			Set<String> allIP = new HashSet<>();
			for(Text val : values){
				IP = val.toString().split("\t");
				allIP.add(IP[0]);
				allIP.add(IP[1]);
			}
			outputValue.set("Total srcIPs and dstIPs : "+ allIP.size()); 
			context.write(NullWritable.get(),outputValue);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3 ) {
			//[Key point] '-D tcptime=10000' equal to 'conf.set("tcptime","10000")' so we can do conf.get("tcptime") to get the value
			System.out.println("checkNumberOfIP: <routerLog> <collectLog> <MergeOut>");
			System.out.println("otherArgs.length = "+otherArgs.length);
			System.exit(2);
		}

		Job job = Job.getInstance(conf,"Merge the collect Log and router Log into file and sort by 'first' timestamp");
		job.setJarByClass(checkNumberOfIP.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, routerLogMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, collectLogMapper.class);
		job.setReducerClass(checkNumberOfIPReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]) );

		job.setNumReduceTasks(1);//Set only one reduce

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new checkNumberOfIP(), args);//Run the class checkNumberOfIP() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}
}
