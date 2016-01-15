package fbicloud.algorithm.botnetDetectionFS; 

import java.io.*;
import java.util.*;

//org.apache.hadoop.mapreduce.x -> Map Reduce 2.0
//org.apache.hadoop.mapred.x -> Map Reduce 1.0
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
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

public class GetNegativeIPs extends Configured implements Tool{
	public static class GetNegativeIPsMapper extends Mapper <LongWritable, Text, Text, Text>{
		private Text interKey = new Text();
		private Text interValue = new Text();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input format
			//3197620261	17	140.116.6.2:55670>1.165.186.246:16470	1	44	44	44	44.0	1	1016	1016	1016	1016.0	2	1060	1016	44	530.0	486.0	0.08	42.4	23.09090909090909	25	0
			String line = value.toString();
			String[] tmp = line.split("\\s+"); //split by one or more "space"
			String srcIP = tmp[2].split(":")[0];

			interKey.set("Key");
			interValue.set(srcIP);
			context.write(interKey,interValue);
		}
	}

	public static class GetNegativeIPsReducer extends Reducer<Text, Text, NullWritable, Text> {
		private Text outputValue = new Text();
		private HashSet<String> detectedIPs = new HashSet<String>();
		public void setup(Context context) throws IOException, InterruptedException {
			String line;
			String srcIPs;
			String [] srcIPsArray;
			String [] IPsplit;

			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(config.get("GroupPhase2MR_out")));
			for (int i=0;i<status.length;i++){
				//Open all files under the specific folder
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				//Input format
				//315     6,G90   1.0,52.0,52.0,52.0,52.0,3.0,160.0,53.0,53.0,53.0,4.0,212.0,53.0,52.0,52.75,0.43301,0.70851,37.55083,3.07692,6.49534     140.116.131.204,140.116.102.122,        140.125.48.168,140.125.204.66,140.125.151.52,140.125.207.40,140.125.195.178,140.125.202.73,140.125.191.124,
				while (line != null){
					srcIPs = line.split("\\s+")[3];
					srcIPsArray = srcIPs.split(",");
					for (int j=0;j<srcIPsArray.length;j++){
						/*
						   Check whether it is private IP
						   Private IP address range
						   10.0.0.0 - 10.255.255.255
						   172.16.0.0 - 172.31.255.255
						   192.168.0.0 - 192.168.255.255
						 */
						IPsplit = srcIPsArray[j].split("\\.");
						if ( IPsplit[0].equals("10") ){
							continue;
						}
						else if(IPsplit[0].equals("172") &&
								(16 <= Integer.parseInt(IPsplit[1]) && Integer.parseInt(IPsplit[1]) <= 31)	){
							continue;
						}
						else if(IPsplit[0].equals("192") && IPsplit[1].equals("168")){
							continue;
						}
						else{
							detectedIPs.add(srcIPsArray[j]);
						}
					}
					line=br.readLine();
				}
			}
		}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			String [] IPsplit;
			HashSet<String> allSrcIPs= new HashSet<String>();

			/*
			   Check whether it is private IP
			   Private IP address range
			   10.0.0.0 - 10.255.255.255
			   172.16.0.0 - 172.31.255.255
			   192.168.0.0 - 192.168.255.255
			 */
			for(Text val : values){
				IPsplit = val.toString().split("\\.");
				if ( IPsplit[0].equals("10") ){
					continue;
				}
				else if(IPsplit[0].equals("172") &&
						(16 <= Integer.parseInt(IPsplit[1]) && Integer.parseInt(IPsplit[1]) <= 31)	){
					continue;
				}
				else if(IPsplit[0].equals("192") && IPsplit[1].equals("168")){
					continue;
				}
				else{
					allSrcIPs.add(val.toString());
				}
			}

			outputValue.set("allSrcIPs = "+ allSrcIPs.size()); 
			context.write(NullWritable.get(),outputValue);

			outputValue.set("detectedIPs = "+detectedIPs.size()); 
			context.write(NullWritable.get(),outputValue);

			allSrcIPs.removeAll(detectedIPs); //remove all objects relative to detectedIPs

			outputValue.set("allNegativeIPs = "+ allSrcIPs.size()); 
			context.write(NullWritable.get(),outputValue);


			outputValue.set("===detectedIPs==="); 
			context.write(NullWritable.get(),outputValue);
			for (String IP : detectedIPs) {
				outputValue.set(IP); 
				context.write(NullWritable.get(),outputValue);
			}

			outputValue.set("===allNegativeIPs==="); 
			context.write(NullWritable.get(),outputValue);

			//Emit all Negative IPs
			for (String IP : allSrcIPs) {
				outputValue.set(IP); 
				context.write(NullWritable.get(),outputValue);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4 ) {
			//[Key point] '-D tcptime=10000' equal to 'conf.set("tcptime","10000")' so we can do conf.get("tcptime") to get the value
			System.out.println("GetNegativeIPs: <BiDirectional> <UniDirectional> <GroupPhase2MR_out> <NegativeIPs>");
			System.out.println("otherArgs.length = "+otherArgs.length);
			System.exit(2);
		}

		conf.set("GroupPhase2MR_out",args[2]);

		Job job = Job.getInstance(conf,"Get the detected IP");
		job.setJarByClass(GetNegativeIPs.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GetNegativeIPsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GetNegativeIPsMapper.class);
		job.setReducerClass(GetNegativeIPsReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]) );

		job.setNumReduceTasks(1);//Set only one reduce

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new GetNegativeIPs(), args);//Run the class GetNegativeIPs() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}
}
