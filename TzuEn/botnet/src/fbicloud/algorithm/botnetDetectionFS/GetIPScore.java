package fbicloud.algorithm.botnetDetectionFS;

import java.io.*;
import java.util.*;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.DoubleWritable;
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

public class GetIPScore extends Configured implements Tool{
	public static class GetIPScoreMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
		private HashMap<String,String> GIDIPMapping  = new HashMap<>();
		private Text interKey = new Text();
		private DoubleWritable interValue = new DoubleWritable();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			//Read from PreMultidimensionalSimRankMR.java output
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(config.get("firstArgs")));
			String[] tmp = new String[2];
			String line;
			for (int i=0;i<status.length;i++){
				//Open all files under the specific folder
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//Input Format		GID			srcIP
					//					17      120.114.117.245
					//					18      120.114.117.80
					tmp = line.split("\t");
					GIDIPMapping.put(tmp[0],tmp[1]);
					line=br.readLine();
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input Format    GID1,GID2    similar              relative GID
			//					10,11    0.06111111    60,69;22,69;12,69;63,69;11,69;13
			
			String[] tmp = value.toString().split("\t");
			if(tmp.length==3){//Normal Case
				String[] GIDPair = tmp[0].split(",");
				String[] IPPair = new String[2];
				IPPair[0]=GIDIPMapping.get(GIDPair[0]);
				IPPair[1]=GIDIPMapping.get(GIDPair[1]);

				if(!IPPair[0].equals(IPPair[1])){
					if(IPPair[0].compareTo(IPPair[1])<0)
						interKey.set(IPPair[0]+","+IPPair[1]);
					else
						interKey.set(IPPair[1]+","+IPPair[0]);
					interValue.set(Double.parseDouble(tmp[1]));
					context.write(interKey,interValue);
				}
			}
		}
	}
	
	public static class GetIPScoreReducerr extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable outputValue= new DoubleWritable();
		private double similarityThreshold;
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			similarityThreshold = Float.parseFloat(config.get("similarity"));
		}
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {
			double sum = 0;
			for(DoubleWritable val : values){
				sum += val.get();
			}
			DecimalFormat df = new DecimalFormat("#.######");
			Double similarity = Double.parseDouble(df.format(sum));
			if(similarity>similarityThreshold){
				outputValue.set(similarity);
				context.write(key , outputValue);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("firstArgs",args[0]);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 3 ) {
			System.out.println("GetIPScore: <GIDIPMappingList> <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Result"); 
		job.setJarByClass(GetIPScore.class);
		
		job.setMapperClass(GetIPScoreMapper.class);
		job.setReducerClass(GetIPScoreReducerr.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new GetIPScore(), args);//Run the class GetIPScore() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}

}
