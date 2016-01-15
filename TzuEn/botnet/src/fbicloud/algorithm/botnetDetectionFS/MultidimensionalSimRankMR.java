package fbicloud.algorithm.botnetDetectionFS;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultidimensionalSimRankMR extends Configured implements Tool{
	
	public static class FirstIterationMapper extends Mapper <LongWritable, Text, Text, Text> {
		private Text interKey = new Text();
		private Text interValue = new Text();
	    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			int i;
			String[] tmp = value.toString().split("\t");
			if(tmp.length==3){//Normal Case
				//Input Format	two groups	Score	(two Group);(two Group);(two Group);...
				//				100,1615      0		360,472;245,360;360,489;236,360
				String[] GroupPair = tmp[2].split(";");
				for(i=0;i<GroupPair.length;i++){
					//key : 	360,472
					//value :	0
					interKey.set(GroupPair[i]);
					interValue.set(tmp[1]);
					context.write(interKey,interValue);
				}

				//Emit itself -> 100,1615	360,472;245,360;360,489;236,360
				interKey.set(tmp[0]);
				interValue.set(tmp[2]);
				context.write(interKey,interValue);
			}
			//tmp.length == 2 is 'Special Case'
			//Input Format	two groups	Score
			//				1014,1014	  1
			//Because 1014 only similar to one FVID. 
			//if A similar to  C 
			//than s(A,A) 1 s(C,C) -> we will ignore s(C,C) default
			//We will ignore this kind of situation
		}
	}
	
	public static class FirstIterationReducer extends Reducer<Text, Text, Text, Text> {
		private HashMap<String,String> FVAdjacencyList  = new HashMap<>();
		String FVIDPairPath;
		private FSDataOutputStream out;
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		public void setup(Context context) throws IOException, InterruptedException {
			String line;
			Configuration config = context.getConfiguration();
			
			//Read FVIDGraphMR
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(config.get("FVGraphMR_adjacencyList")));//Read from FVGraphMR.java output(adjacency list)
			for (int i=0;i<status.length;i++){
				//Open all files under the specific folder
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//Input Format  	FVID		FVID-A,FVID-B,FVID-M,
					//					1001		403,404,587,
					String[] tmp = line.split("\\s+");
					FVAdjacencyList.put(tmp[0],tmp[1]);
					line=br.readLine();
				}
			}

			//Write output file to HDFS
			FVIDPairPath = config.get("FVIDPairPath");
			fs.mkdirs(new Path("/user/hpds/"+FVIDPairPath));
			out = fs.create( new Path("/user/hpds/"+FVIDPairPath+"/part-r-"+context.getTaskAttemptID().getTaskID().getId()));
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			out.close();
		}
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			int i,j;
			int count =0;
			double sum=0;
			String relatedGroup=null;
			String outputStr;

			String [] GroupPair = key.toString().split(",");
			for(Text val : values){
				try{
					sum = sum + Double.parseDouble(val.toString());
					count++;
				}
				catch (Exception ex){ //if val equal to 'itself'
					relatedGroup = val.toString();
				}
			}

			if(relatedGroup==null){//Get the relatedGroup
				StringBuffer outputStrbf = new StringBuffer();
				String [] dstGroup1 = FVAdjacencyList.get(GroupPair[0]).split(",");
				String [] dstGroup2 = FVAdjacencyList.get(GroupPair[1]).split(",");

				//Update outputStrbf
				for(i=0;i<dstGroup1.length;i++){
					for(j=0;j<dstGroup2.length;j++){
						if(!dstGroup1[i].equals(dstGroup2[j])){
							//FVID1,FVID2 -> FVID1 < FVID2 
							if(Integer.parseInt(dstGroup1[i]) < Integer.parseInt(dstGroup2[j]))
								outputStrbf.append(dstGroup1[i] + "," +dstGroup2[j] + ";");
							else
								outputStrbf.append(dstGroup2[j] + "," +dstGroup1[i] + ";");
						}
					}
				}
				relatedGroup = outputStrbf.toString();
			}
			if(GroupPair[0].equals(GroupPair[1])){//331,331		1	291,342;321,342;291,321;
				outputKey.set(key);
				outputValue.set("1\t"+relatedGroup);
				context.write(outputKey,outputValue);
			}
			else{
				outputKey.set(key);
				outputValue.set(String.valueOf(sum*0.6/count)+"\t"+relatedGroup);
				context.write(outputKey,outputValue);
			}

			//Write to HDFS
			outputStr = key.toString()+"\n";
			out.write(outputStr.getBytes(),0,outputStr.length());
		}
	}

	public static class RemoveRedundantMapper extends Mapper <LongWritable, Text, Text, Text> {
		private Set<String> FVIDPair = new HashSet<>();//Store all FVID pairs
		private Text interKey = new Text();
		private Text interValue = new Text();
		public void setup(Context context) throws IOException, InterruptedException {
			String line;
			Configuration config = context.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(config.get("FVIDPairPath")));//Read from last iteration
			for (int i=0;i<status.length;i++){
				//Open all files under the specific folder
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//Input Format	two groups	
					//		100,1615
					FVIDPair.add(line);
					line=br.readLine();
				}
			}
		}
	    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			StringBuffer outsb = new StringBuffer();
			//outsb.delete(0, outsb.length());//Reset

			String[] tmp = value.toString().split("\t");
			if(tmp.length==3){//Normal Case
				//Input Format	two groups	Score	(two Group);(two Group);(two Group);...
				//				100,1615      0		360,472;245,360;360,489;236,360
				String[] GroupPair = tmp[2].split(";");
				for(int i=0;i<GroupPair.length;i++){
					//key : 	2740,1730
					//value :	0
					if(FVIDPair.contains(GroupPair[i])){//Preserve the FVID 
						outsb.append(GroupPair[i]+";");
					}
				}
				interKey.set(tmp[0]+"\t"+tmp[1]);
				interValue.set(outsb.toString());
				context.write(interKey,interValue);
			}
		}
	}

	public static class OtherIterationMapper extends Mapper <LongWritable, Text, Text, Text> {
		private Text interKey = new Text();
		private Text interValue = new Text();
	    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			int i;
			String[] tmp = value.toString().split("\t");
			if(tmp.length==3){//Normal Case
				//Input Format	two groups	Score	(two Group);(two Group);(two Group);...
				//				100,1615      0		360,472;245,360;360,489;236,360
				String[] GroupPair = tmp[2].split(";");
				for(i=0;i<GroupPair.length;i++){
					//key : 	2740,1730
					//value :	0
					interKey.set(GroupPair[i]);
					interValue.set(tmp[1]);
					context.write(interKey,interValue);
				}

				//Emit itself -> 100,1615	2740,1730;2740,191;2740,3263;2740,909
				interKey.set(tmp[0]);
				interValue.set(tmp[2]);
				context.write(interKey,interValue);
			}
		}
	}
	
	public static class OtherIterationReducer extends Reducer<Text, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			int count =0;
			double sum=0;
			String relatedGroup=null;
			String [] GroupPair = key.toString().split(",");
			
			for(Text val : values){
				try{
					sum = sum + Double.parseDouble(val.toString());
					count++;
				}
				catch (Exception ex){ //if val equal to 'itself'
					relatedGroup = val.toString();
				}
			}
			/*
				General Case	relatedGroup != null
				Special Case	relatedGroup == null
				
				<FVIDGraph>
				13187	2089,8747,(otherFVID),(otherFVID),...
				2089	13187
				8747	13187

				<after first iteration withRedundantIPPair>
				2089,8747       0.6	(without contribute IP Pairs)
			*/
			if(relatedGroup!=null){
				if(GroupPair[0].equals(GroupPair[1])){//331,331		1	291,342;321,342;291,321;
					outputKey.set(key);
					outputValue.set("1\t"+relatedGroup);
					context.write(outputKey,outputValue);
				}
				else{
					outputKey.set(key);
					outputValue.set(String.valueOf(sum*0.6/count)+"\t"+relatedGroup);
					context.write(outputKey,outputValue);
				}
			}else{
				if(GroupPair[0].equals(GroupPair[1])){//331,331		1	291,342;321,342;291,321;
					outputKey.set(key);
					outputValue.set("1\t"+relatedGroup);
					context.write(outputKey,outputValue);
				}
				else{
					outputKey.set(key);
					outputValue.set(String.valueOf(sum*0.6/count));//Don't emit relatedGroup
					context.write(outputKey,outputValue);
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		int iteration,i;
		boolean isSuccessfully=true;
		Configuration conf = this.getConf();
		iteration=Integer.parseInt(conf.get("iteration"));
		conf.set("FVGraphMR_adjacencyList",args[0]);
		conf.set("FVIDPairPath",args[2]+"_FVIDPair");
		
		//Map output compression
		//conf.set("mapreduce.map.output.compress", "true");
		//conf.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzoCodec");
		//conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.BZip2Codec");
		//conf.set("mapreduce.output.fileoutputformat.compress", "false");

		//Reduce output compression
		/*
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "block");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
		 */
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 3 ) {
			System.out.println("MultidimensionalSimRankMR <adjacencylist> <contributeList> <out>");
			System.exit(2);
		}

		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	SimRank First Iteration											*
		 *------------------------------------------------------------------*/

		//Need to set 'conf' before Creates a new Job (job.getInstance)
		Job job1 = Job.getInstance(conf, "MultidimensionalSimRankMR Iteration1"); 
		job1.setJarByClass(MultidimensionalSimRankMR.class);

		//Set Mapper,Reducer class
		job1.setMapperClass(FirstIterationMapper.class);
		job1.setReducerClass(FirstIterationReducer.class);

		//Set input,output path
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]+"_Iteration1_withRedundantIPPair"));

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.waitForCompletion(true);

		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Reomve redundant IP Pairs										*
		 *------------------------------------------------------------------*/

		Job job2 = Job.getInstance(conf, "Reomve redundant IP Pairs"); 
		job2.setJarByClass(MultidimensionalSimRankMR.class);

		//Set Mapper class
		job2.setMapperClass(RemoveRedundantMapper.class);

		//Set input,output path
		FileInputFormat.addInputPath(job2, new Path(args[2]+"_Iteration1_withRedundantIPPair"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]+"_Iteration1"));

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.waitForCompletion(true);
		
		/*------------------------------------------------------------------*
		 *								Job 3		 						*
		 *	SimRank Other Iteration											*
		 *------------------------------------------------------------------*/

		for(i=iteration-1;i>0;i--){
			//Need to set 'conf' before Creates a new Job (job.getInstance)
			Job job3 = Job.getInstance(conf, "MultidimensionalSimRankMR Iteration"+String.valueOf(iteration-i+1)); 
			
			job3.setJarByClass(MultidimensionalSimRankMR.class);

			//Set Mapper,Reducer class
			job3.setMapperClass(OtherIterationMapper.class);
			job3.setReducerClass(OtherIterationReducer.class);

			//Set Input Format - read .bz2 and decompress automatically
			//job3.setInputFormatClass(TextInputFormat.class);

			//Set input,output path
			if(i < iteration && i>1){//Middle Iteration
				FileInputFormat.addInputPath(job3, new Path(args[2]+"_Iteration"+String.valueOf(iteration-i)));
				FileOutputFormat.setOutputPath(job3, new Path(args[2]+"_Iteration"+String.valueOf(iteration-i+1)));
			}
			else{//Last Iteration
				FileInputFormat.addInputPath(job3, new Path(args[2]+"_Iteration"+String.valueOf(iteration-i)));
				FileOutputFormat.setOutputPath(job3, new Path(args[2]));
			}
			
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);

			isSuccessfully=isSuccessfully & job3.waitForCompletion(true);
		}	
		return  isSuccessfully ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new MultidimensionalSimRankMR(), args);//Run the class MultidimensionalSimRankMR() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}
}
