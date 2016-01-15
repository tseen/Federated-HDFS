package fbicloud.algorithm.botnetDetectionFS;

import java.io.*;
import java.util.*;

import fbicloud.algorithm.classes.FVInfo;

import org.apache.hadoop.conf.Configuration;                                                                                                                                                           
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FVGraphMR_srcIPoverlap extends Configured implements Tool{
	
	public static class job1Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
		private ArrayList<FVInfo> allFVGroup = new ArrayList<FVInfo>();
		private String line;
		private Text interKey = new Text();
		private DoubleWritable interValue = new DoubleWritable();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			//Read from PreMultidimensionalSimRankMR.java output
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(config.get("FVInfo")));
			for (int i=0;i<status.length;i++){
				//Open all files under the specific folder
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//Input Format		
					//	FVID	protocol,Gx							Feature Vector										srcIPs
					//	30      17,G18  1.75,77.29,44.0,44.0,44.0,0.0,1.37,1393.28,1017.26,1017.26,1017.26,0.0,3.13,1470.58,1017.26,44.0,475.17,475.75,0.0,0.23,0.83,19.27,9436.48      140.116.6.5,140.116.6.3,140.116.6.4,140.116.6.1,140.116.6.2,
					String[] tmp = line.split("\t");
					String[] srcIPsArray = tmp[3].split(",");
					Collection<String> srcIPsCol = new ArrayList<String>();
					for(int j=0;j<srcIPsArray.length;j++){
						srcIPsCol.add(srcIPsArray[j]);
					}
					allFVGroup.add(new FVInfo(Integer.parseInt(tmp[0]),srcIPsCol));
					line=br.readLine();
				}
			}
		}
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input Format		
			//	FVID	protocol,Gx							Feature Vector										srcIPs
			//	30      17,G18  1.75,77.29,44.0,44.0,44.0,0.0,1.37,1393.28,1017.26,1017.26,1017.26,0.0,3.13,1470.58,1017.26,44.0,475.17,475.75,0.0,0.23,0.83,19.27,9436.48      140.116.6.5,140.116.6.3,140.116.6.4,140.116.6.1,140.116.6.2,
			int i;
			int currentID;
			int NoAllIP;
			Collection<String> currentSrcIPsCol = new ArrayList<String>();

			String[] tmp = value.toString().split("\t");
			
			currentID = Integer.parseInt(tmp[0]);
			
			String[] srcIPsArray = tmp[3].split(",");
			for(i=0;i<srcIPsArray.length;i++){
				currentSrcIPsCol.add(srcIPsArray[i]);
			}

			for(i=0;i<allFVGroup.size();i++){
				if(currentID < allFVGroup.get(i).getID()){
					//Collection<String> tmpSrcIPsCol = allFVGroup.get(i).getSrcIPsCol();
					Collection<String> tmpSrcIPsCol = new ArrayList<String>(allFVGroup.get(i).getSrcIPsCol());//Copy Collection

					NoAllIP = currentSrcIPsCol.size() + tmpSrcIPsCol.size();
					tmpSrcIPsCol.retainAll(currentSrcIPsCol);//Get intersection
					if(tmpSrcIPsCol.size()!=0){//There are intersection
						/*--------------------------------------------------*
						 *	Key		FVID1,FVID2								*
						 *	Value	2*(# of overlap IP) / (# of all IPs)	*
						 *--------------------------------------------------*
						 *	Example : 										*
						 *	FVID1 : A,B,C,D		/		FVID2 : C,D,M,N		*
						 *	-> Initial score = 2*(2) / (4+4) = 1/2			*
						 *--------------------------------------------------*/
						interKey.set(String.valueOf(currentID) + "," + String.valueOf(allFVGroup.get(i).getID()));
						interValue.set((double)2*tmpSrcIPsCol.size()/NoAllIP);
						context.write(interKey,interValue);
					}
					tmpSrcIPsCol.clear();
				}
			}
		}
	}

	public static class job2Mapper extends Mapper <LongWritable, Text, Text , Text> {
		private Text interKey = new Text();
		private Text interValue = new Text();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input Format :
			//		FVID1,FVID2	score
			//Input Example:
			//		518,619		0.5

			String line = value.toString();
			String[] str = line.split("\t");
			String[] tmp = str[0].split(",");

			if(str.length == 2){
				//If FVID1, FVID2 <FVID1, FVID2>
				//Key	protocol
				//Value	FeatureVector	sIP	dstIPs
				interKey.set(tmp[0]);
				interValue.set(tmp[1]);
				context.write(interKey, interValue);

				interKey.set(tmp[1]);
				interValue.set(tmp[0]);
				context.write(interKey, interValue);
			}
		}
	}
	public static class job2Reducer extends Reducer<Text, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			//Input Format :
			//		Key		: 	FVID1
			//		Value	:	FVID2
			Set<String> FVSet = new HashSet<>();
			StringBuffer outsb = new StringBuffer();

			for(Text val : values)
				FVSet.add(val.toString());
			for(String FV : FVSet)
				outsb.append(FV + ",");

			outputKey.set(key);
			outputValue.set(outsb.toString());
			context.write(outputKey,outputValue);
		}
	}

	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2 ) {
			System.out.println("FVGraphMR_srcIPoverlap: <in> <out>");
			System.exit(2);
		}

		/*------------------------------------------------------------------*
		 *								Job 1		 						*
		 *	Calculate Initial Score of two FVIDs							*
		 *------------------------------------------------------------------*/

		conf.set("FVInfo",args[0]);
		Job job1 = Job.getInstance(conf, "FVGraphMR_srcIPoverlap_job1 : Calculate Initial Score of two FVIDs");  
		job1.setJarByClass(FVGraphMR_srcIPoverlap.class);

		job1.setMapperClass(job1Mapper.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_job1_initialScore"));

		job1.waitForCompletion(true);

		/*------------------------------------------------------------------*
		 *								Job 2		 						*
		 *	Get Adjacency list												*
		 *------------------------------------------------------------------*/
		
		Job job2 = Job.getInstance(conf, "FVGraphMR_srcIPoverlap_job2 :  Group with prot,sIP,dIP and calculate time features ");  
		job2.setJarByClass(FVGraphMR_srcIPoverlap.class);

		job2.setMapperClass(job2Mapper.class);
		job2.setReducerClass(job2Reducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]+"_job1_initialScore"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		return job2.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new FVGraphMR_srcIPoverlap(), args);//Run the class FVGraphMR_srcIPoverlap() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}
}
