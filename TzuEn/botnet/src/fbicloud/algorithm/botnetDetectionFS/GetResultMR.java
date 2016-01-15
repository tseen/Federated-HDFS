package fbicloud.algorithm.botnetDetectionFS;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
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

public class GetResultMR extends Configured implements Tool{
	public static class GetResultMapper extends Mapper <LongWritable, Text, Text, Text> {
		private double similarityThreshold;
		private Text interKey = new Text();
		private Text interValue = new Text();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			similarityThreshold = Float.parseFloat(config.get("similarity"));
		}
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//Input Format		FVID1,FVID2		score				Relative FVID
			//					1001,1412       0.04788     524,977;524,1399;977,994;994,1399;
			double score=0;
			String[] tmp = value.toString().split("\t");
			String[] FVID = tmp[0].split(",");
			if(FVID[0]!=FVID[1]){
				score = Double.parseDouble(tmp[1]);
				if(score >= similarityThreshold ){
					interKey.set("Key");
					interValue.set(FVID[0]+"\t"+FVID[1]);//FVID1,FVID2     score
					context.write(interKey,interValue);
				}
			}
		}
	}
	
	public static class GetResultReducer extends Reducer<Text, Text, Text, Text> {
		private HashMap<String,String> FVIDIPMapping  = new HashMap<>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			//Read from PreMultidimensionalSimRankMR.java output
			FileSystem fs = FileSystem.get(config);
			FileStatus[] status = fs.listStatus(new Path(config.get("FVIDIPMapping")));
			String[] tmp = new String[2];
			String line;
			for (int i=0;i<status.length;i++){
				//Open all files under the specific folder
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//Input Format		FVID			srcIPs
					//					1680    140.116.121.170,140.116.1.136,140.116.26.49,140.116.122.209,140.116.200.100,
					tmp = line.split("\t");
					FVIDIPMapping.put(tmp[0],tmp[1]);
					line=br.readLine();
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			Set<String> srcIPs = new HashSet<String>();
			List<HashSet<String>> list = new ArrayList<HashSet<String>>();
			boolean firsttime=true;
			int FVID1hit=-1,FVID2hit=-1;//if FVID1hit != -1 -> line[0] is in specific hashset
			StringBuffer outsb = new StringBuffer();

			for(Text val : values){
				String [] line = val.toString().split("\t");// line[0] -> IP1 ,line[1] -> IP2
				if(!line[0].equals(line[1])){
					if(firsttime){
						list.add( new HashSet<String>(Arrays.asList(line[0], line[1])));
						firsttime=false;
					}
					else{
						FVID1hit=-1;
						FVID2hit=-1;
						for(int i=0;i<list.size();i++){	
							if(list.get(i).contains(line[0])){
								FVID1hit=i;
							}
							if(list.get(i).contains(line[1])){
								FVID2hit=i;
							}
						}

						if(FVID1hit!=-1 && FVID2hit!=-1){
							if(FVID1hit!=FVID2hit){
								HashSet<String> tmpHashSet = new HashSet<>();
								tmpHashSet.addAll(list.get(FVID1hit));
								tmpHashSet.addAll(list.get(FVID2hit));
								list.add(tmpHashSet);
								if(FVID1hit<FVID2hit){
									list.remove(FVID2hit);
									list.remove(FVID1hit);
								}
								else{
									list.remove(FVID1hit);
									list.remove(FVID2hit);
								}
							}
						}
						else if(FVID1hit==-1 && FVID2hit!=-1){
							list.get(FVID2hit).add(line[0]);
							list.get(FVID2hit).add(line[1]);
						}
						else if(FVID1hit!=-1 && FVID2hit==-1){
							list.get(FVID1hit).add(line[0]);
							list.get(FVID1hit).add(line[1]);
						}
						else if(FVID1hit==-1 && FVID2hit==-1){
							list.add( new HashSet<String>(Arrays.asList(line[0], line[1])));
						}
						else{
						}
					}
				}
			}

			outputKey.set("Totoal Groups of P2P Botnet = ");
			outputValue.set(String.valueOf(list.size()));
			context.write(outputKey,outputValue);

			for(int i=0;i<list.size();i++){
				
				srcIPs.clear();//Clean hashset
				outsb.delete(0, outsb.length());//Reset

				for(String FVID : list.get(i)){
					String[] tmpArray = FVIDIPMapping.get(FVID).split(",");

					for(int j=0;j<tmpArray.length;j++){
						srcIPs.add(tmpArray[j]);//add SrcIP
					}
				}
				for(String SIP : srcIPs){
					outsb.append(SIP + ",");
				}
				outputKey.set("Botnet member : "+ srcIPs.size());
				outputValue.set(outsb.toString());
				context.write(outputKey, outputValue);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("FVIDIPMapping",args[0]);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 3 ) {
			System.out.println("GetResultMR: <FVIDIPMappingList> <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Result"); 
		job.setJarByClass(GetResultMR.class);
		
		job.setMapperClass(GetResultMapper.class);
		job.setReducerClass(GetResultReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new GetResultMR(), args);//Run the class GetResultMR() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}

}
