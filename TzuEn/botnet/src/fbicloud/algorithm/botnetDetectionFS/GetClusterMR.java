package fbicloud.algorithm.botnetDetectionFS;

import java.io.IOException;
import java.util.*;

import fbicloud.algorithm.classes.BotnetGroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class GetClusterMR extends Configured implements Tool{
	public static class GetClusterMapper extends Mapper <LongWritable, Text, Text, Text> {
		private Text interKey = new Text();
		private Text interValue = new Text();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//	Input Format
			//		140.116.1.138,140.116.103.202   0.03
			String[] line = value.toString().split("\t");
			String[] tmp = line[0].split(",");
			interKey.set("key");
			interValue.set(tmp[0] + "\t" + tmp[1] + "\t" + line[1]);
			// string	140.116.1.138	140.116.103.202
			context.write(interKey,interValue);
		}
	}
	
	public static class GetClusterReducer extends Reducer<Text, Text, Text, Text> {
		private float closeScore;
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			closeScore = Float.parseFloat(config.get("closeScore"));
		}
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			List<BotnetGroup> list = new ArrayList<BotnetGroup>();
			//List<String> IPlist = new ArrayList<String>();
			HashSet<Integer> IPhit = new HashSet<Integer>();
			List<Integer> sortList = new ArrayList<Integer>();
			StringBuffer outsb = new StringBuffer();

			HashSet<String> mergeIPs = new HashSet<String>();
			double mergeScore=0;
			double mergeIPPairNo=0;

			for(Text val : values){

				String [] line = val.toString().split("\t");// line[0] -> IP1 ,line[1] -> IP2
				if(!line[0].equals(line[1])){
					//Initialize
					IPhit.clear();

					//Check similarity score with same IP
					for(int i=0;i<list.size();i++){
						if(Math.abs( list.get(i).getScore() - Double.parseDouble(line[2]) ) < closeScore){//Score are close
							if(list.get(i).getIPs().contains(line[0])){
								IPhit.add(i);
							}
							if(list.get(i).getIPs().contains(line[1])){
								IPhit.add(i);
							}
						}
					}

					if(IPhit.size()>1){//Merge two or more Botnet Group
						//Initialize 
						sortList.clear();
						mergeIPs.clear();
						mergeScore = 0;
						mergeIPPairNo = 0;
						
						for (Integer index: IPhit) {
							sortList.add(index);
							
							for (String IP : list.get(index).getIPs()) {
								mergeIPs.add(IP);
							}
							mergeIPPairNo += list.get(index).getIPPairNo();
							mergeScore += list.get(index).getIPPairNo() * list.get(index).getScore();
						}
						mergeIPs.add(line[0]);
						mergeIPs.add(line[1]);
						mergeScore += Double.parseDouble(line[2]);
						mergeScore = mergeScore/(mergeIPPairNo+1);
						list.add( new BotnetGroup(mergeIPs,mergeScore));

						//Remove premerge botnet group
						Collections.sort(sortList, new MyIntComparable());//Sort from large -> small
						for(Integer integer : sortList){
							list.remove(integer.intValue());//convet integer to int by intValue 
						}
					}
					else if(IPhit.size()==1){//Expand One Botnet Group
						for (Integer index : IPhit) {
							list.get(index).update(line[0], line[1],Double.parseDouble(line[2]));
						}
					}
					else{//IPhit is empty -> Add new Botnet Group
						list.add( new BotnetGroup(line[0], line[1],Double.parseDouble(line[2])));
					}
				}
				/*
				else{
					IPlist.add(line[0]);
				}*/
			}

			outputKey.set("Totoal Groups of P2P Botnet = ");
			outputValue.set(String.valueOf(list.size()));
			context.write(outputKey,outputValue);
			for(int i=0;i<list.size();i++){
				outsb.delete(0, outsb.length());//Reset
				for(String IP : list.get(i).getIPs()){
					outsb.append(IP + ",");
				}
				outputKey.set("Botnet member : "+ list.get(i).getIPs().size());
				outputValue.set(outsb.toString());
				context.write(outputKey, outputValue);
			}

			/*
			outputKey.set("-------------------------------------------------");
			outputValue.set("");
			context.write(outputKey, outputValue);

			tmpStr="";
			for(int i=0;i<IPlist.size();i++){
				tmpStr =tmpStr + IPlist.get(i) + ",";
			}
			outputKey.set("The IPs are similar itself : ");
			outputValue.set(tmpStr);
			context.write(outputKey, outputValue);
			*/
		}
		class MyIntComparable implements Comparator<Integer>{
			@Override
			public int compare(Integer o1, Integer o2) {
				return (o1>o2 ? -1 : (o1==o2 ? 0 : 1));
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2 ) {
			System.out.println("GetClusterMR: <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "GetClusterMR"); 
		job.setJarByClass(GetClusterMR.class);
		
		job.setMapperClass(GetClusterMapper.class);
		job.setReducerClass(GetClusterReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new GetClusterMR(), args);//Run the class GetClusterMR() after parsing with the given generic arguments
		//res == 0 -> normal exit
		//res != 0 -> Something error
		System.exit(res);
	}

}
