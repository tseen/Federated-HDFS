package fbicloud.botrank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class pagerank extends Configured implements Tool{


	public static class pgmapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException
		{		
			String[] input = value.toString().split("\t");
			String k = input[0];
			String v = input[1];
			String rank = input[2];
			String link[] = v.split(",");
			float score = Float.valueOf(rank)/(link.length)*1.0f;
			context.write(new Text(k), new Text(v));
			for( int i = 0; i < link.length; i++)
			{
				context.write(new Text(link[i]), new Text(k+"\t"+score));
			}
		}
	}
	
	public static class pgreducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			float factor = 0.85f;
			float rank = 0f;
			String links = "";
			for(Text f : values)
			{	
				//System.out.println("K:"+key.toString()+" __V:"+f.toString());
				String value = f.toString();
				int lenth = value.split("\t").length;
			/*	if(f.getInt() == 0){
					links = f.getString();
					System.out.println("links:"+links);
				}
				else{
					rank += f.getFloat();
					System.out.println("rank:"+f.getFloat());
				}
				*/
				if(lenth ==2 ){
					String inLink = value.split("\t")[0];
					String rankN = value.split("\t")[1];
					rank += Float.parseFloat(rankN) * 0.85f;
				}
				else{
					links = value;
				}
				//Thread.sleep(5);
				
			}
			//rank = ((1-factor)+(factor*rank));
			String	output = links+"\t"+rank;
			context.write(new Text(key.toString()), new Text(output));
		}
	}
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;

		Job job = Job.getInstance(conf, "PageRank");
		job.setJarByClass(pagerank.class);
		job.setJobName("PageRank");

		job.setMapperClass(pgmapper.class);
		job.setReducerClass(pgreducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new pagerank(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit (res) ;
	}

					
}


