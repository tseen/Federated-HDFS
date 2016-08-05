package fbicloud.botrank;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * This class will responsible for taking the input from a file and
 * running the iterative Map-Reduce functionality to perform Dijikstra Algorithm
 *
 */
public class BFS extends Configured implements Tool {


	public static class BMap extends 
			Mapper<LongWritable, Text, LongWritable, Text> {

		/*
		 * Overriding the map function
		 */
		@Override
		public void map(LongWritable key, Text value,
				Context output)
				throws IOException, NumberFormatException, InterruptedException {
			Text word = new Text();
			String line = value.toString(); // looks like 1 0 2:3:
			String[] sp = line.split("\\s+"); // splits on space
			int distanceadd = Integer.parseInt(sp[1]) + 1;
			String[] PointsTo = sp[2].split(":");
			for (int i = 0; i < PointsTo.length; i++) {
				word.set("VALUE " + distanceadd); // tells me to look at
													// distance value
				output.write(new LongWritable(Long.parseLong(PointsTo[i])),
						word);
				word.clear();
			}

			// pass in current node's distance (if it is the lowest distance)
			word.set("VALUE " + sp[1]);
			output.write(new LongWritable(Long.parseLong(sp[0])), word);
			word.clear();

			word.set("NODES " + sp[2]);
			output.write(new LongWritable(Long.parseLong(sp[0])), word);
			word.clear();
		}
	}

	public static class BReduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		/*
		 * Overriding the reduce function
		 */
		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context output)
				throws IOException, InterruptedException {
			String nodes = "UNMODED";
			Text word = new Text();
			int lowest = 125; // In this 125 is considered as infinite distance
			for(Text value : values) { // looks like NODES/VALUES 1 0 2:3:, we
										// need to use the first as a key
				String[] sp = value.toString().split("\\s+"); // splits on
																	// space
				// look at first value
				if (sp[0].equalsIgnoreCase("NODES")) {
					nodes = null;
					nodes = sp[1];
				} else if (sp[0].equalsIgnoreCase("VALUE")) {
					int distance = Integer.parseInt(sp[1]);
					lowest = Math.min(distance, lowest);
				}
			}
			word.set(lowest + "\t" + nodes);
			output.write(key, word);
			word.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run ( new Configuration(), new BFS(), args ) ;
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}

	public int  run(String[] args) throws Exception {
		

		// Reiteration again and again till the convergence
			Configuration conf = this.getConf() ;
			
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
			
			Job job2 = Job.getInstance ( conf, "BFS" ) ;
			job2.setJarByClass ( BFS.class ) ;
			
			job2.setMapOutputKeyClass ( LongWritable.class ) ;
			job2.setMapOutputValueClass ( Text.class ) ;
			

			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setMapperClass(BMap.class);
			job2.setReducerClass(BReduce.class);
			

			FileInputFormat.addInputPath ( job2, new Path ( args[0] )) ;
			FileOutputFormat.setOutputPath ( job2, new Path ( args[1] ) ) ;
			
			return job2.waitForCompletion(true) ? 0 : 1 ;
			/*input = output + "/part-00000";
			isdone = true;// set the job to NOT run again!
			Path ofile = new Path(input);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));
			HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
			String line = br.readLine();
			// Read the current output file and put it into HashMap
			while (line != null) {
				String[] sp = line.split("\t| ");
				int node = Integer.parseInt(sp[0]);
				int distance = Integer.parseInt(sp[1]);
				imap.put(node, distance);
				line = br.readLine();
			}
			br.close();

			// Check for convergence condition if any node is still left then
			// continue else stop
			Iterator<Integer> itr = imap.keySet().iterator();
			while (itr.hasNext()) {
				int key = itr.next();
				int value = imap.get(key);
				if (value >= 125) {
					isdone = false;
				}
			}
			input = output;
			output = OUT + System.nanoTime();
			*/
		
	}
}
