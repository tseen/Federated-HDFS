package fbicloud.botrank;

import fbicloud.algorithm.botnetDetectionFS.GroupPhase2MR.job4Mapper;
import fbicloud.algorithm.botnetDetectionFS.GroupPhase2MR.job4Reducer;

import fbicloud.algorithm.classes.SimilarityFunction;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GroupPhase2MR_4 extends Configured implements Tool {
	public static class GetAverageFVOfLevelThreeClusterMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		// input value reader
		private String line;
		private String[] subLine;

		// length of line
		private final static int lengthOfLine = 5;

		// map intermediate key
		private Text interKey = new Text();
		private Text interValue = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// input format
			// key :
			// LongWritable
			// value : index
			// protocol,G# timestamp srcIP dstIPs 20 features 0~4

			line = value.toString();
			subLine = line.split("\t");

			String protocol_gid = subLine[0];
			String srcIP = subLine[2];
			String dstIPs = subLine[3];
			String features = subLine[4];

			if (subLine.length == lengthOfLine) {
				interKey.set(protocol_gid); // protocol,G#
				interValue.set(srcIP + "\t" + dstIPs + "\t" + features);
				context.write(interKey, interValue);
			}
		}
	}

	public static class GetAverageFVOfLevelThreeClusterReducer extends
			Reducer<Text, Text, Text, Text> {
		// feature vectors variable
		private double srcToDst_NumOfPkts;
		private double srcToDst_NumOfBytes;
		private double srcToDst_Byte_Max;
		private double srcToDst_Byte_Min;
		private double srcToDst_Byte_Mean;

		private double dstToSrc_NumOfPkts;
		private double dstToSrc_NumOfBytes;
		private double dstToSrc_Byte_Max;
		private double dstToSrc_Byte_Min;
		private double dstToSrc_Byte_Mean;

		private double total_NumOfPkts;
		private double total_NumOfBytes;
		private double total_Byte_Max;
		private double total_Byte_Min;
		private double total_Byte_Mean;
		private double total_Byte_STD;

		private double total_PktsRate;
		private double total_BytesRate;
		private double total_BytesTransferRatio;
		private double duration;

		// input value reader
		private String[] line;
		private String[] feature;
		// counter
		private int count;

		// a HashSet to store srcIPs
		private Set<String> srcIPSet = new TreeSet<String>();
		// a HashSet to store dstIPs
		private Set<String> dstIPSet = new TreeSet<String>();

		// reduce output value
		private Text outputValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// input format
			// key :
			// protocol,G#
			// value : index
			// srcIP dstIPs 20 features 0~2

			// initialize
			srcToDst_NumOfPkts = 0d;
			srcToDst_NumOfBytes = 0d;
			srcToDst_Byte_Max = 0d;
			srcToDst_Byte_Min = 0d;
			srcToDst_Byte_Mean = 0d;

			dstToSrc_NumOfPkts = 0d;
			dstToSrc_NumOfBytes = 0d;
			dstToSrc_Byte_Max = 0d;
			dstToSrc_Byte_Min = 0d;
			dstToSrc_Byte_Mean = 0d;

			total_NumOfPkts = 0d;
			total_NumOfBytes = 0d;
			total_Byte_Max = 0d;
			total_Byte_Min = 0d;
			total_Byte_Mean = 0d;
			total_Byte_STD = 0d;

			total_PktsRate = 0d;
			total_BytesRate = 0d;
			total_BytesTransferRatio = 0d;
			duration = 0d;

			count = 0;

			srcIPSet.clear();
			dstIPSet.clear();
			// string buffer to store srcIPs and dstIPs
			StringBuffer srcIPStr = new StringBuffer();
			StringBuffer dstIPStr = new StringBuffer();

			for (Text val : values) {
				count++;
				line = val.toString().split("\t");

				feature = line[2].split(",");

				srcToDst_NumOfPkts += Double.parseDouble(feature[0]);
				srcToDst_NumOfBytes += Double.parseDouble(feature[1]);
				srcToDst_Byte_Max += Double.parseDouble(feature[2]);
				srcToDst_Byte_Min += Double.parseDouble(feature[3]);
				srcToDst_Byte_Mean += Double.parseDouble(feature[4]);

				dstToSrc_NumOfPkts += Double.parseDouble(feature[5]);
				dstToSrc_NumOfBytes += Double.parseDouble(feature[6]);
				dstToSrc_Byte_Max += Double.parseDouble(feature[7]);
				dstToSrc_Byte_Min += Double.parseDouble(feature[8]);
				dstToSrc_Byte_Mean += Double.parseDouble(feature[9]);

				total_NumOfPkts += Double.parseDouble(feature[10]);
				total_NumOfBytes += Double.parseDouble(feature[11]);
				total_Byte_Max += Double.parseDouble(feature[12]);
				total_Byte_Min += Double.parseDouble(feature[13]);
				total_Byte_Mean += Double.parseDouble(feature[14]);
				total_Byte_STD += Double.parseDouble(feature[15]);

				total_PktsRate += Double.parseDouble(feature[16]);
				total_BytesRate += Double.parseDouble(feature[17]);
				total_BytesTransferRatio += Double.parseDouble(feature[18]);
				duration += Double.parseDouble(feature[19]);

				// add srcIP into hashset
				srcIPSet.add(line[0]);

				// add dstIPs into hashset
				String[] dstIParray = line[1].split(",");
				for (int i = 0; i < dstIParray.length; i++) {
					dstIPSet.add(dstIParray[i]);
				}
			}

			// Get average of each features
			srcToDst_NumOfPkts = round(srcToDst_NumOfPkts / count, 5);
			srcToDst_NumOfBytes = round(srcToDst_NumOfBytes / count, 5);
			srcToDst_Byte_Max = round(srcToDst_Byte_Max / count, 5);
			srcToDst_Byte_Min = round(srcToDst_Byte_Min / count, 5);
			srcToDst_Byte_Mean = round(srcToDst_Byte_Mean / count, 5);

			dstToSrc_NumOfPkts = round(dstToSrc_NumOfPkts / count, 5);
			dstToSrc_NumOfBytes = round(dstToSrc_NumOfBytes / count, 5);
			dstToSrc_Byte_Max = round(dstToSrc_Byte_Max / count, 5);
			dstToSrc_Byte_Min = round(dstToSrc_Byte_Min / count, 5);
			dstToSrc_Byte_Mean = round(dstToSrc_Byte_Mean / count, 5);

			total_NumOfPkts = round(total_NumOfPkts / count, 5);
			total_NumOfBytes = round(total_NumOfBytes / count, 5);
			total_Byte_Max = round(total_Byte_Max / count, 5);
			total_Byte_Min = round(total_Byte_Min / count, 5);
			total_Byte_Mean = round(total_Byte_Mean / count, 5);
			total_Byte_STD = round(total_Byte_STD / count, 5);

			total_PktsRate = round(total_PktsRate / count, 5);
			total_BytesRate = round(total_BytesRate / count, 5);
			total_BytesTransferRatio = round(total_BytesTransferRatio / count,
					5);
			duration = round(duration / count, 5);

			// append srcIP to the String buffer
			for (String sIP : srcIPSet) {
				srcIPStr.append(sIP + ",");
			}
			// append dstIP to the String buffer
			for (String dIP : dstIPSet) {
				dstIPStr.append(dIP + ",");
			}

			outputValue.set(srcIPStr.toString() + "\t" + dstIPStr.toString()
					+ "\t" + srcToDst_NumOfPkts + "," + srcToDst_NumOfBytes
					+ "," + srcToDst_Byte_Max + "," + srcToDst_Byte_Min + ","
					+ srcToDst_Byte_Mean + "," + dstToSrc_NumOfPkts + ","
					+ dstToSrc_NumOfBytes + "," + dstToSrc_Byte_Max + ","
					+ dstToSrc_Byte_Min + "," + dstToSrc_Byte_Mean + ","
					+ total_NumOfPkts + "," + total_NumOfBytes + ","
					+ total_Byte_Max + "," + total_Byte_Min + ","
					+ total_Byte_Mean + "," + total_Byte_STD + ","
					+ total_PktsRate + "," + total_BytesRate + ","
					+ total_BytesTransferRatio + "," + duration);
			context.write(key, outputValue);
			// test
			// System.out.println( "key = " + key.toString() );
			// System.out.println( "srcIPStr.toString() = " +
			// srcIPStr.toString() );
			// System.out.println( "dstIPStr.toString() = " +
			// dstIPStr.toString() );
		}

		public double round(double value, int places) {
			if (places < 0)
				throw new IllegalArgumentException();

			BigDecimal bd = new BigDecimal(value);
			bd = bd.setScale(places, RoundingMode.HALF_UP);
			return bd.doubleValue();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		/*------------------------------------------------------------------*
		 *								Job 4		 						*
		 *	Get average features of same group								*
		 *------------------------------------------------------------------*/
		Job job4 = Job.getInstance(conf, "Group 2 - job4");
		job4.setJarByClass(GroupPhase2MR_4.class);

		job4.setMapperClass(GetAverageFVOfLevelThreeClusterMapper.class);
		job4.setReducerClass(GetAverageFVOfLevelThreeClusterReducer.class);

		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job4, new Path(args[0]));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]
				+ "_GetAverageFVOfLevelThreeCluster"));

		return job4.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new GroupPhase2MR_4(),
				args);
		// Run the class after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res);
	}
}