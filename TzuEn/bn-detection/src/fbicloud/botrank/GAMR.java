package fbicloud.botrank;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import ncku.hpds.fed.MRv2.proxy.GenericProxyMapper;
import ncku.hpds.fed.MRv2.proxy.GenericProxyMapperSeq;
import ncku.hpds.fed.MRv2.proxy.GenericProxyReducer;
import ncku.hpds.fed.MRv2.proxy.GenericProxyReducerSeq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class GAMR extends Configured implements Tool {
	public static final int LONG_BITS = 64;
	public static int LONGS_PER_ARRAY = 6000;
	public static int POPULATION = 12000;

	public static class GAMapper
			extends
			Mapper<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {
		long max = -1;

		LongArrayWritable maxInd;

		private String mapTaskId = "";

		long fit = 0;

		Configuration conf;
		int pop = POPULATION;

		@Override
		public void setup(Context ctx) {
			conf = ctx.getConfiguration();
			mapTaskId = conf.get("mapred.task.id");
			// pop = Integer.parseInt(conf.get("ga.populationPerMapper"));
		}

		long fitness(LongWritable[] individual) {
			long f = 0;
			for (int i = 0; i < individual.length; i++) {
				long mask = 1;
				for (int j = 0; j < LONG_BITS; j++) {
					f += ((individual[i].get() & mask) > 0) ? 1 : 0;
					mask = mask << 1;
				}
			}
			// System.err.println("Fitness of " + individual + " is " + f);
			return f;
		}

		int processedInd = 0;

		public void map(LongArrayWritable key, LongWritable value,
				Context context)
		// OutputCollector<LongArrayWritable, LongWritable> oc, Reporter rep)
				throws IOException {
			// Compute the fitness for every individual
			LongWritable[] individual = key.getArray();
			fit = fitness(individual);
			// System.err.println(value + " : " + individual + " : " + fit);

			// Keep track of the maximum fitness
			if (fit > max) {
				max = fit;
				maxInd = new LongArrayWritable(individual);
			}
			try {
				context.write(key, new LongWritable(fit));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			processedInd++;
			if (processedInd == pop - 1) {
				closeAndWrite();
			}
		}

		public void closeAndWrite() throws IOException {
			// At the end of Map(), write the best found individual to a file
			Path tmpDir = new Path("/user/hpds/" + "GA");
			Path outDir = new Path(tmpDir, "global-map");

			// HDFS does not allow multiple mappers to write to the same file,
			// hence create one for each mapper
			Path outFile = new Path(outDir, mapTaskId);
			FileSystem fileSys = FileSystem.get(conf);
			SequenceFile.Writer writer = SequenceFile.createWriter(fileSys,
					conf, outFile, LongArrayWritable.class, LongWritable.class,
					CompressionType.NONE);

			// System.err.println("Max ind = " + maxInd.toString() + " : " +
			// max);
			writer.append(maxInd, new LongWritable(max));
			writer.close();
		}

	}

	public static class GAReducer
			extends
			Reducer<LongArrayWritable, LongWritable, LongArrayWritable, LongWritable> {

		int tournamentSize = 5;

		int LONGS_PER_ARRAY;

		LongWritable[][] tournamentInd;

		long[] tournamentFitness = new long[2 * tournamentSize];

		int processedIndividuals = 0;

		int r = 0;

		LongArrayWritable[] ind = new LongArrayWritable[2];

		Random rng;
		Context _context;
		int pop = 1;

		GAReducer() {
			rng = new Random(System.nanoTime());
		}

		@Override
		public void setup(Context ctx) {
			Configuration jc = ctx.getConfiguration();
			tournamentInd = new LongWritable[2 * tournamentSize][LONGS_PER_ARRAY];
			pop = POPULATION;
			_context = ctx;
		}

		void crossover() {
			// Perform uniform crossover
			LongWritable[] ind1 = ind[0].getArray();
			LongWritable[] ind2 = ind[1].getArray();
			LongWritable[] newInd1 = new LongWritable[LONGS_PER_ARRAY];
			LongWritable[] newInd2 = new LongWritable[LONGS_PER_ARRAY];
			// System.err.print("[GA] Crossing over " + ind[0] + " + " +
			// ind[1]);

			for (int i = 0; i < LONGS_PER_ARRAY; i++) {
				long i1 = 0, i2 = 0, mask = 1;
				for (int j = 0; j < LONG_BITS; j++) {
					if (rng.nextDouble() > 0.5) {
						i2 |= ind2[i].get() & mask;
						i1 |= ind1[i].get() & mask;
					} else {
						i1 |= ind2[i].get() & mask;
						i2 |= ind1[i].get() & mask;
					}
					mask = mask << 1;
				}
				newInd1[i] = new LongWritable(i1);
				newInd2[i] = new LongWritable(i2);
			}

			ind[0] = new LongArrayWritable(newInd1);
			ind[1] = new LongArrayWritable(newInd2);
			// System.err.println("[GA] Got " + ind[0] + " + " + ind[1]);
		}

		LongWritable[] tournament(int startIndex) {
			// Tournament selection without replacement
			LongWritable[] tournamentWinner = null;
			long tournamentMaxFitness = -1;
			for (int j = 0; j < tournamentSize; j++) {
				if (tournamentFitness[j] > tournamentMaxFitness) {
					tournamentMaxFitness = tournamentFitness[j];
					tournamentWinner = tournamentInd[j];
				}
			}
			return tournamentWinner;
		}

		public void reduce(LongArrayWritable key,
				Iterator<LongWritable> values, Context context)
		// OutputCollector<LongArrayWritable, LongWritable> output, Reporter
				// rep)
				throws IOException {

			while (values.hasNext()) {
				long fitness = values.next().get();
				tournamentInd[processedIndividuals % tournamentSize] = key
						.getArray();
				tournamentFitness[processedIndividuals % tournamentSize] = fitness;

				if (processedIndividuals < tournamentSize) {
					// Wait for individuals to join in the tournament and put
					// them for the last round
					tournamentInd[processedIndividuals % tournamentSize
							+ tournamentSize] = key.getArray();
					tournamentFitness[processedIndividuals % tournamentSize
							+ tournamentSize] = fitness;
				} else {
					// Conduct a tournament over the past window
					ind[processedIndividuals % 2] = new LongArrayWritable(
							tournament(processedIndividuals));

					if ((processedIndividuals - tournamentSize) % 2 == 1) {
						// Do crossover every odd iteration between successive
						// individuals
						crossover();
						try {
							context.write(ind[0], new LongWritable(0));
							context.write(ind[1], new LongWritable(0));
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				processedIndividuals++;
				// System.err.println(" " + processedIndividuals);
			}
			if (processedIndividuals == pop - 1) {
				closeAndWrite();
			}
		}

		public void closeAndWrite() {
			System.out.println("Closing reducer");
			// Cleanup for the last window of tournament
			for (int k = 0; k < tournamentSize; k++) {
				// Conduct a tournament over the past window
				ind[processedIndividuals % 2] = new LongArrayWritable(
						tournament(processedIndividuals));

				if ((processedIndividuals - tournamentSize) % 2 == 1) {
					// Do crossover every odd iteration between successive
					// individuals
					crossover();
					try {
						_context.write(ind[0], new LongWritable(0));
						_context.write(ind[1], new LongWritable(0));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				processedIndividuals++;
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs() ;
		FileSystem fileSys = null;
		try {
			fileSys = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		fileSys.delete(new Path("/user/hpds/GA", "global-map"), true);

		Job job = Job.getInstance(conf, "GAMR");
		job.setJarByClass(GAMR.class);
		job.setJobName("GAMR");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(LongArrayWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(GAMapper.class);
		job.setReducerClass(GAReducer.class);
		
		job.setGroupingComparatorClass(LongArrayWritableComparator.class);
		job.setSortComparatorClass(LongArrayWritableComparator.class);
		
		job.setKeyValueReduceClass(LongArrayWritable.class, LongWritable.class, userDefineReducer.class);
		job.setKeyValueMapClass(LongArrayWritable.class, LongWritable.class, userDefineMapper.class);
		
		
		job.setPartitionerClass(IndividualPartitioner.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static class userDefineMapper extends GenericProxyMapperSeq <LongArrayWritable,LongWritable,LongArrayWritable,LongWritable>{
		public userDefineMapper() throws Exception { super(LongArrayWritable.class,LongWritable.class); }
	}
	public static class userDefineReducer extends GenericProxyReducerSeq <LongArrayWritable,LongWritable>{
		public userDefineReducer() throws Exception { super(LongArrayWritable.class,LongWritable.class); }
	   
	}
	public static void main ( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run( new Configuration(), new GAMR(), args ) ;
		// Run the class MergeLog() after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}



}