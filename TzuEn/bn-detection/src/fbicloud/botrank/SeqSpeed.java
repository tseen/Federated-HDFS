package fbicloud.botrank;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SeqSpeed extends Configured implements Tool {
 

 
  public static class SeqMapper extends 
      Mapper<LongWritable, Text, LongWritable, Text> {

    public void map(LongWritable k,
    		Text v,
                    Context context) 
        throws IOException, InterruptedException {

      	context.write(k, v);
    }
  }

 
  public static class SeqReducer extends 
      Reducer<LongWritable, Text, LongWritable, Text> {
 
    public void reduce(LongWritable k,
    		Iterator<Text> values, Context context)
        throws IOException, InterruptedException {
    		System.out.println("reduce");
    		while (values.hasNext()) {
    			//System.out.println("K:"+k.toString()+" V:"+values.next().toString());
    			context.write(k, values.next());
    		}
    }

 
  }

  /**
   * Run a map/reduce job for estimating Pi.
   *
   * @return the estimated value of Pi
   */
  public static void seqRun(int numMaps, long numPoints,
      Path tmpDir, Configuration conf
      ) throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(conf);
    //setup job conf
    job.setJobName(SeqSpeed.class.getSimpleName());
    job.setJarByClass(SeqSpeed.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapperClass(SeqMapper.class);

    job.setReducerClass(SeqReducer.class);
   // job.setNumReduceTasks(1);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
  //  job.setSpeculativeExecution(false);

    //setup input/output directories
    final Path inDir = new Path(tmpDir, "in");
    final Path outDir = new Path(tmpDir, "out");
    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);

	final FileSystem fs = FileSystem.get(conf);
	if(conf.get("regionCloud", "f").equals("on")){

	    if (fs.exists(tmpDir)) {
	      throw new IOException("Tmp directory " + fs.makeQualified(tmpDir)
	          + " already exists.  Please remove it first.");
	    }
	    if (!fs.mkdirs(inDir)) {
	      throw new IOException("Cannot create input directory " + inDir);
	    }
	
	    
	      //generate an input file for each map task
	      for(int i=0; i < numMaps; ++i) {
	        final Path file = new Path(inDir, "part"+i);
	       
	        final SequenceFile.Writer writer = SequenceFile.createWriter(
	            fs, conf, file,
	            LongWritable.class, Text.class, CompressionType.NONE);
	        try {
	        	for(int i1 = 0; i1 < numPoints; i1++){
	        		final LongWritable k = new LongWritable(new Random().nextLong());
	     	        final Text v = new Text(Float.toString(new Random().nextFloat()));
	        		writer.append(k,v);     		
     		
	        			
	        	}
	        } finally {
	          writer.close();
	        }
	        System.out.println("Wrote input for Map #"+i);
	      }
      }
      //start a map/reduce job
      System.out.println("Starting Job");
      final long startTime = System.currentTimeMillis();
      job.waitForCompletion(true);
      final double duration = (System.currentTimeMillis() - startTime)/1000.0;
      System.out.println("Job Finished in " + duration + " seconds");

     
  }

  /**
   * Parse arguments and then runs a map/reduce job.
   * Print output in standard out.
   * 
   * @return a non-zero if there is an error.  Otherwise, return 0.  
   */
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: "+getClass().getName()+" <nMaps> <nSamples>");
      ToolRunner.printGenericCommandUsage(System.err);
      return 3;
    }
    
    final int nMaps = Integer.parseInt(args[0]);
    final long nSamples = Long.parseLong(args[1]);
    final Path tmpDir = new Path("seq" + args[2]);
        
    System.out.println("Number of Maps  = " + nMaps);
    System.out.println("Samples per Map = " + nSamples);
        
    
     seqRun(nMaps, nSamples, tmpDir, getConf());
    return 0;
  }

  /**
   * main method for running it as a stand alone command. 
   */
  public static void main(String[] argv) throws Exception {
    System.exit(ToolRunner.run(null, new SeqSpeed(), argv));
  }
}

