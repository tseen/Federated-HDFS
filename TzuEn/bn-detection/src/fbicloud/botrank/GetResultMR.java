package fbicloud.botrank ;

import java.io.* ;
import java.util.* ;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured ;
import org.apache.hadoop.fs.Path ;
import org.apache.hadoop.fs.FileSystem ;
import org.apache.hadoop.fs.FileStatus ;
import org.apache.hadoop.io.LongWritable ;
import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.Mapper ;
import org.apache.hadoop.mapreduce.Reducer ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;
import org.apache.hadoop.util.GenericOptionsParser ;
import org.apache.hadoop.util.Tool ;
import org.apache.hadoop.util.ToolRunner ;


public class GetResultMR extends Configured implements Tool
{
	public static class GetResultMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		private Text interKey = new Text() ;
		private Text interValue = new Text() ;
		
		
		public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			//Input Format		FVID1,FVID2		score				Relative fvid
			//					1001,1412       0.04788     524,977;524,1399;977,994;994,1399;
			
			
			String[] subLine = value.toString().split("\t") ;
			String[] fvid = subLine[0].split(",") ;
			
			
			double score = Double.parseDouble( subLine[1] ) ;
			
			if ( score > 0 )
			{
				interKey.set( "key" ) ;
				interValue.set( fvid[0] + "\t" + fvid[1] ) ;
				context.write( interKey, interValue ) ;
			}
		}
	}
	
	public static class GetResultReducer extends Reducer <Text, Text, Text, Text>
	{
		private HashMap<String,String> fvidIPMapping  = new HashMap<String,String>() ;
		
		private Text outputKey = new Text() ;
		private Text outputValue = new Text() ;
		
		
		public void setup ( Context context ) throws IOException, InterruptedException
		{
			Configuration config = context.getConfiguration() ;
			
			String line ;
			String[] subLine = new String[2] ;
			
			FileSystem fs = FileSystem.get( config ) ;
			FileStatus[] status = fs.listStatus( new Path( config.get( "FVIDIPMapping" ) ) ) ;
			
			
			for ( int i = 0 ; i < status.length ; i++ )
			{
				BufferedReader br = new BufferedReader( new InputStreamReader( fs.open( status[i].getPath() ) ) ) ;
				
				line = br.readLine() ;
				while ( line != null )
				{
					//Input Format		fvid			srcIPs
					//					1680    140.116.121.170,140.116.1.136,140.116.26.49,140.116.122.209,140.116.200.100,
					
					subLine = line.split("\t") ;
					fvidIPMapping.put( subLine[0], subLine[1] ) ;
					line = br.readLine() ;
				}
			}
		}
		
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			List<HashSet<String>> list = new ArrayList<HashSet<String>>() ;
			
			Set<String> allFvidSet = new TreeSet<String>() ;
			Set<String> allSrcIPSet = new TreeSet<String>() ;
			
			boolean firsttime = true ;
			int fvid1hit = -1 ;
			int fvid2hit = -1 ; // if fvid1hit != -1 -> line[0] is in specific hashset
			
			
			for ( Text val : values )
			{
				String[] line = val.toString().split("\t") ; // line[0] -> IP1 ,line[1] -> IP2
				
				if ( firsttime )
				{
					list.add( new HashSet<String>( Arrays.asList( line[0], line[1] ) ) ) ;
					firsttime = false ;
				}
				else
				{
					fvid1hit = -1 ;
					fvid2hit = -1 ;
					
					for ( int i = 0 ; i < list.size() ; i ++ )
					{
						if ( list.get(i).contains( line[0] ) )
						{
							fvid1hit = i ;
						}
						if ( list.get(i).contains( line[1] ) )
						{
							fvid2hit = i ;
						}
					}
					
					if ( fvid1hit != -1 && fvid2hit != -1 )
					{
						if ( fvid1hit != fvid2hit )
						{
							HashSet<String> tmpHashSet = new HashSet<>() ;
							
							tmpHashSet.addAll( list.get(fvid1hit) ) ;
							tmpHashSet.addAll( list.get(fvid2hit) ) ;
							
							list.add( tmpHashSet ) ;
							
							if ( fvid1hit < fvid2hit )
							{
								list.remove( fvid2hit ) ;
								list.remove( fvid1hit ) ;
							}
							else
							{
								list.remove( fvid1hit ) ;
								list.remove( fvid2hit ) ;
							}
						}
					}
					else if ( fvid1hit == -1 && fvid2hit != -1 )
					{
						list.get( fvid2hit ).add( line[0] ) ;
						list.get( fvid2hit ).add( line[1] ) ;
					}
					else if ( fvid1hit != -1 && fvid2hit == -1 )
					{
						list.get( fvid1hit ).add( line[0] ) ;
						list.get( fvid1hit ).add( line[1] ) ;
					}
					else if ( fvid1hit == -1 && fvid2hit == -1 )
					{
						list.add( new HashSet<String>( Arrays.asList( line[0], line[1] ) ) ) ;
					}
					else{
					}
				}
			}
			
			outputKey.set( "Totoal Groups of P2P Botnet = " ) ;
			outputValue.set( String.valueOf( list.size() ) ) ;
			context.write( outputKey, outputValue ) ;
			
			
			for ( int i = 0 ; i < list.size() ; i ++ )
			{
				Set<String> fvidSet = new TreeSet<String>() ;
				Set<String> srcIPSet = new TreeSet<String>() ;
				
				StringBuffer outsb = new StringBuffer() ;
				String fvidSetStr = "" ;
				String ipSetStr = "" ;
				
				for ( String fvid : list.get(i) )
				{
					fvidSet.add( fvid ) ;
					
					String[] ip = fvidIPMapping.get(fvid).split(",");
					
					for ( int j = 0 ; j < ip.length ; j ++ )
					{
						srcIPSet.add( ip[j] ) ;
					}
				}
				
				
				for ( String ip : srcIPSet )
				{
					ipSetStr += ip + "," ;
					allSrcIPSet.add( ip ) ;
				}
				for ( String fvid : fvidSet )
				{
					outsb.append ( fvid + "," ) ;
					allFvidSet.add( fvid ) ;
				}
				
				outputKey.set( "Botnet FVID member : " + fvidSet.size() ) ;
				outputValue.set( outsb.toString() ) ;
				context.write( outputKey, outputValue ) ;
				
				outputKey.set( "Botnet Host IP : " + srcIPSet.size() ) ;
				outputValue.set( ipSetStr + "\n" ) ;
				context.write( outputKey, outputValue ) ;
			}
			
			outputKey.set( "Total FVID Set : " ) ;
			outputValue.set( String.valueOf( allFvidSet.size() ) ) ;
			context.write( outputKey, outputValue ) ;
			
			String allIpSetStr = "" ;
			for ( String ip : allSrcIPSet )
			{
				allIpSetStr += ip + "," ;
			}
			outputKey.set( "Total srcIP Set : " + allSrcIPSet.size() ) ;
			outputValue.set( allIpSetStr ) ;
			context.write( outputKey, outputValue ) ;
		}
	}
	
	
	public int run ( String[] args ) throws Exception
	{
		Configuration conf = this.getConf() ;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs() ;
		
		if ( otherArgs.length != 3 )
		{
			System.out.println ( "GetResultMR: <FVIDIPMappingList> <in> <out>" ) ;
			System.exit(2) ;
		}
		
		conf.set ( "FVIDIPMapping", args[0] ) ;
		
		
		Job job = Job.getInstance( conf, "Result" ) ;
		job.setJarByClass( GetResultMR.class ) ;
		
		job.setMapperClass( GetResultMapper.class ) ;
		job.setReducerClass( GetResultReducer.class ) ;
		
		job.setOutputKeyClass( Text.class ) ;
		job.setOutputValueClass( Text.class ) ;
		
		job.setMapOutputKeyClass( Text.class ) ;
		job.setMapOutputValueClass( Text.class ) ;
		
	    FileInputFormat.addInputPath( job, new Path( args[1] ) ) ;
	    FileOutputFormat.setOutputPath( job, new Path( args[2] ) ) ;
		return job.waitForCompletion( true ) ? 0 : 1 ;
	}
	
	public static void main( String[] args ) throws Exception
	{
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run( new Configuration(), new GetResultMR(), args ) ;
		// Run the class GetResultMR() after parsing with the given generic arguments
		// res == 0 -> normal exit
		// res != 0 -> Something error
		System.exit(res) ;
	}
}