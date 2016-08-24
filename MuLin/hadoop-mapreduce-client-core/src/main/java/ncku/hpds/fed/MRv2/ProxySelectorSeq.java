/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/

package ncku.hpds.fed.MRv2 ;

import ncku.hpds.fed.MRv2.proxy.*;

import java.net.*;
import java.io.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ProxySelectorSeq extends AbstractProxySelector{
    // key, value --> generic class
    private Map<String, Map<String, Class< ? extends Mapper>>> proxyMapMapping = 
        new HashMap<String, Map<String, Class< ? extends Mapper>>>(); 
    private Map<String, Map<String, Class< ? extends Mapper>>> proxyMapMappingSeq = 
        new HashMap<String, Map<String, Class< ? extends Mapper>>>(); 
    private Map<String, Map<String, Class< ? extends Reducer>>> proxyReduceMapping = 
        new HashMap<String, Map<String, Class< ? extends Reducer>>>(); 
    private String SeqFormatCanonicalName = SequenceFileInputFormat.class.getCanonicalName();
    private String TextFormatCanonicalName = TextInputFormat.class.getCanonicalName();
    private String mJobInputFormatName = "";
    private Configuration mJobConf;
    private Job mJob;
    private static iAddOtherMapping mCallback = null;

    public ProxySelectorSeq(Configuration conf, Job job ) {
        mJobConf = conf;
        mJob = job;
        proxyMapMapping.clear();
        proxyMapMappingSeq.clear();
        proxyReduceMapping.clear();
        init();
    }
    //----------------------------------------------------------------------- 
    public interface iAddOtherMapping { 
        public void addProxyMapMapping(Map<String, Map<String, Class>> proxyMapMapping);
    }

    public static void setOtherMappingCallback(iAddOtherMapping m) {
        mCallback = m;
    } 
    //----------------------------------------------------------------------- 
    public Class< ? extends Mapper> getProxyMapperClass(Class keyClz, Class valueClz) {
        Map<String, Class<? extends Mapper>> valueMap = proxyMapMapping.get(keyClz.getCanonicalName()) ;
        Class<? extends Mapper> clz = valueMap.get(valueClz.getCanonicalName());
        System.out.println("Select ProxyMapperSeq : " + clz.getCanonicalName() );
        return clz;
    }
    public Class< ? extends Mapper> getProxyMapperClassSeq(Class keyClz, Class valueClz) {
        Map<String, Class<? extends Mapper>> valueMap = proxyMapMappingSeq.get(keyClz.getCanonicalName()) ;
        Class<? extends Mapper> clz = valueMap.get(valueClz.getCanonicalName());
        System.out.println("Select ProxyMapperSeq : " + clz.getCanonicalName() );
        return clz;
    }
    public Class< ? extends Reducer> getProxyReducerClass(Class keyClz, Class valueClz) {
        Map<String, Class<? extends Reducer>> valueMap = proxyReduceMapping.get(keyClz.getCanonicalName()) ;
        Class<? extends Reducer> clz = valueMap.get(valueClz.getCanonicalName());
        
        System.out.println("Select ProxyReducerSeq : " + clz.getCanonicalName() );
        return clz;
    }
    //----------------------------------------------------------------------- 

    protected void init() {
        // text, text 
        try {
            mJobInputFormatName = mJob.getInputFormatClass().getClass().getCanonicalName();
           
            addProxyMappers();
            addProxyReducers();
        } catch ( Exception e ) {
        }
    }
    //----------------------------------------------------------------------- 
    //proxy map/reduce version2
	protected void addProxyMappers(){
		addProxyMapperSeqMapping( DoubleWritable.class, DoubleWritable.class, ProxyMapperSeqDoubleDouble.class );
		addProxyMapperSeqMapping( DoubleWritable.class, FloatWritable.class, ProxyMapperSeqDoubleFloat.class );
		addProxyMapperSeqMapping( DoubleWritable.class, IntWritable.class, ProxyMapperSeqDoubleInt.class );
		addProxyMapperSeqMapping( DoubleWritable.class, LongWritable.class, ProxyMapperSeqDoubleLong.class );
		addProxyMapperSeqMapping( DoubleWritable.class, Text.class, ProxyMapperSeqDoubleText.class );
		addProxyMapperSeqMapping( DoubleWritable.class, UTF8.class, ProxyMapperSeqDoubleUTF8.class );
		addProxyMapperSeqMapping( DoubleWritable.class, VIntWritable.class, ProxyMapperSeqDoubleVInt.class );
		addProxyMapperSeqMapping( DoubleWritable.class, VLongWritable.class, ProxyMapperSeqDoubleVLong.class );
		addProxyMapperSeqMapping( DoubleWritable.class, NullWritable.class, ProxyMapperSeqDoubleNull.class );

		addProxyMapperSeqMapping( FloatWritable.class, DoubleWritable.class, ProxyMapperSeqFloatDouble.class );
		addProxyMapperSeqMapping( FloatWritable.class, FloatWritable.class, ProxyMapperSeqFloatFloat.class );
		addProxyMapperSeqMapping( FloatWritable.class, IntWritable.class, ProxyMapperSeqFloatInt.class );
		addProxyMapperSeqMapping( FloatWritable.class, LongWritable.class, ProxyMapperSeqFloatLong.class );
		addProxyMapperSeqMapping( FloatWritable.class, Text.class, ProxyMapperSeqFloatText.class );
		addProxyMapperSeqMapping( FloatWritable.class, UTF8.class, ProxyMapperSeqFloatUTF8.class );
		addProxyMapperSeqMapping( FloatWritable.class, VIntWritable.class, ProxyMapperSeqFloatVInt.class );
		addProxyMapperSeqMapping( FloatWritable.class, VLongWritable.class, ProxyMapperSeqFloatVLong.class );
		addProxyMapperSeqMapping( FloatWritable.class, NullWritable.class, ProxyMapperSeqFloatNull.class );

		addProxyMapperSeqMapping( IntWritable.class, DoubleWritable.class, ProxyMapperSeqIntDouble.class );
		addProxyMapperSeqMapping( IntWritable.class, FloatWritable.class, ProxyMapperSeqIntFloat.class );
		addProxyMapperSeqMapping( IntWritable.class, IntWritable.class, ProxyMapperSeqIntInt.class );
		addProxyMapperSeqMapping( IntWritable.class, LongWritable.class, ProxyMapperSeqIntLong.class );
		addProxyMapperSeqMapping( IntWritable.class, Text.class, ProxyMapperSeqIntText.class );
		addProxyMapperSeqMapping( IntWritable.class, UTF8.class, ProxyMapperSeqIntUTF8.class );
		addProxyMapperSeqMapping( IntWritable.class, VIntWritable.class, ProxyMapperSeqIntVInt.class );
		addProxyMapperSeqMapping( IntWritable.class, VLongWritable.class, ProxyMapperSeqIntVLong.class );
		addProxyMapperSeqMapping( IntWritable.class, NullWritable.class, ProxyMapperSeqIntNull.class );

		addProxyMapperSeqMapping( LongWritable.class, DoubleWritable.class, ProxyMapperSeqLongDouble.class );
		addProxyMapperSeqMapping( LongWritable.class, FloatWritable.class, ProxyMapperSeqLongFloat.class );
		addProxyMapperSeqMapping( LongWritable.class, IntWritable.class, ProxyMapperSeqLongInt.class );
		addProxyMapperSeqMapping( LongWritable.class, LongWritable.class, ProxyMapperSeqLongLong.class );
		addProxyMapperSeqMapping( LongWritable.class, Text.class, ProxyMapperSeqLongText.class );
		addProxyMapperSeqMapping( LongWritable.class, UTF8.class, ProxyMapperSeqLongUTF8.class );
		addProxyMapperSeqMapping( LongWritable.class, VIntWritable.class, ProxyMapperSeqLongVInt.class );
		addProxyMapperSeqMapping( LongWritable.class, VLongWritable.class, ProxyMapperSeqLongVLong.class );
		addProxyMapperSeqMapping( LongWritable.class, NullWritable.class, ProxyMapperSeqLongNull.class );

		addProxyMapperSeqMapping( Text.class, DoubleWritable.class, ProxyMapperSeqTextDouble.class );
		addProxyMapperSeqMapping( Text.class, FloatWritable.class, ProxyMapperSeqTextFloat.class );
		addProxyMapperSeqMapping( Text.class, IntWritable.class, ProxyMapperSeqTextInt.class );
		addProxyMapperSeqMapping( Text.class, LongWritable.class, ProxyMapperSeqTextLong.class );
		addProxyMapperSeqMapping( Text.class, Text.class, ProxyMapperSeqTextText.class );
		addProxyMapperSeqMapping( Text.class, UTF8.class, ProxyMapperSeqTextUTF8.class );
		addProxyMapperSeqMapping( Text.class, VIntWritable.class, ProxyMapperSeqTextVInt.class );
		addProxyMapperSeqMapping( Text.class, VLongWritable.class, ProxyMapperSeqTextVLong.class );
		addProxyMapperSeqMapping( Text.class, NullWritable.class, ProxyMapperSeqTextNull.class );

	/*	addProxyMapperSeqMapping( UTF8.class, DoubleWritable.class, ProxyMapperSeqUTF8Double.class );
		addProxyMapperSeqMapping( UTF8.class, FloatWritable.class, ProxyMapperSeqUTF8Float.class );
		addProxyMapperSeqMapping( UTF8.class, IntWritable.class, ProxyMapperSeqUTF8Int.class );
		addProxyMapperSeqMapping( UTF8.class, LongWritable.class, ProxyMapperSeqUTF8Long.class );
		addProxyMapperSeqMapping( UTF8.class, Text.class, ProxyMapperSeqUTF8Text.class );
		addProxyMapperSeqMapping( UTF8.class, UTF8.class, ProxyMapperSeqUTF8UTF8.class );
		addProxyMapperSeqMapping( UTF8.class, VIntWritable.class, ProxyMapperSeqUTF8VInt.class );
		addProxyMapperSeqMapping( UTF8.class, VLongWritable.class, ProxyMapperSeqUTF8VLong.class );
		addProxyMapperSeqMapping( UTF8.class, NullWritable.class, ProxyMapperSeqUTF8Null.class );
*/
		addProxyMapperSeqMapping( VIntWritable.class, DoubleWritable.class, ProxyMapperSeqVIntDouble.class );
		addProxyMapperSeqMapping( VIntWritable.class, FloatWritable.class, ProxyMapperSeqVIntFloat.class );
		addProxyMapperSeqMapping( VIntWritable.class, IntWritable.class, ProxyMapperSeqVIntInt.class );
		addProxyMapperSeqMapping( VIntWritable.class, LongWritable.class, ProxyMapperSeqVIntLong.class );
		addProxyMapperSeqMapping( VIntWritable.class, Text.class, ProxyMapperSeqVIntText.class );
		addProxyMapperSeqMapping( VIntWritable.class, UTF8.class, ProxyMapperSeqVIntUTF8.class );
		addProxyMapperSeqMapping( VIntWritable.class, VIntWritable.class, ProxyMapperSeqVIntVInt.class );
		addProxyMapperSeqMapping( VIntWritable.class, VLongWritable.class, ProxyMapperSeqVIntVLong.class );
		addProxyMapperSeqMapping( VIntWritable.class, NullWritable.class, ProxyMapperSeqVIntNull.class );

		addProxyMapperSeqMapping( VLongWritable.class, DoubleWritable.class, ProxyMapperSeqVLongDouble.class );
		addProxyMapperSeqMapping( VLongWritable.class, FloatWritable.class, ProxyMapperSeqVLongFloat.class );
		addProxyMapperSeqMapping( VLongWritable.class, IntWritable.class, ProxyMapperSeqVLongInt.class );
		addProxyMapperSeqMapping( VLongWritable.class, LongWritable.class, ProxyMapperSeqVLongLong.class );
		addProxyMapperSeqMapping( VLongWritable.class, Text.class, ProxyMapperSeqVLongText.class );
		addProxyMapperSeqMapping( VLongWritable.class, UTF8.class, ProxyMapperSeqVLongUTF8.class );
		addProxyMapperSeqMapping( VLongWritable.class, VIntWritable.class, ProxyMapperSeqVLongVInt.class );
		addProxyMapperSeqMapping( VLongWritable.class, VLongWritable.class, ProxyMapperSeqVLongVLong.class );
		addProxyMapperSeqMapping( VLongWritable.class, NullWritable.class, ProxyMapperSeqVLongNull.class );
		
		addProxyMapperSeqMapping( NullWritable.class, DoubleWritable.class, ProxyMapperSeqNullDouble.class );
		addProxyMapperSeqMapping( NullWritable.class, FloatWritable.class, ProxyMapperSeqNullFloat.class );
		addProxyMapperSeqMapping( NullWritable.class, IntWritable.class, ProxyMapperSeqNullInt.class );
		addProxyMapperSeqMapping( NullWritable.class, LongWritable.class, ProxyMapperSeqNullLong.class );
		addProxyMapperSeqMapping( NullWritable.class, Text.class, ProxyMapperSeqNullText.class );
		addProxyMapperSeqMapping( NullWritable.class, UTF8.class, ProxyMapperSeqNullUTF8.class );
		addProxyMapperSeqMapping( NullWritable.class, VIntWritable.class, ProxyMapperSeqNullVInt.class );
		addProxyMapperSeqMapping( NullWritable.class, VLongWritable.class, ProxyMapperSeqNullVLong.class );
		addProxyMapperSeqMapping( NullWritable.class, NullWritable.class, ProxyMapperSeqNullNull.class );
		
		addProxyMapperSeqMapping( BooleanWritable.class, DoubleWritable.class, ProxyMapperSeqBooleanDouble.class );
		addProxyMapperSeqMapping( BooleanWritable.class, FloatWritable.class, ProxyMapperSeqBooleanFloat.class );
		addProxyMapperSeqMapping( BooleanWritable.class, IntWritable.class, ProxyMapperSeqBooleanInt.class );
		addProxyMapperSeqMapping( BooleanWritable.class, LongWritable.class, ProxyMapperSeqBooleanLong.class );
		addProxyMapperSeqMapping( BooleanWritable.class, Text.class, ProxyMapperSeqBooleanText.class );
		addProxyMapperSeqMapping( BooleanWritable.class, UTF8.class, ProxyMapperSeqBooleanUTF8.class );
		addProxyMapperSeqMapping( BooleanWritable.class, VIntWritable.class, ProxyMapperSeqBooleanVInt.class );
		addProxyMapperSeqMapping( BooleanWritable.class, VLongWritable.class, ProxyMapperSeqBooleanVLong.class );
		addProxyMapperSeqMapping( BooleanWritable.class, BooleanWritable.class, ProxyMapperSeqBooleanBoolean.class );


	
	}

	protected void addProxyReducers(){
		addProxyReducerSeqMapping( DoubleWritable.class, DoubleWritable.class, ProxyReducerSeqDoubleDouble.class );
		addProxyReducerSeqMapping( DoubleWritable.class, FloatWritable.class, ProxyReducerSeqDoubleFloat.class );
		addProxyReducerSeqMapping( DoubleWritable.class, IntWritable.class, ProxyReducerSeqDoubleInt.class );
		addProxyReducerSeqMapping( DoubleWritable.class, LongWritable.class, ProxyReducerSeqDoubleLong.class );
		addProxyReducerSeqMapping( DoubleWritable.class, Text.class, ProxyReducerSeqDoubleText.class );
		addProxyReducerSeqMapping( DoubleWritable.class, UTF8.class, ProxyReducerSeqDoubleUTF8.class );
		addProxyReducerSeqMapping( DoubleWritable.class, VIntWritable.class, ProxyReducerSeqDoubleVInt.class );
		addProxyReducerSeqMapping( DoubleWritable.class, VLongWritable.class, ProxyReducerSeqDoubleVLong.class );
		addProxyReducerSeqMapping( DoubleWritable.class, NullWritable.class, ProxyReducerSeqDoubleNull.class );

		addProxyReducerSeqMapping( FloatWritable.class, DoubleWritable.class, ProxyReducerSeqFloatDouble.class );
		addProxyReducerSeqMapping( FloatWritable.class, FloatWritable.class, ProxyReducerSeqFloatFloat.class );
		addProxyReducerSeqMapping( FloatWritable.class, IntWritable.class, ProxyReducerSeqFloatInt.class );
		addProxyReducerSeqMapping( FloatWritable.class, LongWritable.class, ProxyReducerSeqFloatLong.class );
		addProxyReducerSeqMapping( FloatWritable.class, Text.class, ProxyReducerSeqFloatText.class );
		addProxyReducerSeqMapping( FloatWritable.class, UTF8.class, ProxyReducerSeqFloatUTF8.class );
		addProxyReducerSeqMapping( FloatWritable.class, VIntWritable.class, ProxyReducerSeqFloatVInt.class );
		addProxyReducerSeqMapping( FloatWritable.class, VLongWritable.class, ProxyReducerSeqFloatVLong.class );
		addProxyReducerSeqMapping( FloatWritable.class, NullWritable.class, ProxyReducerSeqFloatNull.class );

		addProxyReducerSeqMapping( IntWritable.class, DoubleWritable.class, ProxyReducerSeqIntDouble.class );
		addProxyReducerSeqMapping( IntWritable.class, FloatWritable.class, ProxyReducerSeqIntFloat.class );
		addProxyReducerSeqMapping( IntWritable.class, IntWritable.class, ProxyReducerSeqIntInt.class );
		addProxyReducerSeqMapping( IntWritable.class, LongWritable.class, ProxyReducerSeqIntLong.class );
		addProxyReducerSeqMapping( IntWritable.class, Text.class, ProxyReducerSeqIntText.class );
		addProxyReducerSeqMapping( IntWritable.class, UTF8.class, ProxyReducerSeqIntUTF8.class );
		addProxyReducerSeqMapping( IntWritable.class, VIntWritable.class, ProxyReducerSeqIntVInt.class );
		addProxyReducerSeqMapping( IntWritable.class, VLongWritable.class, ProxyReducerSeqIntVLong.class );
		addProxyReducerSeqMapping( IntWritable.class, NullWritable.class, ProxyReducerSeqIntNull.class );

		addProxyReducerSeqMapping( LongWritable.class, DoubleWritable.class, ProxyReducerSeqLongDouble.class );
		addProxyReducerSeqMapping( LongWritable.class, FloatWritable.class, ProxyReducerSeqLongFloat.class );
		addProxyReducerSeqMapping( LongWritable.class, IntWritable.class, ProxyReducerSeqLongInt.class );
		addProxyReducerSeqMapping( LongWritable.class, LongWritable.class, ProxyReducerSeqLongLong.class );
		addProxyReducerSeqMapping( LongWritable.class, Text.class, ProxyReducerSeqLongText.class );
		addProxyReducerSeqMapping( LongWritable.class, UTF8.class, ProxyReducerSeqLongUTF8.class );
		addProxyReducerSeqMapping( LongWritable.class, VIntWritable.class, ProxyReducerSeqLongVInt.class );
		addProxyReducerSeqMapping( LongWritable.class, VLongWritable.class, ProxyReducerSeqLongVLong.class );
		addProxyReducerSeqMapping( LongWritable.class, NullWritable.class, ProxyReducerSeqLongNull.class );

		addProxyReducerSeqMapping( Text.class, DoubleWritable.class, ProxyReducerSeqTextDouble.class );
		addProxyReducerSeqMapping( Text.class, FloatWritable.class, ProxyReducerSeqTextFloat.class );
		addProxyReducerSeqMapping( Text.class, IntWritable.class, ProxyReducerSeqTextInt.class );
		addProxyReducerSeqMapping( Text.class, LongWritable.class, ProxyReducerSeqTextLong.class );
		addProxyReducerSeqMapping( Text.class, Text.class, ProxyReducerSeqTextText.class );
		addProxyReducerSeqMapping( Text.class, UTF8.class, ProxyReducerSeqTextUTF8.class );
		addProxyReducerSeqMapping( Text.class, VIntWritable.class, ProxyReducerSeqTextVInt.class );
		addProxyReducerSeqMapping( Text.class, VLongWritable.class, ProxyReducerSeqTextVLong.class );
		addProxyReducerSeqMapping( Text.class, NullWritable.class, ProxyReducerSeqTextNull.class );

		addProxyReducerSeqMapping( UTF8.class, DoubleWritable.class, ProxyReducerSeqUTF8Double.class );
		addProxyReducerSeqMapping( UTF8.class, FloatWritable.class, ProxyReducerSeqUTF8Float.class );
		addProxyReducerSeqMapping( UTF8.class, IntWritable.class, ProxyReducerSeqUTF8Int.class );
		addProxyReducerSeqMapping( UTF8.class, LongWritable.class, ProxyReducerSeqUTF8Long.class );
		addProxyReducerSeqMapping( UTF8.class, Text.class, ProxyReducerSeqUTF8Text.class );
		addProxyReducerSeqMapping( UTF8.class, UTF8.class, ProxyReducerSeqUTF8UTF8.class );
		addProxyReducerSeqMapping( UTF8.class, VIntWritable.class, ProxyReducerSeqUTF8VInt.class );
		addProxyReducerSeqMapping( UTF8.class, VLongWritable.class, ProxyReducerSeqUTF8VLong.class );
		addProxyReducerSeqMapping( UTF8.class, NullWritable.class, ProxyReducerSeqUTF8Null.class );

		addProxyReducerSeqMapping( VIntWritable.class, DoubleWritable.class, ProxyReducerSeqVIntDouble.class );
		addProxyReducerSeqMapping( VIntWritable.class, FloatWritable.class, ProxyReducerSeqVIntFloat.class );
		addProxyReducerSeqMapping( VIntWritable.class, IntWritable.class, ProxyReducerSeqVIntInt.class );
		addProxyReducerSeqMapping( VIntWritable.class, LongWritable.class, ProxyReducerSeqVIntLong.class );
		addProxyReducerSeqMapping( VIntWritable.class, Text.class, ProxyReducerSeqVIntText.class );
		addProxyReducerSeqMapping( VIntWritable.class, UTF8.class, ProxyReducerSeqVIntUTF8.class );
		addProxyReducerSeqMapping( VIntWritable.class, VIntWritable.class, ProxyReducerSeqVIntVInt.class );
		addProxyReducerSeqMapping( VIntWritable.class, VLongWritable.class, ProxyReducerSeqVIntVLong.class );
		addProxyReducerSeqMapping( VIntWritable.class, NullWritable.class, ProxyReducerSeqVIntNull.class );

		addProxyReducerSeqMapping( VLongWritable.class, DoubleWritable.class, ProxyReducerSeqVLongDouble.class );
		addProxyReducerSeqMapping( VLongWritable.class, FloatWritable.class, ProxyReducerSeqVLongFloat.class );
		addProxyReducerSeqMapping( VLongWritable.class, IntWritable.class, ProxyReducerSeqVLongInt.class );
		addProxyReducerSeqMapping( VLongWritable.class, LongWritable.class, ProxyReducerSeqVLongLong.class );
		addProxyReducerSeqMapping( VLongWritable.class, Text.class, ProxyReducerSeqVLongText.class );
		addProxyReducerSeqMapping( VLongWritable.class, UTF8.class, ProxyReducerSeqVLongUTF8.class );
		addProxyReducerSeqMapping( VLongWritable.class, VIntWritable.class, ProxyReducerSeqVLongVInt.class );
		addProxyReducerSeqMapping( VLongWritable.class, VLongWritable.class, ProxyReducerSeqVLongVLong.class );
		addProxyReducerSeqMapping( VLongWritable.class, NullWritable.class, ProxyReducerSeqVLongNull.class );
		
		addProxyReducerSeqMapping( NullWritable.class, DoubleWritable.class, ProxyReducerSeqNullDouble.class );
		addProxyReducerSeqMapping( NullWritable.class, FloatWritable.class, ProxyReducerSeqNullFloat.class );
		addProxyReducerSeqMapping( NullWritable.class, IntWritable.class, ProxyReducerSeqNullInt.class );
		addProxyReducerSeqMapping( NullWritable.class, LongWritable.class, ProxyReducerSeqNullLong.class );
		addProxyReducerSeqMapping( NullWritable.class, Text.class, ProxyReducerSeqNullText.class );
		addProxyReducerSeqMapping( NullWritable.class, UTF8.class, ProxyReducerSeqNullUTF8.class );
		addProxyReducerSeqMapping( NullWritable.class, VIntWritable.class, ProxyReducerSeqNullVInt.class );
		addProxyReducerSeqMapping( NullWritable.class, VLongWritable.class, ProxyReducerSeqNullVLong.class );
		addProxyReducerSeqMapping( NullWritable.class, NullWritable.class, ProxyReducerSeqNullNull.class );

		addProxyReducerSeqMapping( BooleanWritable.class, DoubleWritable.class, ProxyReducerSeqBooleanDouble.class );
		addProxyReducerSeqMapping( BooleanWritable.class, FloatWritable.class, ProxyReducerSeqBooleanFloat.class );
		addProxyReducerSeqMapping( BooleanWritable.class, IntWritable.class, ProxyReducerSeqBooleanInt.class );
		addProxyReducerSeqMapping( BooleanWritable.class, LongWritable.class, ProxyReducerSeqBooleanLong.class );
		addProxyReducerSeqMapping( BooleanWritable.class, Text.class, ProxyReducerSeqBooleanText.class );
		addProxyReducerSeqMapping( BooleanWritable.class, UTF8.class, ProxyReducerSeqBooleanUTF8.class );
		addProxyReducerSeqMapping( BooleanWritable.class, VIntWritable.class, ProxyReducerSeqBooleanVInt.class );
		addProxyReducerSeqMapping( BooleanWritable.class, VLongWritable.class, ProxyReducerSeqBooleanVLong.class );
		addProxyReducerSeqMapping( BooleanWritable.class, BooleanWritable.class, ProxyReducerSeqBooleanBoolean.class );

	}
    //----------------------------------------------------------------------- 
    public void addProxyMapperSeqMapping( Class keyClz, Class valueClz, Class<? extends Mapper> pmClz ) {
        Map<String, Class<? extends Mapper>> valueMap = proxyMapMapping.get(keyClz.getCanonicalName());
        if ( valueMap == null ) {
            valueMap = new HashMap<String, Class<? extends Mapper>>();
            proxyMapMapping.put(keyClz.getCanonicalName(), valueMap);
        }
        valueMap.put( valueClz.getCanonicalName(), pmClz );
    }
    public void addProxyMapperSeqMappingSeq( Class keyClz, Class valueClz, Class<? extends Mapper> pmClz ) {
        Map<String, Class<? extends Mapper>> valueMap = proxyMapMappingSeq.get(keyClz.getCanonicalName());
        if ( valueMap == null ) {
            valueMap = new HashMap<String, Class<? extends Mapper>>();
            proxyMapMappingSeq.put(keyClz.getCanonicalName(), valueMap);
        }
        valueMap.put( valueClz.getCanonicalName(), pmClz );
    }
    public void addProxyReducerSeqMapping( Class keyClz, Class valueClz, Class<? extends Reducer> pmClz ) {
        Map<String, Class <? extends Reducer>> valueMap = proxyReduceMapping.get(keyClz.getCanonicalName());
        if ( valueMap == null ) {
            valueMap = new HashMap<String, Class <? extends Reducer>>();
            proxyReduceMapping.put(keyClz.getCanonicalName(), valueMap);
        }
        valueMap.put( valueClz.getCanonicalName(), pmClz );
    }
    //----------------------------------------------------------------------- 
	// Proxy Mapper Dummay Classes 

	// Proxy Mapper DoubleWritable
	public static class ProxyMapperSeqDoubleDouble extends GenericProxyMapperSeq <DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable>{
		public ProxyMapperSeqDoubleDouble() throws Exception { super(DoubleWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqDoubleFloat extends GenericProxyMapperSeq <DoubleWritable,FloatWritable,DoubleWritable,FloatWritable>{
		public ProxyMapperSeqDoubleFloat() throws Exception { super(DoubleWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqDoubleInt extends GenericProxyMapperSeq <DoubleWritable,IntWritable,DoubleWritable,IntWritable>{
		public ProxyMapperSeqDoubleInt() throws Exception { super(DoubleWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqDoubleLong extends GenericProxyMapperSeq <DoubleWritable,LongWritable,DoubleWritable,LongWritable>{
		public ProxyMapperSeqDoubleLong() throws Exception { super(DoubleWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqDoubleText extends GenericProxyMapperSeq <DoubleWritable,Text,DoubleWritable,Text>{
		public ProxyMapperSeqDoubleText() throws Exception { super(DoubleWritable.class,Text.class); }
	}
	public static class ProxyMapperSeqDoubleUTF8 extends GenericProxyMapperSeq <DoubleWritable,UTF8,DoubleWritable,UTF8>{
		public ProxyMapperSeqDoubleUTF8() throws Exception { super(DoubleWritable.class,UTF8.class); }
	}
	public static class ProxyMapperSeqDoubleVInt extends GenericProxyMapperSeq <DoubleWritable,VIntWritable,DoubleWritable,VIntWritable>{
		public ProxyMapperSeqDoubleVInt() throws Exception { super(DoubleWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqDoubleVLong extends GenericProxyMapperSeq <DoubleWritable,VLongWritable,DoubleWritable,VLongWritable>{
		public ProxyMapperSeqDoubleVLong() throws Exception { super(DoubleWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqDoubleNull extends GenericProxyMapperSeq <DoubleWritable,NullWritable,DoubleWritable,NullWritable>{
		public ProxyMapperSeqDoubleNull() throws Exception { super(DoubleWritable.class,NullWritable.class); }
	}

	// Proxy Mapper FloatWritable
	public static class ProxyMapperSeqFloatDouble extends GenericProxyMapperSeq <FloatWritable,DoubleWritable,FloatWritable,DoubleWritable>{
		public ProxyMapperSeqFloatDouble() throws Exception { super(FloatWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqFloatFloat extends GenericProxyMapperSeq <FloatWritable,FloatWritable,FloatWritable,FloatWritable>{
		public ProxyMapperSeqFloatFloat() throws Exception { super(FloatWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqFloatInt extends GenericProxyMapperSeq <FloatWritable,IntWritable,FloatWritable,IntWritable>{
		public ProxyMapperSeqFloatInt() throws Exception { super(FloatWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqFloatLong extends GenericProxyMapperSeq <FloatWritable,LongWritable,FloatWritable,LongWritable>{
		public ProxyMapperSeqFloatLong() throws Exception { super(FloatWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqFloatText extends GenericProxyMapperSeq <FloatWritable,Text,FloatWritable,Text>{
		public ProxyMapperSeqFloatText() throws Exception { super(FloatWritable.class,Text.class); }
	}
	public static class ProxyMapperSeqFloatUTF8 extends GenericProxyMapperSeq <FloatWritable,UTF8,FloatWritable,UTF8>{
		public ProxyMapperSeqFloatUTF8() throws Exception { super(FloatWritable.class,UTF8.class); }
	}
	public static class ProxyMapperSeqFloatVInt extends GenericProxyMapperSeq <FloatWritable,VIntWritable,FloatWritable,VIntWritable>{
		public ProxyMapperSeqFloatVInt() throws Exception { super(FloatWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqFloatVLong extends GenericProxyMapperSeq <FloatWritable,VLongWritable,FloatWritable,VLongWritable>{
		public ProxyMapperSeqFloatVLong() throws Exception { super(FloatWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqFloatNull extends GenericProxyMapperSeq <FloatWritable,NullWritable,FloatWritable,NullWritable>{
		public ProxyMapperSeqFloatNull() throws Exception { super(FloatWritable.class,NullWritable.class); }
	}

	// Proxy Mapper IntWritable
	public static class ProxyMapperSeqIntDouble extends GenericProxyMapperSeq <IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
		public ProxyMapperSeqIntDouble() throws Exception { super(IntWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqIntFloat extends GenericProxyMapperSeq <IntWritable,FloatWritable,IntWritable,FloatWritable>{
		public ProxyMapperSeqIntFloat() throws Exception { super(IntWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqIntInt extends GenericProxyMapperSeq <IntWritable,IntWritable,IntWritable,IntWritable>{
		public ProxyMapperSeqIntInt() throws Exception { super(IntWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqIntLong extends GenericProxyMapperSeq <IntWritable,LongWritable,IntWritable,LongWritable>{
		public ProxyMapperSeqIntLong() throws Exception { super(IntWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqIntText extends GenericProxyMapperSeq <IntWritable,Text,IntWritable,Text>{
		public ProxyMapperSeqIntText() throws Exception { super(IntWritable.class,Text.class); }
	}
	public static class ProxyMapperSeqIntUTF8 extends GenericProxyMapperSeq <IntWritable,UTF8,IntWritable,UTF8>{
		public ProxyMapperSeqIntUTF8() throws Exception { super(IntWritable.class,UTF8.class); }
	}
	public static class ProxyMapperSeqIntVInt extends GenericProxyMapperSeq <IntWritable,VIntWritable,IntWritable,VIntWritable>{
		public ProxyMapperSeqIntVInt() throws Exception { super(IntWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqIntVLong extends GenericProxyMapperSeq <IntWritable,VLongWritable,IntWritable,VLongWritable>{
		public ProxyMapperSeqIntVLong() throws Exception { super(IntWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqIntNull extends GenericProxyMapperSeq <IntWritable,NullWritable,IntWritable,NullWritable>{
		public ProxyMapperSeqIntNull() throws Exception { super(IntWritable.class,NullWritable.class); }
	}

	// Proxy Mapper LongWritable
	public static class ProxyMapperSeqLongDouble extends GenericProxyMapperSeq <LongWritable,DoubleWritable,LongWritable,DoubleWritable>{
		public ProxyMapperSeqLongDouble() throws Exception { super(LongWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqLongFloat extends GenericProxyMapperSeq <LongWritable,FloatWritable,LongWritable,FloatWritable>{
		public ProxyMapperSeqLongFloat() throws Exception { super(LongWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqLongInt extends GenericProxyMapperSeq <LongWritable,IntWritable,LongWritable,IntWritable>{
		public ProxyMapperSeqLongInt() throws Exception { super(LongWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqLongLong extends GenericProxyMapperSeq <LongWritable,LongWritable,LongWritable,LongWritable>{
		public ProxyMapperSeqLongLong() throws Exception { super(LongWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqLongText extends GenericProxyMapperSeq <LongWritable,Text,LongWritable,Text>{
		public ProxyMapperSeqLongText() throws Exception { super(LongWritable.class,Text.class); }
	}
	public static class ProxyMapperSeqLongUTF8 extends GenericProxyMapperSeq <LongWritable,UTF8,LongWritable,UTF8>{
		public ProxyMapperSeqLongUTF8() throws Exception { super(LongWritable.class,UTF8.class); }
	}
	public static class ProxyMapperSeqLongVInt extends GenericProxyMapperSeq <LongWritable,VIntWritable,LongWritable,VIntWritable>{
		public ProxyMapperSeqLongVInt() throws Exception { super(LongWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqLongVLong extends GenericProxyMapperSeq <LongWritable,VLongWritable,LongWritable,VLongWritable>{
		public ProxyMapperSeqLongVLong() throws Exception { super(LongWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqLongNull extends GenericProxyMapperSeq <LongWritable,NullWritable,LongWritable,NullWritable>{
		public ProxyMapperSeqLongNull() throws Exception { super(LongWritable.class,NullWritable.class); }
	}

	// Proxy Mapper Text
	public static class ProxyMapperSeqTextDouble extends GenericProxyMapperSeq <Text,DoubleWritable,Text,DoubleWritable>{
		public ProxyMapperSeqTextDouble() throws Exception { super(Text.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqTextFloat extends GenericProxyMapperSeq <Text,FloatWritable,Text,FloatWritable>{
		public ProxyMapperSeqTextFloat() throws Exception { super(Text.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqTextInt extends GenericProxyMapperSeq <Text,IntWritable,Text,IntWritable>{
		public ProxyMapperSeqTextInt() throws Exception { super(Text.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqTextLong extends GenericProxyMapperSeq <Text,LongWritable,Text,LongWritable>{
		public ProxyMapperSeqTextLong() throws Exception { super(Text.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqTextText extends GenericProxyMapperSeq <Text,Text,Text,Text>{
		public ProxyMapperSeqTextText() throws Exception { super(Text.class,Text.class); }
	}
	public static class ProxyMapperSeqTextUTF8 extends GenericProxyMapperSeq <Text,UTF8,Text,UTF8>{
		public ProxyMapperSeqTextUTF8() throws Exception { super(Text.class,UTF8.class); }
	}
	public static class ProxyMapperSeqTextVInt extends GenericProxyMapperSeq <Text,VIntWritable,Text,VIntWritable>{
		public ProxyMapperSeqTextVInt() throws Exception { super(Text.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqTextVLong extends GenericProxyMapperSeq <Text,VLongWritable,Text,VLongWritable>{
		public ProxyMapperSeqTextVLong() throws Exception { super(Text.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqTextNull extends GenericProxyMapperSeq <Text,NullWritable,Text,NullWritable>{
		public ProxyMapperSeqTextNull() throws Exception { super(Text.class,NullWritable.class); }
	}
/*
	// Proxy Mapper UTF8
	public static class ProxyMapperSeqUTF8Double extends GenericProxyMapperSeq <UTF8,DoubleWritable>{
		public ProxyMapperSeqUTF8Double() throws Exception { super(UTF8.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqUTF8Float extends GenericProxyMapperSeq <UTF8,FloatWritable>{
		public ProxyMapperSeqUTF8Float() throws Exception { super(UTF8.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqUTF8Int extends GenericProxyMapperSeq <UTF8,IntWritable>{
		public ProxyMapperSeqUTF8Int() throws Exception { super(UTF8.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqUTF8Long extends GenericProxyMapperSeq <UTF8,LongWritable>{
		public ProxyMapperSeqUTF8Long() throws Exception { super(UTF8.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqUTF8Text extends GenericProxyMapperSeq <UTF8,Text>{
		public ProxyMapperSeqUTF8Text() throws Exception { super(UTF8.class,Text.class); }
	}
	public static class ProxyMapperSeqUTF8UTF8 extends GenericProxyMapperSeq <UTF8,UTF8>{
		public ProxyMapperSeqUTF8UTF8() throws Exception { super(UTF8.class,UTF8.class); }
	}
	public static class ProxyMapperSeqUTF8VInt extends GenericProxyMapperSeq <UTF8,VIntWritable>{
		public ProxyMapperSeqUTF8VInt() throws Exception { super(UTF8.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqUTF8VLong extends GenericProxyMapperSeq <UTF8,VLongWritable>{
		public ProxyMapperSeqUTF8VLong() throws Exception { super(UTF8.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqUTF8Null extends GenericProxyMapperSeq <UTF8,NullWritable>{
		public ProxyMapperSeqUTF8Null() throws Exception { super(UTF8.class,NullWritable.class); }
	}
*/
	// Proxy Mapper VIntWritable
	public static class ProxyMapperSeqVIntDouble extends GenericProxyMapperSeq <VIntWritable,DoubleWritable,VIntWritable,DoubleWritable>{
		public ProxyMapperSeqVIntDouble() throws Exception { super(VIntWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqVIntFloat extends GenericProxyMapperSeq <VIntWritable,FloatWritable,VIntWritable,FloatWritable>{
		public ProxyMapperSeqVIntFloat() throws Exception { super(VIntWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqVIntInt extends GenericProxyMapperSeq <VIntWritable,IntWritable,VIntWritable,IntWritable>{
		public ProxyMapperSeqVIntInt() throws Exception { super(VIntWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqVIntLong extends GenericProxyMapperSeq <VIntWritable,LongWritable,VIntWritable,LongWritable>{
		public ProxyMapperSeqVIntLong() throws Exception { super(VIntWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqVIntText extends GenericProxyMapperSeq <VIntWritable,Text,VIntWritable,Text>{
		public ProxyMapperSeqVIntText() throws Exception { super(VIntWritable.class,Text.class); }
	}
	public static class ProxyMapperSeqVIntUTF8 extends GenericProxyMapperSeq <VIntWritable,UTF8,VIntWritable,UTF8>{
		public ProxyMapperSeqVIntUTF8() throws Exception { super(VIntWritable.class,UTF8.class); }
	}
	public static class ProxyMapperSeqVIntVInt extends GenericProxyMapperSeq <VIntWritable,VIntWritable,VIntWritable,VIntWritable>{
		public ProxyMapperSeqVIntVInt() throws Exception { super(VIntWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqVIntVLong extends GenericProxyMapperSeq <VIntWritable,VLongWritable,VIntWritable,VLongWritable>{
		public ProxyMapperSeqVIntVLong() throws Exception { super(VIntWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqVIntNull extends GenericProxyMapperSeq <VIntWritable,NullWritable,VIntWritable,NullWritable>{
		public ProxyMapperSeqVIntNull() throws Exception { super(VIntWritable.class,NullWritable.class); }
	}

	// Proxy Mapper VLongWritable
	public static class ProxyMapperSeqVLongDouble extends GenericProxyMapperSeq <VLongWritable,DoubleWritable,VLongWritable,DoubleWritable>{
		public ProxyMapperSeqVLongDouble() throws Exception { super(VLongWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperSeqVLongFloat extends GenericProxyMapperSeq <VLongWritable,FloatWritable,VLongWritable,FloatWritable>{
		public ProxyMapperSeqVLongFloat() throws Exception { super(VLongWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperSeqVLongInt extends GenericProxyMapperSeq <VLongWritable,IntWritable,VLongWritable,IntWritable>{
		public ProxyMapperSeqVLongInt() throws Exception { super(VLongWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperSeqVLongLong extends GenericProxyMapperSeq <VLongWritable,LongWritable,VLongWritable,LongWritable>{
		public ProxyMapperSeqVLongLong() throws Exception { super(VLongWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperSeqVLongText extends GenericProxyMapperSeq <VLongWritable,Text,VLongWritable,Text>{
		public ProxyMapperSeqVLongText() throws Exception { super(VLongWritable.class,Text.class); }
	}
	public static class ProxyMapperSeqVLongUTF8 extends GenericProxyMapperSeq <VLongWritable,UTF8,VLongWritable,UTF8>{
		public ProxyMapperSeqVLongUTF8() throws Exception { super(VLongWritable.class,UTF8.class); }
	}
	public static class ProxyMapperSeqVLongVInt extends GenericProxyMapperSeq <VLongWritable,VIntWritable,VLongWritable,VIntWritable>{
		public ProxyMapperSeqVLongVInt() throws Exception { super(VLongWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperSeqVLongVLong extends GenericProxyMapperSeq <VLongWritable,VLongWritable,VLongWritable,VLongWritable>{
		public ProxyMapperSeqVLongVLong() throws Exception { super(VLongWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperSeqVLongNull extends GenericProxyMapperSeq <VLongWritable,NullWritable,VLongWritable,NullWritable>{
		public ProxyMapperSeqVLongNull() throws Exception { super(VLongWritable.class,NullWritable.class); }
	}
    //--------------------------------------------------------------------------------------------------------
    // proxy mapper for sequencefile
	// Proxy Mapper DoubleWritable
	
	// Proxy Mapper NullWritable
		public static class ProxyMapperSeqNullDouble extends GenericProxyMapperSeq <NullWritable,DoubleWritable,NullWritable,DoubleWritable>{
			public ProxyMapperSeqNullDouble() throws Exception { super(NullWritable.class,DoubleWritable.class); }
		}
		public static class ProxyMapperSeqNullFloat extends GenericProxyMapperSeq <NullWritable,FloatWritable,NullWritable,FloatWritable>{
			public ProxyMapperSeqNullFloat() throws Exception { super(NullWritable.class,FloatWritable.class); }
		}
		public static class ProxyMapperSeqNullInt extends GenericProxyMapperSeq <NullWritable,IntWritable,NullWritable,IntWritable>{
			public ProxyMapperSeqNullInt() throws Exception { super(NullWritable.class,IntWritable.class); }
		}
		public static class ProxyMapperSeqNullLong extends GenericProxyMapperSeq <NullWritable,LongWritable,NullWritable,LongWritable>{
			public ProxyMapperSeqNullLong() throws Exception { super(NullWritable.class,LongWritable.class); }
		}
		public static class ProxyMapperSeqNullText extends GenericProxyMapperSeq <NullWritable,Text,NullWritable,Text>{
			public ProxyMapperSeqNullText() throws Exception { super(NullWritable.class,Text.class); }
		}
		public static class ProxyMapperSeqNullUTF8 extends GenericProxyMapperSeq <NullWritable,UTF8,NullWritable,UTF8>{
			public ProxyMapperSeqNullUTF8() throws Exception { super(NullWritable.class,UTF8.class); }
		}
		public static class ProxyMapperSeqNullVInt extends GenericProxyMapperSeq <NullWritable,VIntWritable,NullWritable,VIntWritable>{
			public ProxyMapperSeqNullVInt() throws Exception { super(NullWritable.class,VIntWritable.class); }
		}
		public static class ProxyMapperSeqNullVLong extends GenericProxyMapperSeq <NullWritable,VLongWritable,NullWritable,VLongWritable>{
			public ProxyMapperSeqNullVLong() throws Exception { super(NullWritable.class,VLongWritable.class); }
		}
		public static class ProxyMapperSeqNullNull extends GenericProxyMapperSeq <NullWritable,NullWritable,NullWritable,NullWritable>{
			public ProxyMapperSeqNullNull() throws Exception { super(NullWritable.class,NullWritable.class); }
		}

		public static class ProxyMapperSeqBooleanDouble extends GenericProxyMapperSeq <BooleanWritable,DoubleWritable,BooleanWritable,DoubleWritable>{
			public ProxyMapperSeqBooleanDouble() throws Exception { super(BooleanWritable.class,DoubleWritable.class); }
		}
		public static class ProxyMapperSeqBooleanFloat extends GenericProxyMapperSeq <BooleanWritable,FloatWritable,BooleanWritable,FloatWritable>{
			public ProxyMapperSeqBooleanFloat() throws Exception { super(BooleanWritable.class,FloatWritable.class); }
		}
		public static class ProxyMapperSeqBooleanInt extends GenericProxyMapperSeq <BooleanWritable,IntWritable,BooleanWritable,IntWritable>{
			public ProxyMapperSeqBooleanInt() throws Exception { super(BooleanWritable.class,IntWritable.class); }
		}
		public static class ProxyMapperSeqBooleanLong extends GenericProxyMapperSeq <BooleanWritable,LongWritable,BooleanWritable,LongWritable>{
			public ProxyMapperSeqBooleanLong() throws Exception { super(BooleanWritable.class,LongWritable.class); }
		}
		public static class ProxyMapperSeqBooleanText extends GenericProxyMapperSeq <BooleanWritable,Text,BooleanWritable,Text>{
			public ProxyMapperSeqBooleanText() throws Exception { super(BooleanWritable.class,Text.class); }
		}
		public static class ProxyMapperSeqBooleanUTF8 extends GenericProxyMapperSeq <BooleanWritable,UTF8,BooleanWritable,UTF8>{
			public ProxyMapperSeqBooleanUTF8() throws Exception { super(BooleanWritable.class,UTF8.class); }
		}
		public static class ProxyMapperSeqBooleanVInt extends GenericProxyMapperSeq <BooleanWritable,VIntWritable,BooleanWritable,VIntWritable>{
			public ProxyMapperSeqBooleanVInt() throws Exception { super(BooleanWritable.class,VIntWritable.class); }
		}
		public static class ProxyMapperSeqBooleanVLong extends GenericProxyMapperSeq <BooleanWritable,VLongWritable,BooleanWritable,VLongWritable>{
			public ProxyMapperSeqBooleanVLong() throws Exception { super(BooleanWritable.class,VLongWritable.class); }
		}
		public static class ProxyMapperSeqBooleanBoolean extends GenericProxyMapperSeq <BooleanWritable,BooleanWritable,BooleanWritable,BooleanWritable>{
			public ProxyMapperSeqBooleanBoolean() throws Exception { super(BooleanWritable.class,BooleanWritable.class); }
		}
    //----------------------------------------------------------------------- 
	// Proxy Reducer Dummy Classes 

	// Proxy Reducer DoubleWritable
	public static class ProxyReducerSeqDoubleDouble extends GenericProxyReducerSeq <DoubleWritable, DoubleWritable>{
        public ProxyReducerSeqDoubleDouble() throws Exception { super(DoubleWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqDoubleFloat extends GenericProxyReducerSeq <DoubleWritable, FloatWritable>{
        public ProxyReducerSeqDoubleFloat() throws Exception { super(DoubleWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqDoubleInt extends GenericProxyReducerSeq <DoubleWritable, IntWritable>{
        public ProxyReducerSeqDoubleInt() throws Exception { super(DoubleWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqDoubleLong extends GenericProxyReducerSeq <DoubleWritable, LongWritable>{
        public ProxyReducerSeqDoubleLong() throws Exception { super(DoubleWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqDoubleText extends GenericProxyReducerSeq <DoubleWritable, Text>{
        public ProxyReducerSeqDoubleText() throws Exception { super(DoubleWritable.class, Text.class); }
    }
	public static class ProxyReducerSeqDoubleUTF8 extends GenericProxyReducerSeq <DoubleWritable, UTF8>{
        public ProxyReducerSeqDoubleUTF8() throws Exception { super(DoubleWritable.class, UTF8.class); }
    }
	public static class ProxyReducerSeqDoubleVInt extends GenericProxyReducerSeq <DoubleWritable, VIntWritable>{
        public ProxyReducerSeqDoubleVInt() throws Exception { super(DoubleWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqDoubleVLong extends GenericProxyReducerSeq <DoubleWritable, VLongWritable>{
        public ProxyReducerSeqDoubleVLong() throws Exception { super(DoubleWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqDoubleNull extends GenericProxyReducerSeq <DoubleWritable, NullWritable>{
        public ProxyReducerSeqDoubleNull() throws Exception { super(DoubleWritable.class, NullWritable.class); }
    }
	// Proxy Reducer FloatWritable
	public static class ProxyReducerSeqFloatDouble extends GenericProxyReducerSeq <FloatWritable, DoubleWritable>{
        public ProxyReducerSeqFloatDouble() throws Exception { super(FloatWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqFloatFloat extends GenericProxyReducerSeq <FloatWritable, FloatWritable>{
        public ProxyReducerSeqFloatFloat() throws Exception { super(FloatWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqFloatInt extends GenericProxyReducerSeq <FloatWritable, IntWritable>{
        public ProxyReducerSeqFloatInt() throws Exception { super(FloatWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqFloatLong extends GenericProxyReducerSeq <FloatWritable, LongWritable>{
        public ProxyReducerSeqFloatLong() throws Exception { super(FloatWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqFloatText extends GenericProxyReducerSeq <FloatWritable, Text>{
        public ProxyReducerSeqFloatText() throws Exception { super(FloatWritable.class, Text.class); }
    }
	public static class ProxyReducerSeqFloatUTF8 extends GenericProxyReducerSeq <FloatWritable, UTF8>{
        public ProxyReducerSeqFloatUTF8() throws Exception { super(FloatWritable.class, UTF8.class); }
    }
	public static class ProxyReducerSeqFloatVInt extends GenericProxyReducerSeq <FloatWritable, VIntWritable>{
        public ProxyReducerSeqFloatVInt() throws Exception { super(FloatWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqFloatVLong extends GenericProxyReducerSeq <FloatWritable, VLongWritable>{
        public ProxyReducerSeqFloatVLong() throws Exception { super(FloatWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqFloatNull extends GenericProxyReducerSeq <FloatWritable, NullWritable>{
        public ProxyReducerSeqFloatNull() throws Exception { super(FloatWritable.class, NullWritable.class); }
    }
	// Proxy Reducer IntWritable
	public static class ProxyReducerSeqIntDouble extends GenericProxyReducerSeq <IntWritable, DoubleWritable>{
        public ProxyReducerSeqIntDouble() throws Exception { super(IntWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqIntFloat extends GenericProxyReducerSeq <IntWritable, FloatWritable>{
        public ProxyReducerSeqIntFloat() throws Exception { super(IntWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqIntInt extends GenericProxyReducerSeq <IntWritable, IntWritable>{
        public ProxyReducerSeqIntInt() throws Exception { super(IntWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqIntLong extends GenericProxyReducerSeq <IntWritable, LongWritable>{
        public ProxyReducerSeqIntLong() throws Exception { super(IntWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqIntText extends GenericProxyReducerSeq <IntWritable, Text>{
        public ProxyReducerSeqIntText() throws Exception { super(IntWritable.class, Text.class); }
    }
	public static class ProxyReducerSeqIntUTF8 extends GenericProxyReducerSeq <IntWritable, UTF8>{
        public ProxyReducerSeqIntUTF8() throws Exception { super(IntWritable.class, UTF8.class); }
    }
	public static class ProxyReducerSeqIntVInt extends GenericProxyReducerSeq <IntWritable, VIntWritable>{
        public ProxyReducerSeqIntVInt() throws Exception { super(IntWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqIntVLong extends GenericProxyReducerSeq <IntWritable, VLongWritable>{
        public ProxyReducerSeqIntVLong() throws Exception { super(IntWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqIntNull extends GenericProxyReducerSeq <IntWritable, NullWritable>{
        public ProxyReducerSeqIntNull() throws Exception { super(IntWritable.class, NullWritable.class); }
    }

	// Proxy Reducer LongWritable
	public static class ProxyReducerSeqLongDouble extends GenericProxyReducerSeq <LongWritable, DoubleWritable>{
        public ProxyReducerSeqLongDouble() throws Exception { super(LongWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqLongFloat extends GenericProxyReducerSeq <LongWritable, FloatWritable>{
        public ProxyReducerSeqLongFloat() throws Exception { super(LongWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqLongInt extends GenericProxyReducerSeq <LongWritable, IntWritable>{
        public ProxyReducerSeqLongInt() throws Exception { super(LongWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqLongLong extends GenericProxyReducerSeq <LongWritable, LongWritable>{
        public ProxyReducerSeqLongLong() throws Exception { super(LongWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqLongText extends GenericProxyReducerSeq <LongWritable, Text>{
        public ProxyReducerSeqLongText() throws Exception { super(LongWritable.class, Text.class); }
    }
	public static class ProxyReducerSeqLongUTF8 extends GenericProxyReducerSeq <LongWritable, UTF8>{
        public ProxyReducerSeqLongUTF8() throws Exception { super(LongWritable.class, UTF8.class); }
    }
	public static class ProxyReducerSeqLongVInt extends GenericProxyReducerSeq <LongWritable, VIntWritable>{
        public ProxyReducerSeqLongVInt() throws Exception { super(LongWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqLongVLong extends GenericProxyReducerSeq <LongWritable, VLongWritable>{
        public ProxyReducerSeqLongVLong() throws Exception { super(LongWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqLongNull extends GenericProxyReducerSeq <LongWritable, NullWritable>{
        public ProxyReducerSeqLongNull() throws Exception { super(LongWritable.class, NullWritable.class); }
    }

	// Proxy Reducer Text
	public static class ProxyReducerSeqTextDouble extends GenericProxyReducerSeq <Text, DoubleWritable>{
        public ProxyReducerSeqTextDouble() throws Exception { super(Text.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqTextFloat extends GenericProxyReducerSeq <Text, FloatWritable>{
        public ProxyReducerSeqTextFloat() throws Exception { super(Text.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqTextInt extends GenericProxyReducerSeq <Text, IntWritable>{
        public ProxyReducerSeqTextInt() throws Exception { super(Text.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqTextLong extends GenericProxyReducerSeq <Text, LongWritable>{
        public ProxyReducerSeqTextLong() throws Exception { super(Text.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqTextText extends GenericProxyReducerSeq <Text, Text>{
        public ProxyReducerSeqTextText() throws Exception { super(Text.class, Text.class); }
    }
	public static class ProxyReducerSeqTextUTF8 extends GenericProxyReducerSeq <Text, UTF8>{
        public ProxyReducerSeqTextUTF8() throws Exception { super(Text.class, UTF8.class); }
    }
	public static class ProxyReducerSeqTextVInt extends GenericProxyReducerSeq <Text, VIntWritable>{
        public ProxyReducerSeqTextVInt() throws Exception { super(Text.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqTextVLong extends GenericProxyReducerSeq <Text, VLongWritable>{
        public ProxyReducerSeqTextVLong() throws Exception { super(Text.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqTextNull extends GenericProxyReducerSeq <Text, NullWritable>{
        public ProxyReducerSeqTextNull() throws Exception { super(Text.class, NullWritable.class); }
    }
	
	// Proxy Reducer UTF8
	public static class ProxyReducerSeqUTF8Double extends GenericProxyReducerSeq <UTF8, DoubleWritable>{
        public ProxyReducerSeqUTF8Double() throws Exception { super(UTF8.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqUTF8Float extends GenericProxyReducerSeq <UTF8, FloatWritable>{
        public ProxyReducerSeqUTF8Float() throws Exception { super(UTF8.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqUTF8Int extends GenericProxyReducerSeq <UTF8, IntWritable>{
        public ProxyReducerSeqUTF8Int() throws Exception { super(UTF8.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqUTF8Long extends GenericProxyReducerSeq <UTF8, LongWritable>{
        public ProxyReducerSeqUTF8Long() throws Exception { super(UTF8.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqUTF8Text extends GenericProxyReducerSeq <UTF8, Text>{
        public ProxyReducerSeqUTF8Text() throws Exception { super(UTF8.class, Text.class); }
    }
	public static class ProxyReducerSeqUTF8UTF8 extends GenericProxyReducerSeq <UTF8, UTF8>{
        public ProxyReducerSeqUTF8UTF8() throws Exception { super(UTF8.class, UTF8.class); }
    }
	public static class ProxyReducerSeqUTF8VInt extends GenericProxyReducerSeq <UTF8, VIntWritable>{
        public ProxyReducerSeqUTF8VInt() throws Exception { super(UTF8.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqUTF8VLong extends GenericProxyReducerSeq <UTF8, VLongWritable>{
        public ProxyReducerSeqUTF8VLong() throws Exception { super(UTF8.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqUTF8Null extends GenericProxyReducerSeq <UTF8, NullWritable>{
        public ProxyReducerSeqUTF8Null() throws Exception { super(UTF8.class, NullWritable.class); }
    }

	// Proxy Reducer VIntWritable
	public static class ProxyReducerSeqVIntDouble extends GenericProxyReducerSeq <VIntWritable, DoubleWritable>{
        public ProxyReducerSeqVIntDouble() throws Exception { super(VIntWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqVIntFloat extends GenericProxyReducerSeq <VIntWritable, FloatWritable>{
        public ProxyReducerSeqVIntFloat() throws Exception { super(VIntWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqVIntInt extends GenericProxyReducerSeq <VIntWritable, IntWritable>{
        public ProxyReducerSeqVIntInt() throws Exception { super(VIntWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqVIntLong extends GenericProxyReducerSeq <VIntWritable, LongWritable>{
        public ProxyReducerSeqVIntLong() throws Exception { super(VIntWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqVIntText extends GenericProxyReducerSeq <VIntWritable, Text>{
        public ProxyReducerSeqVIntText() throws Exception { super(VIntWritable.class, Text.class); }
    }
	public static class ProxyReducerSeqVIntUTF8 extends GenericProxyReducerSeq <VIntWritable, UTF8>{
        public ProxyReducerSeqVIntUTF8() throws Exception { super(VIntWritable.class, UTF8.class); }
    }
	public static class ProxyReducerSeqVIntVInt extends GenericProxyReducerSeq <VIntWritable, VIntWritable>{
        public ProxyReducerSeqVIntVInt() throws Exception { super(VIntWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqVIntVLong extends GenericProxyReducerSeq <VIntWritable, VLongWritable>{
        public ProxyReducerSeqVIntVLong() throws Exception { super(VIntWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqVIntNull extends GenericProxyReducerSeq <VIntWritable, NullWritable>{
        public ProxyReducerSeqVIntNull() throws Exception { super(VIntWritable.class, NullWritable.class); }
    }

	// Proxy Reducer VLongWritable
	public static class ProxyReducerSeqVLongDouble extends GenericProxyReducerSeq <VLongWritable, DoubleWritable>{
        public ProxyReducerSeqVLongDouble () throws Exception { super(VLongWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerSeqVLongFloat extends GenericProxyReducerSeq <VLongWritable, FloatWritable>{
        public ProxyReducerSeqVLongFloat () throws Exception { super(VLongWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerSeqVLongInt extends GenericProxyReducerSeq <VLongWritable, IntWritable>{
        public ProxyReducerSeqVLongInt () throws Exception { super(VLongWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerSeqVLongLong extends GenericProxyReducerSeq <VLongWritable, LongWritable>{
        public ProxyReducerSeqVLongLong () throws Exception { super(VLongWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerSeqVLongText extends GenericProxyReducerSeq <VLongWritable, Text>{
        public ProxyReducerSeqVLongText () throws Exception { super(VLongWritable.class, Text.class); }
    }
	public static class ProxyReducerSeqVLongUTF8 extends GenericProxyReducerSeq <VLongWritable, UTF8>{
        public ProxyReducerSeqVLongUTF8 () throws Exception { super(VLongWritable.class, UTF8.class); }
    }
	public static class ProxyReducerSeqVLongVInt extends GenericProxyReducerSeq <VLongWritable, VIntWritable>{
        public ProxyReducerSeqVLongVInt () throws Exception { super(VLongWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerSeqVLongVLong extends GenericProxyReducerSeq <VLongWritable, VLongWritable>{
        public ProxyReducerSeqVLongVLong () throws Exception { super(VLongWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerSeqVLongNull extends GenericProxyReducerSeq <VLongWritable, NullWritable>{
        public ProxyReducerSeqVLongNull () throws Exception { super(VLongWritable.class, NullWritable.class); }
    }
	
	// Proxy Reducer DoubleWritable
		public static class ProxyReducerSeqNullDouble extends GenericProxyReducerSeq <NullWritable, DoubleWritable>{
	        public ProxyReducerSeqNullDouble() throws Exception { super(NullWritable.class, DoubleWritable.class); }
	    }
		public static class ProxyReducerSeqNullFloat extends GenericProxyReducerSeq <NullWritable, FloatWritable>{
	        public ProxyReducerSeqNullFloat() throws Exception { super(NullWritable.class, FloatWritable.class); }
	    }
		public static class ProxyReducerSeqNullInt extends GenericProxyReducerSeq <NullWritable, IntWritable>{
	        public ProxyReducerSeqNullInt() throws Exception { super(NullWritable.class, IntWritable.class); }
	    }
		public static class ProxyReducerSeqNullLong extends GenericProxyReducerSeq <NullWritable, LongWritable>{
	        public ProxyReducerSeqNullLong() throws Exception { super(NullWritable.class, LongWritable.class); }
	    }
		public static class ProxyReducerSeqNullText extends GenericProxyReducerSeq <NullWritable, Text>{
	        public ProxyReducerSeqNullText() throws Exception { super(NullWritable.class, Text.class); }
	    }
		public static class ProxyReducerSeqNullUTF8 extends GenericProxyReducerSeq <NullWritable, UTF8>{
	        public ProxyReducerSeqNullUTF8() throws Exception { super(NullWritable.class, UTF8.class); }
	    }
		public static class ProxyReducerSeqNullVInt extends GenericProxyReducerSeq <NullWritable, VIntWritable>{
	        public ProxyReducerSeqNullVInt() throws Exception { super(NullWritable.class, VIntWritable.class); }
	    }
		public static class ProxyReducerSeqNullVLong extends GenericProxyReducerSeq <NullWritable, VLongWritable>{
	        public ProxyReducerSeqNullVLong() throws Exception { super(NullWritable.class, VLongWritable.class); }
	    }
		public static class ProxyReducerSeqNullNull extends GenericProxyReducerSeq <NullWritable, NullWritable>{
	        public ProxyReducerSeqNullNull() throws Exception { super(NullWritable.class, NullWritable.class); }
	    }

		public static class ProxyReducerSeqBooleanDouble extends GenericProxyReducerSeq <BooleanWritable, DoubleWritable>{
	        public ProxyReducerSeqBooleanDouble() throws Exception { super(BooleanWritable.class, DoubleWritable.class); }
	    }
		public static class ProxyReducerSeqBooleanFloat extends GenericProxyReducerSeq <BooleanWritable, FloatWritable>{
	        public ProxyReducerSeqBooleanFloat() throws Exception { super(BooleanWritable.class, FloatWritable.class); }
	    }
		public static class ProxyReducerSeqBooleanInt extends GenericProxyReducerSeq <BooleanWritable, IntWritable>{
	        public ProxyReducerSeqBooleanInt() throws Exception { super(BooleanWritable.class, IntWritable.class); }
	    }
		public static class ProxyReducerSeqBooleanLong extends GenericProxyReducerSeq <BooleanWritable, LongWritable>{
	        public ProxyReducerSeqBooleanLong() throws Exception { super(BooleanWritable.class, LongWritable.class); }
	    }
		public static class ProxyReducerSeqBooleanText extends GenericProxyReducerSeq <BooleanWritable, Text>{
	        public ProxyReducerSeqBooleanText() throws Exception { super(BooleanWritable.class, Text.class); }
	    }
		public static class ProxyReducerSeqBooleanUTF8 extends GenericProxyReducerSeq <BooleanWritable, UTF8>{
	        public ProxyReducerSeqBooleanUTF8() throws Exception { super(BooleanWritable.class, UTF8.class); }
	    }
		public static class ProxyReducerSeqBooleanVInt extends GenericProxyReducerSeq <BooleanWritable, VIntWritable>{
	        public ProxyReducerSeqBooleanVInt() throws Exception { super(BooleanWritable.class, VIntWritable.class); }
	    }
		public static class ProxyReducerSeqBooleanVLong extends GenericProxyReducerSeq <BooleanWritable, VLongWritable>{
	        public ProxyReducerSeqBooleanVLong() throws Exception { super(BooleanWritable.class, VLongWritable.class); }
	    }
		public static class ProxyReducerSeqBooleanBoolean extends GenericProxyReducerSeq <BooleanWritable, BooleanWritable>{
	        public ProxyReducerSeqBooleanBoolean() throws Exception { super(BooleanWritable.class, BooleanWritable.class); }
	    }


   
}


