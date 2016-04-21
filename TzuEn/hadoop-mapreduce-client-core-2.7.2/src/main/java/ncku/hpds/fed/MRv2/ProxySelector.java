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

public class ProxySelector {
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

    public ProxySelector(Configuration conf, Job job ) {
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
        System.out.println("Select ProxyMapper : " + clz.getCanonicalName() );
        return clz;
    }
    public Class< ? extends Mapper> getProxyMapperClassSeq(Class keyClz, Class valueClz) {
        Map<String, Class<? extends Mapper>> valueMap = proxyMapMappingSeq.get(keyClz.getCanonicalName()) ;
        Class<? extends Mapper> clz = valueMap.get(valueClz.getCanonicalName());
        System.out.println("Select ProxyMapper : " + clz.getCanonicalName() );
        return clz;
    }
    public Class< ? extends Reducer> getProxyReducerClass(Class keyClz, Class valueClz) {
        Map<String, Class<? extends Reducer>> valueMap = proxyReduceMapping.get(keyClz.getCanonicalName()) ;
        Class<? extends Reducer> clz = valueMap.get(valueClz.getCanonicalName());
        
        System.out.println("Select ProxyReducer : " + clz.getCanonicalName() );
        return clz;
    }
    //----------------------------------------------------------------------- 

    private void init() {
        // text, text 
        try {
            mJobInputFormatName = mJob.getInputFormatClass().getClass().getCanonicalName();
            // add proxy mapper mapping
            /*
            addProxyMapperTextKeyOutMapping();
            addProxyMapperIntKeyOutMapping();
            addProxyMapperLongKeyOutMapping();
            addProxyMapperFloatKeyOutMapping();
            */
            // add proxy reducer mapping
            /*
            addProxyReducerTextKeyOutMapping();
            addProxyReducerIntKeyOutMapping();
            addProxyReducerLongKeyOutMapping();
            addProxyReducerFloatKeyOutMapping();
            */
            /*
            if ( mCallback != null ) 
                mCallback.addProxyMapMapping(this.proxyMapMapping);
            */
            addProxyMappers();
            addProxyReducers();
        } catch ( Exception e ) {
        }
    }
    //----------------------------------------------------------------------- 
    //proxy map/reduce version2
	private void addProxyMappers(){
		addProxyMapperMapping( DoubleWritable.class, DoubleWritable.class, ProxyMapperDoubleDouble.class );
		addProxyMapperMapping( DoubleWritable.class, FloatWritable.class, ProxyMapperDoubleFloat.class );
		addProxyMapperMapping( DoubleWritable.class, IntWritable.class, ProxyMapperDoubleInt.class );
		addProxyMapperMapping( DoubleWritable.class, LongWritable.class, ProxyMapperDoubleLong.class );
		addProxyMapperMapping( DoubleWritable.class, Text.class, ProxyMapperDoubleText.class );
		addProxyMapperMapping( DoubleWritable.class, UTF8.class, ProxyMapperDoubleUTF8.class );
		addProxyMapperMapping( DoubleWritable.class, VIntWritable.class, ProxyMapperDoubleVInt.class );
		addProxyMapperMapping( DoubleWritable.class, VLongWritable.class, ProxyMapperDoubleVLong.class );
		addProxyMapperMapping( DoubleWritable.class, NullWritable.class, ProxyMapperDoubleNull.class );

		addProxyMapperMapping( FloatWritable.class, DoubleWritable.class, ProxyMapperFloatDouble.class );
		addProxyMapperMapping( FloatWritable.class, FloatWritable.class, ProxyMapperFloatFloat.class );
		addProxyMapperMapping( FloatWritable.class, IntWritable.class, ProxyMapperFloatInt.class );
		addProxyMapperMapping( FloatWritable.class, LongWritable.class, ProxyMapperFloatLong.class );
		addProxyMapperMapping( FloatWritable.class, Text.class, ProxyMapperFloatText.class );
		addProxyMapperMapping( FloatWritable.class, UTF8.class, ProxyMapperFloatUTF8.class );
		addProxyMapperMapping( FloatWritable.class, VIntWritable.class, ProxyMapperFloatVInt.class );
		addProxyMapperMapping( FloatWritable.class, VLongWritable.class, ProxyMapperFloatVLong.class );
		addProxyMapperMapping( FloatWritable.class, NullWritable.class, ProxyMapperFloatNull.class );

		addProxyMapperMapping( IntWritable.class, DoubleWritable.class, ProxyMapperIntDouble.class );
		addProxyMapperMapping( IntWritable.class, FloatWritable.class, ProxyMapperIntFloat.class );
		addProxyMapperMapping( IntWritable.class, IntWritable.class, ProxyMapperIntInt.class );
		addProxyMapperMapping( IntWritable.class, LongWritable.class, ProxyMapperIntLong.class );
		addProxyMapperMapping( IntWritable.class, Text.class, ProxyMapperIntText.class );
		addProxyMapperMapping( IntWritable.class, UTF8.class, ProxyMapperIntUTF8.class );
		addProxyMapperMapping( IntWritable.class, VIntWritable.class, ProxyMapperIntVInt.class );
		addProxyMapperMapping( IntWritable.class, VLongWritable.class, ProxyMapperIntVLong.class );
		addProxyMapperMapping( IntWritable.class, NullWritable.class, ProxyMapperIntNull.class );

		addProxyMapperMapping( LongWritable.class, DoubleWritable.class, ProxyMapperLongDouble.class );
		addProxyMapperMapping( LongWritable.class, FloatWritable.class, ProxyMapperLongFloat.class );
		addProxyMapperMapping( LongWritable.class, IntWritable.class, ProxyMapperLongInt.class );
		addProxyMapperMapping( LongWritable.class, LongWritable.class, ProxyMapperLongLong.class );
		addProxyMapperMapping( LongWritable.class, Text.class, ProxyMapperLongText.class );
		addProxyMapperMapping( LongWritable.class, UTF8.class, ProxyMapperLongUTF8.class );
		addProxyMapperMapping( LongWritable.class, VIntWritable.class, ProxyMapperLongVInt.class );
		addProxyMapperMapping( LongWritable.class, VLongWritable.class, ProxyMapperLongVLong.class );
		addProxyMapperMapping( LongWritable.class, NullWritable.class, ProxyMapperLongNull.class );

		addProxyMapperMapping( Text.class, DoubleWritable.class, ProxyMapperTextDouble.class );
		addProxyMapperMapping( Text.class, FloatWritable.class, ProxyMapperTextFloat.class );
		addProxyMapperMapping( Text.class, IntWritable.class, ProxyMapperTextInt.class );
		addProxyMapperMapping( Text.class, LongWritable.class, ProxyMapperTextLong.class );
		addProxyMapperMapping( Text.class, Text.class, ProxyMapperTextText.class );
		addProxyMapperMapping( Text.class, UTF8.class, ProxyMapperTextUTF8.class );
		addProxyMapperMapping( Text.class, VIntWritable.class, ProxyMapperTextVInt.class );
		addProxyMapperMapping( Text.class, VLongWritable.class, ProxyMapperTextVLong.class );
		addProxyMapperMapping( Text.class, NullWritable.class, ProxyMapperTextNull.class );

		addProxyMapperMapping( UTF8.class, DoubleWritable.class, ProxyMapperUTF8Double.class );
		addProxyMapperMapping( UTF8.class, FloatWritable.class, ProxyMapperUTF8Float.class );
		addProxyMapperMapping( UTF8.class, IntWritable.class, ProxyMapperUTF8Int.class );
		addProxyMapperMapping( UTF8.class, LongWritable.class, ProxyMapperUTF8Long.class );
		addProxyMapperMapping( UTF8.class, Text.class, ProxyMapperUTF8Text.class );
		addProxyMapperMapping( UTF8.class, UTF8.class, ProxyMapperUTF8UTF8.class );
		addProxyMapperMapping( UTF8.class, VIntWritable.class, ProxyMapperUTF8VInt.class );
		addProxyMapperMapping( UTF8.class, VLongWritable.class, ProxyMapperUTF8VLong.class );
		addProxyMapperMapping( UTF8.class, NullWritable.class, ProxyMapperUTF8Null.class );

		addProxyMapperMapping( VIntWritable.class, DoubleWritable.class, ProxyMapperVIntDouble.class );
		addProxyMapperMapping( VIntWritable.class, FloatWritable.class, ProxyMapperVIntFloat.class );
		addProxyMapperMapping( VIntWritable.class, IntWritable.class, ProxyMapperVIntInt.class );
		addProxyMapperMapping( VIntWritable.class, LongWritable.class, ProxyMapperVIntLong.class );
		addProxyMapperMapping( VIntWritable.class, Text.class, ProxyMapperVIntText.class );
		addProxyMapperMapping( VIntWritable.class, UTF8.class, ProxyMapperVIntUTF8.class );
		addProxyMapperMapping( VIntWritable.class, VIntWritable.class, ProxyMapperVIntVInt.class );
		addProxyMapperMapping( VIntWritable.class, VLongWritable.class, ProxyMapperVIntVLong.class );
		addProxyMapperMapping( VIntWritable.class, NullWritable.class, ProxyMapperVIntNull.class );

		addProxyMapperMapping( VLongWritable.class, DoubleWritable.class, ProxyMapperVLongDouble.class );
		addProxyMapperMapping( VLongWritable.class, FloatWritable.class, ProxyMapperVLongFloat.class );
		addProxyMapperMapping( VLongWritable.class, IntWritable.class, ProxyMapperVLongInt.class );
		addProxyMapperMapping( VLongWritable.class, LongWritable.class, ProxyMapperVLongLong.class );
		addProxyMapperMapping( VLongWritable.class, Text.class, ProxyMapperVLongText.class );
		addProxyMapperMapping( VLongWritable.class, UTF8.class, ProxyMapperVLongUTF8.class );
		addProxyMapperMapping( VLongWritable.class, VIntWritable.class, ProxyMapperVLongVInt.class );
		addProxyMapperMapping( VLongWritable.class, VLongWritable.class, ProxyMapperVLongVLong.class );
		addProxyMapperMapping( VLongWritable.class, NullWritable.class, ProxyMapperVLongNull.class );
		
		addProxyMapperMapping( NullWritable.class, DoubleWritable.class, ProxyMapperNullDouble.class );
		addProxyMapperMapping( NullWritable.class, FloatWritable.class, ProxyMapperNullFloat.class );
		addProxyMapperMapping( NullWritable.class, IntWritable.class, ProxyMapperNullInt.class );
		addProxyMapperMapping( NullWritable.class, LongWritable.class, ProxyMapperNullLong.class );
		addProxyMapperMapping( NullWritable.class, Text.class, ProxyMapperNullText.class );
		addProxyMapperMapping( NullWritable.class, UTF8.class, ProxyMapperNullUTF8.class );
		addProxyMapperMapping( NullWritable.class, VIntWritable.class, ProxyMapperNullVInt.class );
		addProxyMapperMapping( NullWritable.class, VLongWritable.class, ProxyMapperNullVLong.class );
		addProxyMapperMapping( NullWritable.class, NullWritable.class, ProxyMapperNullNull.class );


		addProxyMapperMappingSeq( DoubleWritable.class, DoubleWritable.class, ProxyMapperDoubleDoubleSeq.class );
		addProxyMapperMappingSeq( DoubleWritable.class, FloatWritable.class, ProxyMapperDoubleFloatSeq.class );
		addProxyMapperMappingSeq( DoubleWritable.class, IntWritable.class, ProxyMapperDoubleIntSeq.class );
		addProxyMapperMappingSeq( DoubleWritable.class, LongWritable.class, ProxyMapperDoubleLongSeq.class );
		addProxyMapperMappingSeq( DoubleWritable.class, Text.class, ProxyMapperDoubleTextSeq.class );
		addProxyMapperMappingSeq( DoubleWritable.class, UTF8.class, ProxyMapperDoubleUTF8Seq.class );
		addProxyMapperMappingSeq( DoubleWritable.class, VIntWritable.class, ProxyMapperDoubleVIntSeq.class );
		addProxyMapperMappingSeq( DoubleWritable.class, VLongWritable.class, ProxyMapperDoubleVLongSeq.class );
		addProxyMapperMappingSeq( FloatWritable.class, DoubleWritable.class, ProxyMapperFloatDoubleSeq.class );
		addProxyMapperMappingSeq( FloatWritable.class, FloatWritable.class, ProxyMapperFloatFloatSeq.class );
		addProxyMapperMappingSeq( FloatWritable.class, IntWritable.class, ProxyMapperFloatIntSeq.class );
		addProxyMapperMappingSeq( FloatWritable.class, LongWritable.class, ProxyMapperFloatLongSeq.class );
		addProxyMapperMappingSeq( FloatWritable.class, Text.class, ProxyMapperFloatTextSeq.class );
		addProxyMapperMappingSeq( FloatWritable.class, UTF8.class, ProxyMapperFloatUTF8Seq.class );
		addProxyMapperMappingSeq( FloatWritable.class, VIntWritable.class, ProxyMapperFloatVIntSeq.class );
		addProxyMapperMappingSeq( FloatWritable.class, VLongWritable.class, ProxyMapperFloatVLongSeq.class );
		addProxyMapperMappingSeq( IntWritable.class, DoubleWritable.class, ProxyMapperIntDoubleSeq.class );
		addProxyMapperMappingSeq( IntWritable.class, FloatWritable.class, ProxyMapperIntFloatSeq.class );
		addProxyMapperMappingSeq( IntWritable.class, IntWritable.class, ProxyMapperIntIntSeq.class );
		addProxyMapperMappingSeq( IntWritable.class, LongWritable.class, ProxyMapperIntLongSeq.class );
		addProxyMapperMappingSeq( IntWritable.class, Text.class, ProxyMapperIntTextSeq.class );
		addProxyMapperMappingSeq( IntWritable.class, UTF8.class, ProxyMapperIntUTF8Seq.class );
		addProxyMapperMappingSeq( IntWritable.class, VIntWritable.class, ProxyMapperIntVIntSeq.class );
		addProxyMapperMappingSeq( IntWritable.class, VLongWritable.class, ProxyMapperIntVLongSeq.class );
		addProxyMapperMappingSeq( LongWritable.class, DoubleWritable.class, ProxyMapperLongDoubleSeq.class );
		addProxyMapperMappingSeq( LongWritable.class, FloatWritable.class, ProxyMapperLongFloatSeq.class );
		addProxyMapperMappingSeq( LongWritable.class, IntWritable.class, ProxyMapperLongIntSeq.class );
		addProxyMapperMappingSeq( LongWritable.class, LongWritable.class, ProxyMapperLongLongSeq.class );
		addProxyMapperMappingSeq( LongWritable.class, Text.class, ProxyMapperLongTextSeq.class );
		addProxyMapperMappingSeq( LongWritable.class, UTF8.class, ProxyMapperLongUTF8Seq.class );
		addProxyMapperMappingSeq( LongWritable.class, VIntWritable.class, ProxyMapperLongVIntSeq.class );
		addProxyMapperMappingSeq( LongWritable.class, VLongWritable.class, ProxyMapperLongVLongSeq.class );
		addProxyMapperMappingSeq( Text.class, DoubleWritable.class, ProxyMapperTextDoubleSeq.class );
		addProxyMapperMappingSeq( Text.class, FloatWritable.class, ProxyMapperTextFloatSeq.class );
		addProxyMapperMappingSeq( Text.class, IntWritable.class, ProxyMapperTextIntSeq.class );
		addProxyMapperMappingSeq( Text.class, LongWritable.class, ProxyMapperTextLongSeq.class );
		addProxyMapperMappingSeq( Text.class, Text.class, ProxyMapperTextTextSeq.class );
		addProxyMapperMappingSeq( Text.class, UTF8.class, ProxyMapperTextUTF8Seq.class );
		addProxyMapperMappingSeq( Text.class, VIntWritable.class, ProxyMapperTextVIntSeq.class );
		addProxyMapperMappingSeq( Text.class, VLongWritable.class, ProxyMapperTextVLongSeq.class );
		addProxyMapperMappingSeq( UTF8.class, DoubleWritable.class, ProxyMapperUTF8DoubleSeq.class );
		addProxyMapperMappingSeq( UTF8.class, FloatWritable.class, ProxyMapperUTF8FloatSeq.class );
		addProxyMapperMappingSeq( UTF8.class, IntWritable.class, ProxyMapperUTF8IntSeq.class );
		addProxyMapperMappingSeq( UTF8.class, LongWritable.class, ProxyMapperUTF8LongSeq.class );
		addProxyMapperMappingSeq( UTF8.class, Text.class, ProxyMapperUTF8TextSeq.class );
		addProxyMapperMappingSeq( UTF8.class, UTF8.class, ProxyMapperUTF8UTF8Seq.class );
		addProxyMapperMappingSeq( UTF8.class, VIntWritable.class, ProxyMapperUTF8VIntSeq.class );
		addProxyMapperMappingSeq( UTF8.class, VLongWritable.class, ProxyMapperUTF8VLongSeq.class );
		addProxyMapperMappingSeq( VIntWritable.class, DoubleWritable.class, ProxyMapperVIntDoubleSeq.class );
		addProxyMapperMappingSeq( VIntWritable.class, FloatWritable.class, ProxyMapperVIntFloatSeq.class );
		addProxyMapperMappingSeq( VIntWritable.class, IntWritable.class, ProxyMapperVIntIntSeq.class );
		addProxyMapperMappingSeq( VIntWritable.class, LongWritable.class, ProxyMapperVIntLongSeq.class );
		addProxyMapperMappingSeq( VIntWritable.class, Text.class, ProxyMapperVIntTextSeq.class );
		addProxyMapperMappingSeq( VIntWritable.class, UTF8.class, ProxyMapperVIntUTF8Seq.class );
		addProxyMapperMappingSeq( VIntWritable.class, VIntWritable.class, ProxyMapperVIntVIntSeq.class );
		addProxyMapperMappingSeq( VIntWritable.class, VLongWritable.class, ProxyMapperVIntVLongSeq.class );
		addProxyMapperMappingSeq( VLongWritable.class, DoubleWritable.class, ProxyMapperVLongDoubleSeq.class );
		addProxyMapperMappingSeq( VLongWritable.class, FloatWritable.class, ProxyMapperVLongFloatSeq.class );
		addProxyMapperMappingSeq( VLongWritable.class, IntWritable.class, ProxyMapperVLongIntSeq.class );
		addProxyMapperMappingSeq( VLongWritable.class, LongWritable.class, ProxyMapperVLongLongSeq.class );
		addProxyMapperMappingSeq( VLongWritable.class, Text.class, ProxyMapperVLongTextSeq.class );
		addProxyMapperMappingSeq( VLongWritable.class, UTF8.class, ProxyMapperVLongUTF8Seq.class );
		addProxyMapperMappingSeq( VLongWritable.class, VIntWritable.class, ProxyMapperVLongVIntSeq.class );
		addProxyMapperMappingSeq( VLongWritable.class, VLongWritable.class, ProxyMapperVLongVLongSeq.class );
	}

	private void addProxyReducers(){
		addProxyReducerMapping( DoubleWritable.class, DoubleWritable.class, ProxyReducerDoubleDouble.class );
		addProxyReducerMapping( DoubleWritable.class, FloatWritable.class, ProxyReducerDoubleFloat.class );
		addProxyReducerMapping( DoubleWritable.class, IntWritable.class, ProxyReducerDoubleInt.class );
		addProxyReducerMapping( DoubleWritable.class, LongWritable.class, ProxyReducerDoubleLong.class );
		addProxyReducerMapping( DoubleWritable.class, Text.class, ProxyReducerDoubleText.class );
		addProxyReducerMapping( DoubleWritable.class, UTF8.class, ProxyReducerDoubleUTF8.class );
		addProxyReducerMapping( DoubleWritable.class, VIntWritable.class, ProxyReducerDoubleVInt.class );
		addProxyReducerMapping( DoubleWritable.class, VLongWritable.class, ProxyReducerDoubleVLong.class );
		addProxyReducerMapping( DoubleWritable.class, NullWritable.class, ProxyReducerDoubleNull.class );

		addProxyReducerMapping( FloatWritable.class, DoubleWritable.class, ProxyReducerFloatDouble.class );
		addProxyReducerMapping( FloatWritable.class, FloatWritable.class, ProxyReducerFloatFloat.class );
		addProxyReducerMapping( FloatWritable.class, IntWritable.class, ProxyReducerFloatInt.class );
		addProxyReducerMapping( FloatWritable.class, LongWritable.class, ProxyReducerFloatLong.class );
		addProxyReducerMapping( FloatWritable.class, Text.class, ProxyReducerFloatText.class );
		addProxyReducerMapping( FloatWritable.class, UTF8.class, ProxyReducerFloatUTF8.class );
		addProxyReducerMapping( FloatWritable.class, VIntWritable.class, ProxyReducerFloatVInt.class );
		addProxyReducerMapping( FloatWritable.class, VLongWritable.class, ProxyReducerFloatVLong.class );
		addProxyReducerMapping( FloatWritable.class, NullWritable.class, ProxyReducerFloatNull.class );

		addProxyReducerMapping( IntWritable.class, DoubleWritable.class, ProxyReducerIntDouble.class );
		addProxyReducerMapping( IntWritable.class, FloatWritable.class, ProxyReducerIntFloat.class );
		addProxyReducerMapping( IntWritable.class, IntWritable.class, ProxyReducerIntInt.class );
		addProxyReducerMapping( IntWritable.class, LongWritable.class, ProxyReducerIntLong.class );
		addProxyReducerMapping( IntWritable.class, Text.class, ProxyReducerIntText.class );
		addProxyReducerMapping( IntWritable.class, UTF8.class, ProxyReducerIntUTF8.class );
		addProxyReducerMapping( IntWritable.class, VIntWritable.class, ProxyReducerIntVInt.class );
		addProxyReducerMapping( IntWritable.class, VLongWritable.class, ProxyReducerIntVLong.class );
		addProxyReducerMapping( IntWritable.class, NullWritable.class, ProxyReducerIntNull.class );

		addProxyReducerMapping( LongWritable.class, DoubleWritable.class, ProxyReducerLongDouble.class );
		addProxyReducerMapping( LongWritable.class, FloatWritable.class, ProxyReducerLongFloat.class );
		addProxyReducerMapping( LongWritable.class, IntWritable.class, ProxyReducerLongInt.class );
		addProxyReducerMapping( LongWritable.class, LongWritable.class, ProxyReducerLongLong.class );
		addProxyReducerMapping( LongWritable.class, Text.class, ProxyReducerLongText.class );
		addProxyReducerMapping( LongWritable.class, UTF8.class, ProxyReducerLongUTF8.class );
		addProxyReducerMapping( LongWritable.class, VIntWritable.class, ProxyReducerLongVInt.class );
		addProxyReducerMapping( LongWritable.class, VLongWritable.class, ProxyReducerLongVLong.class );
		addProxyReducerMapping( LongWritable.class, NullWritable.class, ProxyReducerLongNull.class );

		addProxyReducerMapping( Text.class, DoubleWritable.class, ProxyReducerTextDouble.class );
		addProxyReducerMapping( Text.class, FloatWritable.class, ProxyReducerTextFloat.class );
		addProxyReducerMapping( Text.class, IntWritable.class, ProxyReducerTextInt.class );
		addProxyReducerMapping( Text.class, LongWritable.class, ProxyReducerTextLong.class );
		addProxyReducerMapping( Text.class, Text.class, ProxyReducerTextText.class );
		addProxyReducerMapping( Text.class, UTF8.class, ProxyReducerTextUTF8.class );
		addProxyReducerMapping( Text.class, VIntWritable.class, ProxyReducerTextVInt.class );
		addProxyReducerMapping( Text.class, VLongWritable.class, ProxyReducerTextVLong.class );
		addProxyReducerMapping( Text.class, NullWritable.class, ProxyReducerTextNull.class );

		addProxyReducerMapping( UTF8.class, DoubleWritable.class, ProxyReducerUTF8Double.class );
		addProxyReducerMapping( UTF8.class, FloatWritable.class, ProxyReducerUTF8Float.class );
		addProxyReducerMapping( UTF8.class, IntWritable.class, ProxyReducerUTF8Int.class );
		addProxyReducerMapping( UTF8.class, LongWritable.class, ProxyReducerUTF8Long.class );
		addProxyReducerMapping( UTF8.class, Text.class, ProxyReducerUTF8Text.class );
		addProxyReducerMapping( UTF8.class, UTF8.class, ProxyReducerUTF8UTF8.class );
		addProxyReducerMapping( UTF8.class, VIntWritable.class, ProxyReducerUTF8VInt.class );
		addProxyReducerMapping( UTF8.class, VLongWritable.class, ProxyReducerUTF8VLong.class );
		addProxyReducerMapping( UTF8.class, NullWritable.class, ProxyReducerUTF8Null.class );

		addProxyReducerMapping( VIntWritable.class, DoubleWritable.class, ProxyReducerVIntDouble.class );
		addProxyReducerMapping( VIntWritable.class, FloatWritable.class, ProxyReducerVIntFloat.class );
		addProxyReducerMapping( VIntWritable.class, IntWritable.class, ProxyReducerVIntInt.class );
		addProxyReducerMapping( VIntWritable.class, LongWritable.class, ProxyReducerVIntLong.class );
		addProxyReducerMapping( VIntWritable.class, Text.class, ProxyReducerVIntText.class );
		addProxyReducerMapping( VIntWritable.class, UTF8.class, ProxyReducerVIntUTF8.class );
		addProxyReducerMapping( VIntWritable.class, VIntWritable.class, ProxyReducerVIntVInt.class );
		addProxyReducerMapping( VIntWritable.class, VLongWritable.class, ProxyReducerVIntVLong.class );
		addProxyReducerMapping( VIntWritable.class, NullWritable.class, ProxyReducerVIntNull.class );

		addProxyReducerMapping( VLongWritable.class, DoubleWritable.class, ProxyReducerVLongDouble.class );
		addProxyReducerMapping( VLongWritable.class, FloatWritable.class, ProxyReducerVLongFloat.class );
		addProxyReducerMapping( VLongWritable.class, IntWritable.class, ProxyReducerVLongInt.class );
		addProxyReducerMapping( VLongWritable.class, LongWritable.class, ProxyReducerVLongLong.class );
		addProxyReducerMapping( VLongWritable.class, Text.class, ProxyReducerVLongText.class );
		addProxyReducerMapping( VLongWritable.class, UTF8.class, ProxyReducerVLongUTF8.class );
		addProxyReducerMapping( VLongWritable.class, VIntWritable.class, ProxyReducerVLongVInt.class );
		addProxyReducerMapping( VLongWritable.class, VLongWritable.class, ProxyReducerVLongVLong.class );
		addProxyReducerMapping( VLongWritable.class, NullWritable.class, ProxyReducerVLongNull.class );
		
		addProxyReducerMapping( NullWritable.class, DoubleWritable.class, ProxyReducerNullDouble.class );
		addProxyReducerMapping( NullWritable.class, FloatWritable.class, ProxyReducerNullFloat.class );
		addProxyReducerMapping( NullWritable.class, IntWritable.class, ProxyReducerNullInt.class );
		addProxyReducerMapping( NullWritable.class, LongWritable.class, ProxyReducerNullLong.class );
		addProxyReducerMapping( NullWritable.class, Text.class, ProxyReducerNullText.class );
		addProxyReducerMapping( NullWritable.class, UTF8.class, ProxyReducerNullUTF8.class );
		addProxyReducerMapping( NullWritable.class, VIntWritable.class, ProxyReducerNullVInt.class );
		addProxyReducerMapping( NullWritable.class, VLongWritable.class, ProxyReducerNullVLong.class );
		addProxyReducerMapping( NullWritable.class, NullWritable.class, ProxyReducerNullNull.class );

	}
    //----------------------------------------------------------------------- 
    public void addProxyMapperMapping( Class keyClz, Class valueClz, Class<? extends Mapper> pmClz ) {
        Map<String, Class<? extends Mapper>> valueMap = proxyMapMapping.get(keyClz.getCanonicalName());
        if ( valueMap == null ) {
            valueMap = new HashMap<String, Class<? extends Mapper>>();
            proxyMapMapping.put(keyClz.getCanonicalName(), valueMap);
        }
        valueMap.put( valueClz.getCanonicalName(), pmClz );
    }
    public void addProxyMapperMappingSeq( Class keyClz, Class valueClz, Class<? extends Mapper> pmClz ) {
        Map<String, Class<? extends Mapper>> valueMap = proxyMapMappingSeq.get(keyClz.getCanonicalName());
        if ( valueMap == null ) {
            valueMap = new HashMap<String, Class<? extends Mapper>>();
            proxyMapMappingSeq.put(keyClz.getCanonicalName(), valueMap);
        }
        valueMap.put( valueClz.getCanonicalName(), pmClz );
    }
    public void addProxyReducerMapping( Class keyClz, Class valueClz, Class<? extends Reducer> pmClz ) {
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
	public static class ProxyMapperDoubleDouble extends GenericProxyMapper <DoubleWritable,DoubleWritable>{
		public ProxyMapperDoubleDouble() throws Exception { super(DoubleWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperDoubleFloat extends GenericProxyMapper <DoubleWritable,FloatWritable>{
		public ProxyMapperDoubleFloat() throws Exception { super(DoubleWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperDoubleInt extends GenericProxyMapper <DoubleWritable,IntWritable>{
		public ProxyMapperDoubleInt() throws Exception { super(DoubleWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperDoubleLong extends GenericProxyMapper <DoubleWritable,LongWritable>{
		public ProxyMapperDoubleLong() throws Exception { super(DoubleWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperDoubleText extends GenericProxyMapper <DoubleWritable,Text>{
		public ProxyMapperDoubleText() throws Exception { super(DoubleWritable.class,Text.class); }
	}
	public static class ProxyMapperDoubleUTF8 extends GenericProxyMapper <DoubleWritable,UTF8>{
		public ProxyMapperDoubleUTF8() throws Exception { super(DoubleWritable.class,UTF8.class); }
	}
	public static class ProxyMapperDoubleVInt extends GenericProxyMapper <DoubleWritable,VIntWritable>{
		public ProxyMapperDoubleVInt() throws Exception { super(DoubleWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperDoubleVLong extends GenericProxyMapper <DoubleWritable,VLongWritable>{
		public ProxyMapperDoubleVLong() throws Exception { super(DoubleWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperDoubleNull extends GenericProxyMapper <DoubleWritable,NullWritable>{
		public ProxyMapperDoubleNull() throws Exception { super(DoubleWritable.class,NullWritable.class); }
	}

	// Proxy Mapper FloatWritable
	public static class ProxyMapperFloatDouble extends GenericProxyMapper <FloatWritable,DoubleWritable>{
		public ProxyMapperFloatDouble() throws Exception { super(FloatWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperFloatFloat extends GenericProxyMapper <FloatWritable,FloatWritable>{
		public ProxyMapperFloatFloat() throws Exception { super(FloatWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperFloatInt extends GenericProxyMapper <FloatWritable,IntWritable>{
		public ProxyMapperFloatInt() throws Exception { super(FloatWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperFloatLong extends GenericProxyMapper <FloatWritable,LongWritable>{
		public ProxyMapperFloatLong() throws Exception { super(FloatWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperFloatText extends GenericProxyMapper <FloatWritable,Text>{
		public ProxyMapperFloatText() throws Exception { super(FloatWritable.class,Text.class); }
	}
	public static class ProxyMapperFloatUTF8 extends GenericProxyMapper <FloatWritable,UTF8>{
		public ProxyMapperFloatUTF8() throws Exception { super(FloatWritable.class,UTF8.class); }
	}
	public static class ProxyMapperFloatVInt extends GenericProxyMapper <FloatWritable,VIntWritable>{
		public ProxyMapperFloatVInt() throws Exception { super(FloatWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperFloatVLong extends GenericProxyMapper <FloatWritable,VLongWritable>{
		public ProxyMapperFloatVLong() throws Exception { super(FloatWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperFloatNull extends GenericProxyMapper <FloatWritable,NullWritable>{
		public ProxyMapperFloatNull() throws Exception { super(FloatWritable.class,NullWritable.class); }
	}

	// Proxy Mapper IntWritable
	public static class ProxyMapperIntDouble extends GenericProxyMapper <IntWritable,DoubleWritable>{
		public ProxyMapperIntDouble() throws Exception { super(IntWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperIntFloat extends GenericProxyMapper <IntWritable,FloatWritable>{
		public ProxyMapperIntFloat() throws Exception { super(IntWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperIntInt extends GenericProxyMapper <IntWritable,IntWritable>{
		public ProxyMapperIntInt() throws Exception { super(IntWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperIntLong extends GenericProxyMapper <IntWritable,LongWritable>{
		public ProxyMapperIntLong() throws Exception { super(IntWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperIntText extends GenericProxyMapper <IntWritable,Text>{
		public ProxyMapperIntText() throws Exception { super(IntWritable.class,Text.class); }
	}
	public static class ProxyMapperIntUTF8 extends GenericProxyMapper <IntWritable,UTF8>{
		public ProxyMapperIntUTF8() throws Exception { super(IntWritable.class,UTF8.class); }
	}
	public static class ProxyMapperIntVInt extends GenericProxyMapper <IntWritable,VIntWritable>{
		public ProxyMapperIntVInt() throws Exception { super(IntWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperIntVLong extends GenericProxyMapper <IntWritable,VLongWritable>{
		public ProxyMapperIntVLong() throws Exception { super(IntWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperIntNull extends GenericProxyMapper <IntWritable,NullWritable>{
		public ProxyMapperIntNull() throws Exception { super(IntWritable.class,NullWritable.class); }
	}

	// Proxy Mapper LongWritable
	public static class ProxyMapperLongDouble extends GenericProxyMapper <LongWritable,DoubleWritable>{
		public ProxyMapperLongDouble() throws Exception { super(LongWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperLongFloat extends GenericProxyMapper <LongWritable,FloatWritable>{
		public ProxyMapperLongFloat() throws Exception { super(LongWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperLongInt extends GenericProxyMapper <LongWritable,IntWritable>{
		public ProxyMapperLongInt() throws Exception { super(LongWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperLongLong extends GenericProxyMapper <LongWritable,LongWritable>{
		public ProxyMapperLongLong() throws Exception { super(LongWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperLongText extends GenericProxyMapper <LongWritable,Text>{
		public ProxyMapperLongText() throws Exception { super(LongWritable.class,Text.class); }
	}
	public static class ProxyMapperLongUTF8 extends GenericProxyMapper <LongWritable,UTF8>{
		public ProxyMapperLongUTF8() throws Exception { super(LongWritable.class,UTF8.class); }
	}
	public static class ProxyMapperLongVInt extends GenericProxyMapper <LongWritable,VIntWritable>{
		public ProxyMapperLongVInt() throws Exception { super(LongWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperLongVLong extends GenericProxyMapper <LongWritable,VLongWritable>{
		public ProxyMapperLongVLong() throws Exception { super(LongWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperLongNull extends GenericProxyMapper <LongWritable,NullWritable>{
		public ProxyMapperLongNull() throws Exception { super(LongWritable.class,NullWritable.class); }
	}

	// Proxy Mapper Text
	public static class ProxyMapperTextDouble extends GenericProxyMapper <Text,DoubleWritable>{
		public ProxyMapperTextDouble() throws Exception { super(Text.class,DoubleWritable.class); }
	}
	public static class ProxyMapperTextFloat extends GenericProxyMapper <Text,FloatWritable>{
		public ProxyMapperTextFloat() throws Exception { super(Text.class,FloatWritable.class); }
	}
	public static class ProxyMapperTextInt extends GenericProxyMapper <Text,IntWritable>{
		public ProxyMapperTextInt() throws Exception { super(Text.class,IntWritable.class); }
	}
	public static class ProxyMapperTextLong extends GenericProxyMapper <Text,LongWritable>{
		public ProxyMapperTextLong() throws Exception { super(Text.class,LongWritable.class); }
	}
	public static class ProxyMapperTextText extends GenericProxyMapper <Text,Text>{
		public ProxyMapperTextText() throws Exception { super(Text.class,Text.class); }
	}
	public static class ProxyMapperTextUTF8 extends GenericProxyMapper <Text,UTF8>{
		public ProxyMapperTextUTF8() throws Exception { super(Text.class,UTF8.class); }
	}
	public static class ProxyMapperTextVInt extends GenericProxyMapper <Text,VIntWritable>{
		public ProxyMapperTextVInt() throws Exception { super(Text.class,VIntWritable.class); }
	}
	public static class ProxyMapperTextVLong extends GenericProxyMapper <Text,VLongWritable>{
		public ProxyMapperTextVLong() throws Exception { super(Text.class,VLongWritable.class); }
	}
	public static class ProxyMapperTextNull extends GenericProxyMapper <Text,NullWritable>{
		public ProxyMapperTextNull() throws Exception { super(Text.class,NullWritable.class); }
	}

	// Proxy Mapper UTF8
	public static class ProxyMapperUTF8Double extends GenericProxyMapper <UTF8,DoubleWritable>{
		public ProxyMapperUTF8Double() throws Exception { super(UTF8.class,DoubleWritable.class); }
	}
	public static class ProxyMapperUTF8Float extends GenericProxyMapper <UTF8,FloatWritable>{
		public ProxyMapperUTF8Float() throws Exception { super(UTF8.class,FloatWritable.class); }
	}
	public static class ProxyMapperUTF8Int extends GenericProxyMapper <UTF8,IntWritable>{
		public ProxyMapperUTF8Int() throws Exception { super(UTF8.class,IntWritable.class); }
	}
	public static class ProxyMapperUTF8Long extends GenericProxyMapper <UTF8,LongWritable>{
		public ProxyMapperUTF8Long() throws Exception { super(UTF8.class,LongWritable.class); }
	}
	public static class ProxyMapperUTF8Text extends GenericProxyMapper <UTF8,Text>{
		public ProxyMapperUTF8Text() throws Exception { super(UTF8.class,Text.class); }
	}
	public static class ProxyMapperUTF8UTF8 extends GenericProxyMapper <UTF8,UTF8>{
		public ProxyMapperUTF8UTF8() throws Exception { super(UTF8.class,UTF8.class); }
	}
	public static class ProxyMapperUTF8VInt extends GenericProxyMapper <UTF8,VIntWritable>{
		public ProxyMapperUTF8VInt() throws Exception { super(UTF8.class,VIntWritable.class); }
	}
	public static class ProxyMapperUTF8VLong extends GenericProxyMapper <UTF8,VLongWritable>{
		public ProxyMapperUTF8VLong() throws Exception { super(UTF8.class,VLongWritable.class); }
	}
	public static class ProxyMapperUTF8Null extends GenericProxyMapper <UTF8,NullWritable>{
		public ProxyMapperUTF8Null() throws Exception { super(UTF8.class,NullWritable.class); }
	}

	// Proxy Mapper VIntWritable
	public static class ProxyMapperVIntDouble extends GenericProxyMapper <VIntWritable,DoubleWritable>{
		public ProxyMapperVIntDouble() throws Exception { super(VIntWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperVIntFloat extends GenericProxyMapper <VIntWritable,FloatWritable>{
		public ProxyMapperVIntFloat() throws Exception { super(VIntWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperVIntInt extends GenericProxyMapper <VIntWritable,IntWritable>{
		public ProxyMapperVIntInt() throws Exception { super(VIntWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperVIntLong extends GenericProxyMapper <VIntWritable,LongWritable>{
		public ProxyMapperVIntLong() throws Exception { super(VIntWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperVIntText extends GenericProxyMapper <VIntWritable,Text>{
		public ProxyMapperVIntText() throws Exception { super(VIntWritable.class,Text.class); }
	}
	public static class ProxyMapperVIntUTF8 extends GenericProxyMapper <VIntWritable,UTF8>{
		public ProxyMapperVIntUTF8() throws Exception { super(VIntWritable.class,UTF8.class); }
	}
	public static class ProxyMapperVIntVInt extends GenericProxyMapper <VIntWritable,VIntWritable>{
		public ProxyMapperVIntVInt() throws Exception { super(VIntWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperVIntVLong extends GenericProxyMapper <VIntWritable,VLongWritable>{
		public ProxyMapperVIntVLong() throws Exception { super(VIntWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperVIntNull extends GenericProxyMapper <VIntWritable,NullWritable>{
		public ProxyMapperVIntNull() throws Exception { super(VIntWritable.class,NullWritable.class); }
	}

	// Proxy Mapper VLongWritable
	public static class ProxyMapperVLongDouble extends GenericProxyMapper <VLongWritable,DoubleWritable>{
		public ProxyMapperVLongDouble() throws Exception { super(VLongWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperVLongFloat extends GenericProxyMapper <VLongWritable,FloatWritable>{
		public ProxyMapperVLongFloat() throws Exception { super(VLongWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperVLongInt extends GenericProxyMapper <VLongWritable,IntWritable>{
		public ProxyMapperVLongInt() throws Exception { super(VLongWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperVLongLong extends GenericProxyMapper <VLongWritable,LongWritable>{
		public ProxyMapperVLongLong() throws Exception { super(VLongWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperVLongText extends GenericProxyMapper <VLongWritable,Text>{
		public ProxyMapperVLongText() throws Exception { super(VLongWritable.class,Text.class); }
	}
	public static class ProxyMapperVLongUTF8 extends GenericProxyMapper <VLongWritable,UTF8>{
		public ProxyMapperVLongUTF8() throws Exception { super(VLongWritable.class,UTF8.class); }
	}
	public static class ProxyMapperVLongVInt extends GenericProxyMapper <VLongWritable,VIntWritable>{
		public ProxyMapperVLongVInt() throws Exception { super(VLongWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperVLongVLong extends GenericProxyMapper <VLongWritable,VLongWritable>{
		public ProxyMapperVLongVLong() throws Exception { super(VLongWritable.class,VLongWritable.class); }
	}
	public static class ProxyMapperVLongNull extends GenericProxyMapper <VLongWritable,NullWritable>{
		public ProxyMapperVLongNull() throws Exception { super(VLongWritable.class,NullWritable.class); }
	}
    //--------------------------------------------------------------------------------------------------------
    // proxy mapper for sequencefile
	// Proxy Mapper DoubleWritable
	public static class ProxyMapperDoubleDoubleSeq extends GenericProxyMapperSeq <DoubleWritable,DoubleWritable>{
		public ProxyMapperDoubleDoubleSeq() throws Exception { super(DoubleWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperDoubleFloatSeq extends GenericProxyMapperSeq <DoubleWritable,FloatWritable>{
		public ProxyMapperDoubleFloatSeq() throws Exception { super(DoubleWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperDoubleIntSeq extends GenericProxyMapperSeq <DoubleWritable,IntWritable>{
		public ProxyMapperDoubleIntSeq() throws Exception { super(DoubleWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperDoubleLongSeq extends GenericProxyMapperSeq <DoubleWritable,LongWritable>{
		public ProxyMapperDoubleLongSeq() throws Exception { super(DoubleWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperDoubleTextSeq extends GenericProxyMapperSeq <DoubleWritable,Text>{
		public ProxyMapperDoubleTextSeq() throws Exception { super(DoubleWritable.class,Text.class); }
	}
	public static class ProxyMapperDoubleUTF8Seq extends GenericProxyMapperSeq <DoubleWritable,UTF8>{
		public ProxyMapperDoubleUTF8Seq() throws Exception { super(DoubleWritable.class,UTF8.class); }
	}
	public static class ProxyMapperDoubleVIntSeq extends GenericProxyMapperSeq <DoubleWritable,VIntWritable>{
		public ProxyMapperDoubleVIntSeq() throws Exception { super(DoubleWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperDoubleVLongSeq extends GenericProxyMapperSeq <DoubleWritable,VLongWritable>{
		public ProxyMapperDoubleVLongSeq() throws Exception { super(DoubleWritable.class,VLongWritable.class); }
	}

	// Proxy Mapper FloatWritable
	public static class ProxyMapperFloatDoubleSeq extends GenericProxyMapperSeq <FloatWritable,DoubleWritable>{
		public ProxyMapperFloatDoubleSeq() throws Exception { super(FloatWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperFloatFloatSeq extends GenericProxyMapperSeq <FloatWritable,FloatWritable>{
		public ProxyMapperFloatFloatSeq() throws Exception { super(FloatWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperFloatIntSeq extends GenericProxyMapperSeq <FloatWritable,IntWritable>{
		public ProxyMapperFloatIntSeq() throws Exception { super(FloatWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperFloatLongSeq extends GenericProxyMapperSeq <FloatWritable,LongWritable>{
		public ProxyMapperFloatLongSeq() throws Exception { super(FloatWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperFloatTextSeq extends GenericProxyMapperSeq <FloatWritable,Text>{
		public ProxyMapperFloatTextSeq() throws Exception { super(FloatWritable.class,Text.class); }
	}
	public static class ProxyMapperFloatUTF8Seq extends GenericProxyMapperSeq <FloatWritable,UTF8>{
		public ProxyMapperFloatUTF8Seq() throws Exception { super(FloatWritable.class,UTF8.class); }
	}
	public static class ProxyMapperFloatVIntSeq extends GenericProxyMapperSeq <FloatWritable,VIntWritable>{
		public ProxyMapperFloatVIntSeq() throws Exception { super(FloatWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperFloatVLongSeq extends GenericProxyMapperSeq <FloatWritable,VLongWritable>{
		public ProxyMapperFloatVLongSeq() throws Exception { super(FloatWritable.class,VLongWritable.class); }
	}

	// Proxy Mapper IntWritable
	public static class ProxyMapperIntDoubleSeq extends GenericProxyMapperSeq <IntWritable,DoubleWritable>{
		public ProxyMapperIntDoubleSeq() throws Exception { super(IntWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperIntFloatSeq extends GenericProxyMapperSeq <IntWritable,FloatWritable>{
		public ProxyMapperIntFloatSeq() throws Exception { super(IntWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperIntIntSeq extends GenericProxyMapperSeq <IntWritable,IntWritable>{
		public ProxyMapperIntIntSeq() throws Exception { super(IntWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperIntLongSeq extends GenericProxyMapperSeq <IntWritable,LongWritable>{
		public ProxyMapperIntLongSeq() throws Exception { super(IntWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperIntTextSeq extends GenericProxyMapperSeq <IntWritable,Text>{
		public ProxyMapperIntTextSeq() throws Exception { super(IntWritable.class,Text.class); }
	}
	public static class ProxyMapperIntUTF8Seq extends GenericProxyMapperSeq <IntWritable,UTF8>{
		public ProxyMapperIntUTF8Seq() throws Exception { super(IntWritable.class,UTF8.class); }
	}
	public static class ProxyMapperIntVIntSeq extends GenericProxyMapperSeq <IntWritable,VIntWritable>{
		public ProxyMapperIntVIntSeq() throws Exception { super(IntWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperIntVLongSeq extends GenericProxyMapperSeq <IntWritable,VLongWritable>{
		public ProxyMapperIntVLongSeq() throws Exception { super(IntWritable.class,VLongWritable.class); }
	}

	// Proxy Mapper LongWritable
	public static class ProxyMapperLongDoubleSeq extends GenericProxyMapperSeq <LongWritable,DoubleWritable>{
		public ProxyMapperLongDoubleSeq() throws Exception { super(LongWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperLongFloatSeq extends GenericProxyMapperSeq <LongWritable,FloatWritable>{
		public ProxyMapperLongFloatSeq() throws Exception { super(LongWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperLongIntSeq extends GenericProxyMapperSeq <LongWritable,IntWritable>{
		public ProxyMapperLongIntSeq() throws Exception { super(LongWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperLongLongSeq extends GenericProxyMapperSeq <LongWritable,LongWritable>{
		public ProxyMapperLongLongSeq() throws Exception { super(LongWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperLongTextSeq extends GenericProxyMapperSeq <LongWritable,Text>{
		public ProxyMapperLongTextSeq() throws Exception { super(LongWritable.class,Text.class); }
	}
	public static class ProxyMapperLongUTF8Seq extends GenericProxyMapperSeq <LongWritable,UTF8>{
		public ProxyMapperLongUTF8Seq() throws Exception { super(LongWritable.class,UTF8.class); }
	}
	public static class ProxyMapperLongVIntSeq extends GenericProxyMapperSeq <LongWritable,VIntWritable>{
		public ProxyMapperLongVIntSeq() throws Exception { super(LongWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperLongVLongSeq extends GenericProxyMapperSeq <LongWritable,VLongWritable>{
		public ProxyMapperLongVLongSeq() throws Exception { super(LongWritable.class,VLongWritable.class); }
	}

	// Proxy Mapper Text
	public static class ProxyMapperTextDoubleSeq extends GenericProxyMapperSeq <Text,DoubleWritable>{
		public ProxyMapperTextDoubleSeq() throws Exception { super(Text.class,DoubleWritable.class); }
	}
	public static class ProxyMapperTextFloatSeq extends GenericProxyMapperSeq <Text,FloatWritable>{
		public ProxyMapperTextFloatSeq() throws Exception { super(Text.class,FloatWritable.class); }
	}
	public static class ProxyMapperTextIntSeq extends GenericProxyMapperSeq <Text,IntWritable>{
		public ProxyMapperTextIntSeq() throws Exception { super(Text.class,IntWritable.class); }
	}
	public static class ProxyMapperTextLongSeq extends GenericProxyMapperSeq <Text,LongWritable>{
		public ProxyMapperTextLongSeq() throws Exception { super(Text.class,LongWritable.class); }
	}
	public static class ProxyMapperTextTextSeq extends GenericProxyMapperSeq <Text,Text>{
		public ProxyMapperTextTextSeq() throws Exception { super(Text.class,Text.class); }
	}
	public static class ProxyMapperTextUTF8Seq extends GenericProxyMapperSeq <Text,UTF8>{
		public ProxyMapperTextUTF8Seq() throws Exception { super(Text.class,UTF8.class); }
	}
	public static class ProxyMapperTextVIntSeq extends GenericProxyMapperSeq <Text,VIntWritable>{
		public ProxyMapperTextVIntSeq() throws Exception { super(Text.class,VIntWritable.class); }
	}
	public static class ProxyMapperTextVLongSeq extends GenericProxyMapperSeq <Text,VLongWritable>{
		public ProxyMapperTextVLongSeq() throws Exception { super(Text.class,VLongWritable.class); }
	}

	// Proxy Mapper UTF8
	public static class ProxyMapperUTF8DoubleSeq extends GenericProxyMapperSeq <UTF8,DoubleWritable>{
		public ProxyMapperUTF8DoubleSeq() throws Exception { super(UTF8.class,DoubleWritable.class); }
	}
	public static class ProxyMapperUTF8FloatSeq extends GenericProxyMapperSeq <UTF8,FloatWritable>{
		public ProxyMapperUTF8FloatSeq() throws Exception { super(UTF8.class,FloatWritable.class); }
	}
	public static class ProxyMapperUTF8IntSeq extends GenericProxyMapperSeq <UTF8,IntWritable>{
		public ProxyMapperUTF8IntSeq() throws Exception { super(UTF8.class,IntWritable.class); }
	}
	public static class ProxyMapperUTF8LongSeq extends GenericProxyMapperSeq <UTF8,LongWritable>{
		public ProxyMapperUTF8LongSeq() throws Exception { super(UTF8.class,LongWritable.class); }
	}
	public static class ProxyMapperUTF8TextSeq extends GenericProxyMapperSeq <UTF8,Text>{
		public ProxyMapperUTF8TextSeq() throws Exception { super(UTF8.class,Text.class); }
	}
	public static class ProxyMapperUTF8UTF8Seq extends GenericProxyMapperSeq <UTF8,UTF8>{
		public ProxyMapperUTF8UTF8Seq() throws Exception { super(UTF8.class,UTF8.class); }
	}
	public static class ProxyMapperUTF8VIntSeq extends GenericProxyMapperSeq <UTF8,VIntWritable>{
		public ProxyMapperUTF8VIntSeq() throws Exception { super(UTF8.class,VIntWritable.class); }
	}
	public static class ProxyMapperUTF8VLongSeq extends GenericProxyMapperSeq <UTF8,VLongWritable>{
		public ProxyMapperUTF8VLongSeq() throws Exception { super(UTF8.class,VLongWritable.class); }
	}

	// Proxy Mapper VIntWritable
	public static class ProxyMapperVIntDoubleSeq extends GenericProxyMapperSeq <VIntWritable,DoubleWritable>{
		public ProxyMapperVIntDoubleSeq() throws Exception { super(VIntWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperVIntFloatSeq extends GenericProxyMapperSeq <VIntWritable,FloatWritable>{
		public ProxyMapperVIntFloatSeq() throws Exception { super(VIntWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperVIntIntSeq extends GenericProxyMapperSeq <VIntWritable,IntWritable>{
		public ProxyMapperVIntIntSeq() throws Exception { super(VIntWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperVIntLongSeq extends GenericProxyMapperSeq <VIntWritable,LongWritable>{
		public ProxyMapperVIntLongSeq() throws Exception { super(VIntWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperVIntTextSeq extends GenericProxyMapperSeq <VIntWritable,Text>{
		public ProxyMapperVIntTextSeq() throws Exception { super(VIntWritable.class,Text.class); }
	}
	public static class ProxyMapperVIntUTF8Seq extends GenericProxyMapperSeq <VIntWritable,UTF8>{
		public ProxyMapperVIntUTF8Seq() throws Exception { super(VIntWritable.class,UTF8.class); }
	}
	public static class ProxyMapperVIntVIntSeq extends GenericProxyMapperSeq <VIntWritable,VIntWritable>{
		public ProxyMapperVIntVIntSeq() throws Exception { super(VIntWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperVIntVLongSeq extends GenericProxyMapperSeq <VIntWritable,VLongWritable>{
		public ProxyMapperVIntVLongSeq() throws Exception { super(VIntWritable.class,VLongWritable.class); }
	}

	// Proxy Mapper VLongWritable
	public static class ProxyMapperVLongDoubleSeq extends GenericProxyMapperSeq <VLongWritable,DoubleWritable>{
		public ProxyMapperVLongDoubleSeq() throws Exception { super(VLongWritable.class,DoubleWritable.class); }
	}
	public static class ProxyMapperVLongFloatSeq extends GenericProxyMapperSeq <VLongWritable,FloatWritable>{
		public ProxyMapperVLongFloatSeq() throws Exception { super(VLongWritable.class,FloatWritable.class); }
	}
	public static class ProxyMapperVLongIntSeq extends GenericProxyMapperSeq <VLongWritable,IntWritable>{
		public ProxyMapperVLongIntSeq() throws Exception { super(VLongWritable.class,IntWritable.class); }
	}
	public static class ProxyMapperVLongLongSeq extends GenericProxyMapperSeq <VLongWritable,LongWritable>{
		public ProxyMapperVLongLongSeq() throws Exception { super(VLongWritable.class,LongWritable.class); }
	}
	public static class ProxyMapperVLongTextSeq extends GenericProxyMapperSeq <VLongWritable,Text>{
		public ProxyMapperVLongTextSeq() throws Exception { super(VLongWritable.class,Text.class); }
	}
	public static class ProxyMapperVLongUTF8Seq extends GenericProxyMapperSeq <VLongWritable,UTF8>{
		public ProxyMapperVLongUTF8Seq() throws Exception { super(VLongWritable.class,UTF8.class); }
	}
	public static class ProxyMapperVLongVIntSeq extends GenericProxyMapperSeq <VLongWritable,VIntWritable>{
		public ProxyMapperVLongVIntSeq() throws Exception { super(VLongWritable.class,VIntWritable.class); }
	}
	public static class ProxyMapperVLongVLongSeq extends GenericProxyMapperSeq <VLongWritable,VLongWritable>{
		public ProxyMapperVLongVLongSeq() throws Exception { super(VLongWritable.class,VLongWritable.class); }
	}
	// Proxy Mapper NullWritable
		public static class ProxyMapperNullDouble extends GenericProxyMapper <NullWritable,DoubleWritable>{
			public ProxyMapperNullDouble() throws Exception { super(NullWritable.class,DoubleWritable.class); }
		}
		public static class ProxyMapperNullFloat extends GenericProxyMapper <NullWritable,FloatWritable>{
			public ProxyMapperNullFloat() throws Exception { super(NullWritable.class,FloatWritable.class); }
		}
		public static class ProxyMapperNullInt extends GenericProxyMapper <NullWritable,IntWritable>{
			public ProxyMapperNullInt() throws Exception { super(NullWritable.class,IntWritable.class); }
		}
		public static class ProxyMapperNullLong extends GenericProxyMapper <NullWritable,LongWritable>{
			public ProxyMapperNullLong() throws Exception { super(NullWritable.class,LongWritable.class); }
		}
		public static class ProxyMapperNullText extends GenericProxyMapper <NullWritable,Text>{
			public ProxyMapperNullText() throws Exception { super(NullWritable.class,Text.class); }
		}
		public static class ProxyMapperNullUTF8 extends GenericProxyMapper <NullWritable,UTF8>{
			public ProxyMapperNullUTF8() throws Exception { super(NullWritable.class,UTF8.class); }
		}
		public static class ProxyMapperNullVInt extends GenericProxyMapper <NullWritable,VIntWritable>{
			public ProxyMapperNullVInt() throws Exception { super(NullWritable.class,VIntWritable.class); }
		}
		public static class ProxyMapperNullVLong extends GenericProxyMapper <NullWritable,VLongWritable>{
			public ProxyMapperNullVLong() throws Exception { super(NullWritable.class,VLongWritable.class); }
		}
		public static class ProxyMapperNullNull extends GenericProxyMapper <NullWritable,NullWritable>{
			public ProxyMapperNullNull() throws Exception { super(NullWritable.class,NullWritable.class); }
		}
    //----------------------------------------------------------------------- 
	// Proxy Reducer Dummy Classes 

	// Proxy Reducer DoubleWritable
	public static class ProxyReducerDoubleDouble extends GenericProxyReducer <DoubleWritable, DoubleWritable>{
        public ProxyReducerDoubleDouble() throws Exception { super(DoubleWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerDoubleFloat extends GenericProxyReducer <DoubleWritable, FloatWritable>{
        public ProxyReducerDoubleFloat() throws Exception { super(DoubleWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerDoubleInt extends GenericProxyReducer <DoubleWritable, IntWritable>{
        public ProxyReducerDoubleInt() throws Exception { super(DoubleWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerDoubleLong extends GenericProxyReducer <DoubleWritable, LongWritable>{
        public ProxyReducerDoubleLong() throws Exception { super(DoubleWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerDoubleText extends GenericProxyReducer <DoubleWritable, Text>{
        public ProxyReducerDoubleText() throws Exception { super(DoubleWritable.class, Text.class); }
    }
	public static class ProxyReducerDoubleUTF8 extends GenericProxyReducer <DoubleWritable, UTF8>{
        public ProxyReducerDoubleUTF8() throws Exception { super(DoubleWritable.class, UTF8.class); }
    }
	public static class ProxyReducerDoubleVInt extends GenericProxyReducer <DoubleWritable, VIntWritable>{
        public ProxyReducerDoubleVInt() throws Exception { super(DoubleWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerDoubleVLong extends GenericProxyReducer <DoubleWritable, VLongWritable>{
        public ProxyReducerDoubleVLong() throws Exception { super(DoubleWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerDoubleNull extends GenericProxyReducer <DoubleWritable, NullWritable>{
        public ProxyReducerDoubleNull() throws Exception { super(DoubleWritable.class, NullWritable.class); }
    }
	// Proxy Reducer FloatWritable
	public static class ProxyReducerFloatDouble extends GenericProxyReducer <FloatWritable, DoubleWritable>{
        public ProxyReducerFloatDouble() throws Exception { super(FloatWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerFloatFloat extends GenericProxyReducer <FloatWritable, FloatWritable>{
        public ProxyReducerFloatFloat() throws Exception { super(FloatWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerFloatInt extends GenericProxyReducer <FloatWritable, IntWritable>{
        public ProxyReducerFloatInt() throws Exception { super(FloatWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerFloatLong extends GenericProxyReducer <FloatWritable, LongWritable>{
        public ProxyReducerFloatLong() throws Exception { super(FloatWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerFloatText extends GenericProxyReducer <FloatWritable, Text>{
        public ProxyReducerFloatText() throws Exception { super(FloatWritable.class, Text.class); }
    }
	public static class ProxyReducerFloatUTF8 extends GenericProxyReducer <FloatWritable, UTF8>{
        public ProxyReducerFloatUTF8() throws Exception { super(FloatWritable.class, UTF8.class); }
    }
	public static class ProxyReducerFloatVInt extends GenericProxyReducer <FloatWritable, VIntWritable>{
        public ProxyReducerFloatVInt() throws Exception { super(FloatWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerFloatVLong extends GenericProxyReducer <FloatWritable, VLongWritable>{
        public ProxyReducerFloatVLong() throws Exception { super(FloatWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerFloatNull extends GenericProxyReducer <FloatWritable, NullWritable>{
        public ProxyReducerFloatNull() throws Exception { super(FloatWritable.class, NullWritable.class); }
    }
	// Proxy Reducer IntWritable
	public static class ProxyReducerIntDouble extends GenericProxyReducer <IntWritable, DoubleWritable>{
        public ProxyReducerIntDouble() throws Exception { super(IntWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerIntFloat extends GenericProxyReducer <IntWritable, FloatWritable>{
        public ProxyReducerIntFloat() throws Exception { super(IntWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerIntInt extends GenericProxyReducer <IntWritable, IntWritable>{
        public ProxyReducerIntInt() throws Exception { super(IntWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerIntLong extends GenericProxyReducer <IntWritable, LongWritable>{
        public ProxyReducerIntLong() throws Exception { super(IntWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerIntText extends GenericProxyReducer <IntWritable, Text>{
        public ProxyReducerIntText() throws Exception { super(IntWritable.class, Text.class); }
    }
	public static class ProxyReducerIntUTF8 extends GenericProxyReducer <IntWritable, UTF8>{
        public ProxyReducerIntUTF8() throws Exception { super(IntWritable.class, UTF8.class); }
    }
	public static class ProxyReducerIntVInt extends GenericProxyReducer <IntWritable, VIntWritable>{
        public ProxyReducerIntVInt() throws Exception { super(IntWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerIntVLong extends GenericProxyReducer <IntWritable, VLongWritable>{
        public ProxyReducerIntVLong() throws Exception { super(IntWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerIntNull extends GenericProxyReducer <IntWritable, NullWritable>{
        public ProxyReducerIntNull() throws Exception { super(IntWritable.class, NullWritable.class); }
    }

	// Proxy Reducer LongWritable
	public static class ProxyReducerLongDouble extends GenericProxyReducer <LongWritable, DoubleWritable>{
        public ProxyReducerLongDouble() throws Exception { super(LongWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerLongFloat extends GenericProxyReducer <LongWritable, FloatWritable>{
        public ProxyReducerLongFloat() throws Exception { super(LongWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerLongInt extends GenericProxyReducer <LongWritable, IntWritable>{
        public ProxyReducerLongInt() throws Exception { super(LongWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerLongLong extends GenericProxyReducer <LongWritable, LongWritable>{
        public ProxyReducerLongLong() throws Exception { super(LongWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerLongText extends GenericProxyReducer <LongWritable, Text>{
        public ProxyReducerLongText() throws Exception { super(LongWritable.class, Text.class); }
    }
	public static class ProxyReducerLongUTF8 extends GenericProxyReducer <LongWritable, UTF8>{
        public ProxyReducerLongUTF8() throws Exception { super(LongWritable.class, UTF8.class); }
    }
	public static class ProxyReducerLongVInt extends GenericProxyReducer <LongWritable, VIntWritable>{
        public ProxyReducerLongVInt() throws Exception { super(LongWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerLongVLong extends GenericProxyReducer <LongWritable, VLongWritable>{
        public ProxyReducerLongVLong() throws Exception { super(LongWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerLongNull extends GenericProxyReducer <LongWritable, NullWritable>{
        public ProxyReducerLongNull() throws Exception { super(LongWritable.class, NullWritable.class); }
    }

	// Proxy Reducer Text
	public static class ProxyReducerTextDouble extends GenericProxyReducer <Text, DoubleWritable>{
        public ProxyReducerTextDouble() throws Exception { super(Text.class, DoubleWritable.class); }
    }
	public static class ProxyReducerTextFloat extends GenericProxyReducer <Text, FloatWritable>{
        public ProxyReducerTextFloat() throws Exception { super(Text.class, FloatWritable.class); }
    }
	public static class ProxyReducerTextInt extends GenericProxyReducer <Text, IntWritable>{
        public ProxyReducerTextInt() throws Exception { super(Text.class, IntWritable.class); }
    }
	public static class ProxyReducerTextLong extends GenericProxyReducer <Text, LongWritable>{
        public ProxyReducerTextLong() throws Exception { super(Text.class, LongWritable.class); }
    }
	public static class ProxyReducerTextText extends GenericProxyReducer <Text, Text>{
        public ProxyReducerTextText() throws Exception { super(Text.class, Text.class); }
    }
	public static class ProxyReducerTextUTF8 extends GenericProxyReducer <Text, UTF8>{
        public ProxyReducerTextUTF8() throws Exception { super(Text.class, UTF8.class); }
    }
	public static class ProxyReducerTextVInt extends GenericProxyReducer <Text, VIntWritable>{
        public ProxyReducerTextVInt() throws Exception { super(Text.class, VIntWritable.class); }
    }
	public static class ProxyReducerTextVLong extends GenericProxyReducer <Text, VLongWritable>{
        public ProxyReducerTextVLong() throws Exception { super(Text.class, VLongWritable.class); }
    }
	public static class ProxyReducerTextNull extends GenericProxyReducer <Text, NullWritable>{
        public ProxyReducerTextNull() throws Exception { super(Text.class, NullWritable.class); }
    }
	
	// Proxy Reducer UTF8
	public static class ProxyReducerUTF8Double extends GenericProxyReducer <UTF8, DoubleWritable>{
        public ProxyReducerUTF8Double() throws Exception { super(UTF8.class, DoubleWritable.class); }
    }
	public static class ProxyReducerUTF8Float extends GenericProxyReducer <UTF8, FloatWritable>{
        public ProxyReducerUTF8Float() throws Exception { super(UTF8.class, FloatWritable.class); }
    }
	public static class ProxyReducerUTF8Int extends GenericProxyReducer <UTF8, IntWritable>{
        public ProxyReducerUTF8Int() throws Exception { super(UTF8.class, IntWritable.class); }
    }
	public static class ProxyReducerUTF8Long extends GenericProxyReducer <UTF8, LongWritable>{
        public ProxyReducerUTF8Long() throws Exception { super(UTF8.class, LongWritable.class); }
    }
	public static class ProxyReducerUTF8Text extends GenericProxyReducer <UTF8, Text>{
        public ProxyReducerUTF8Text() throws Exception { super(UTF8.class, Text.class); }
    }
	public static class ProxyReducerUTF8UTF8 extends GenericProxyReducer <UTF8, UTF8>{
        public ProxyReducerUTF8UTF8() throws Exception { super(UTF8.class, UTF8.class); }
    }
	public static class ProxyReducerUTF8VInt extends GenericProxyReducer <UTF8, VIntWritable>{
        public ProxyReducerUTF8VInt() throws Exception { super(UTF8.class, VIntWritable.class); }
    }
	public static class ProxyReducerUTF8VLong extends GenericProxyReducer <UTF8, VLongWritable>{
        public ProxyReducerUTF8VLong() throws Exception { super(UTF8.class, VLongWritable.class); }
    }
	public static class ProxyReducerUTF8Null extends GenericProxyReducer <UTF8, NullWritable>{
        public ProxyReducerUTF8Null() throws Exception { super(UTF8.class, NullWritable.class); }
    }

	// Proxy Reducer VIntWritable
	public static class ProxyReducerVIntDouble extends GenericProxyReducer <VIntWritable, DoubleWritable>{
        public ProxyReducerVIntDouble() throws Exception { super(VIntWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerVIntFloat extends GenericProxyReducer <VIntWritable, FloatWritable>{
        public ProxyReducerVIntFloat() throws Exception { super(VIntWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerVIntInt extends GenericProxyReducer <VIntWritable, IntWritable>{
        public ProxyReducerVIntInt() throws Exception { super(VIntWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerVIntLong extends GenericProxyReducer <VIntWritable, LongWritable>{
        public ProxyReducerVIntLong() throws Exception { super(VIntWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerVIntText extends GenericProxyReducer <VIntWritable, Text>{
        public ProxyReducerVIntText() throws Exception { super(VIntWritable.class, Text.class); }
    }
	public static class ProxyReducerVIntUTF8 extends GenericProxyReducer <VIntWritable, UTF8>{
        public ProxyReducerVIntUTF8() throws Exception { super(VIntWritable.class, UTF8.class); }
    }
	public static class ProxyReducerVIntVInt extends GenericProxyReducer <VIntWritable, VIntWritable>{
        public ProxyReducerVIntVInt() throws Exception { super(VIntWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerVIntVLong extends GenericProxyReducer <VIntWritable, VLongWritable>{
        public ProxyReducerVIntVLong() throws Exception { super(VIntWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerVIntNull extends GenericProxyReducer <VIntWritable, NullWritable>{
        public ProxyReducerVIntNull() throws Exception { super(VIntWritable.class, NullWritable.class); }
    }

	// Proxy Reducer VLongWritable
	public static class ProxyReducerVLongDouble extends GenericProxyReducer <VLongWritable, DoubleWritable>{
        public ProxyReducerVLongDouble () throws Exception { super(VLongWritable.class, DoubleWritable.class); }
    }
	public static class ProxyReducerVLongFloat extends GenericProxyReducer <VLongWritable, FloatWritable>{
        public ProxyReducerVLongFloat () throws Exception { super(VLongWritable.class, FloatWritable.class); }
    }
	public static class ProxyReducerVLongInt extends GenericProxyReducer <VLongWritable, IntWritable>{
        public ProxyReducerVLongInt () throws Exception { super(VLongWritable.class, IntWritable.class); }
    }
	public static class ProxyReducerVLongLong extends GenericProxyReducer <VLongWritable, LongWritable>{
        public ProxyReducerVLongLong () throws Exception { super(VLongWritable.class, LongWritable.class); }
    }
	public static class ProxyReducerVLongText extends GenericProxyReducer <VLongWritable, Text>{
        public ProxyReducerVLongText () throws Exception { super(VLongWritable.class, Text.class); }
    }
	public static class ProxyReducerVLongUTF8 extends GenericProxyReducer <VLongWritable, UTF8>{
        public ProxyReducerVLongUTF8 () throws Exception { super(VLongWritable.class, UTF8.class); }
    }
	public static class ProxyReducerVLongVInt extends GenericProxyReducer <VLongWritable, VIntWritable>{
        public ProxyReducerVLongVInt () throws Exception { super(VLongWritable.class, VIntWritable.class); }
    }
	public static class ProxyReducerVLongVLong extends GenericProxyReducer <VLongWritable, VLongWritable>{
        public ProxyReducerVLongVLong () throws Exception { super(VLongWritable.class, VLongWritable.class); }
    }
	public static class ProxyReducerVLongNull extends GenericProxyReducer <VLongWritable, NullWritable>{
        public ProxyReducerVLongNull () throws Exception { super(VLongWritable.class, NullWritable.class); }
    }
	
	// Proxy Reducer DoubleWritable
		public static class ProxyReducerNullDouble extends GenericProxyReducer <NullWritable, DoubleWritable>{
	        public ProxyReducerNullDouble() throws Exception { super(NullWritable.class, DoubleWritable.class); }
	    }
		public static class ProxyReducerNullFloat extends GenericProxyReducer <NullWritable, FloatWritable>{
	        public ProxyReducerNullFloat() throws Exception { super(NullWritable.class, FloatWritable.class); }
	    }
		public static class ProxyReducerNullInt extends GenericProxyReducer <NullWritable, IntWritable>{
	        public ProxyReducerNullInt() throws Exception { super(NullWritable.class, IntWritable.class); }
	    }
		public static class ProxyReducerNullLong extends GenericProxyReducer <NullWritable, LongWritable>{
	        public ProxyReducerNullLong() throws Exception { super(NullWritable.class, LongWritable.class); }
	    }
		public static class ProxyReducerNullText extends GenericProxyReducer <NullWritable, Text>{
	        public ProxyReducerNullText() throws Exception { super(NullWritable.class, Text.class); }
	    }
		public static class ProxyReducerNullUTF8 extends GenericProxyReducer <NullWritable, UTF8>{
	        public ProxyReducerNullUTF8() throws Exception { super(NullWritable.class, UTF8.class); }
	    }
		public static class ProxyReducerNullVInt extends GenericProxyReducer <NullWritable, VIntWritable>{
	        public ProxyReducerNullVInt() throws Exception { super(NullWritable.class, VIntWritable.class); }
	    }
		public static class ProxyReducerNullVLong extends GenericProxyReducer <NullWritable, VLongWritable>{
	        public ProxyReducerNullVLong() throws Exception { super(NullWritable.class, VLongWritable.class); }
	    }
		public static class ProxyReducerNullNull extends GenericProxyReducer <NullWritable, NullWritable>{
	        public ProxyReducerNullNull() throws Exception { super(NullWritable.class, NullWritable.class); }
	    }

    //==============================================================================================
    //----------------------------------------------------------------------- 
    // version 1
    // proxy mapper
    // Key : Text
    private void addProxyMapperTextKeyOutMapping() throws Exception {
        Map<String, Class <? extends Mapper>> valueMap = new HashMap<String, Class<? extends Mapper>>();

        // Text, Long
        GenericProxyMapper<Text,LongWritable> gMapTextLong = 
            new  GenericProxyMapper<Text,LongWritable>(Text.class,LongWritable.class);
        valueMap.put( LongWritable.class.getCanonicalName(), gMapTextLong.getClass());

        // Text, Int
        GenericProxyMapper<Text,IntWritable> gMapTextInt = 
            new  GenericProxyMapper<Text,IntWritable>(Text.class,IntWritable.class);
        valueMap.put( IntWritable.class.getCanonicalName(), gMapTextInt.getClass());

        // Text, Float
        GenericProxyMapper<Text,FloatWritable> gMapTextFloat = 
            new  GenericProxyMapper<Text,FloatWritable>(Text.class,FloatWritable.class);
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapTextFloat.getClass());

        // Text, Text
        GenericProxyMapper<Text,Text> gMapTextText = 
            new  GenericProxyMapper<Text,Text>(Text.class,Text.class);
        valueMap.put( Text.class.getCanonicalName(), gMapTextText.getClass());

        proxyMapMapping.put(Text.class.getCanonicalName(), valueMap); 

    }

    // Key : Integer 
    private void addProxyMapperIntKeyOutMapping() throws Exception {
        Map<String, Class<? extends Mapper>> valueMap = new HashMap<String, Class<? extends Mapper>>();

        // Int, Long
        GenericProxyMapper<IntWritable,LongWritable> gMapIntLong = 
            new  GenericProxyMapper<IntWritable,LongWritable>(IntWritable.class,LongWritable.class);
        valueMap.put( LongWritable.class.getCanonicalName(), gMapIntLong.getClass());

        // Int, Int
        GenericProxyMapper<IntWritable,IntWritable> gMapIntInt = 
            new  GenericProxyMapper<IntWritable,IntWritable>(IntWritable.class,IntWritable.class);
        valueMap.put( IntWritable.class.getCanonicalName(), gMapIntInt.getClass());

        // Int, Float
        GenericProxyMapper<IntWritable,FloatWritable> gMapIntFloat = 
            new  GenericProxyMapper<IntWritable,FloatWritable>(IntWritable.class,FloatWritable.class);
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapIntFloat.getClass());

        // Int, Text
        GenericProxyMapper<IntWritable,Text> gMapIntText = 
            new  GenericProxyMapper<IntWritable,Text>(IntWritable.class,Text.class);
        valueMap.put( Text.class.getCanonicalName(), gMapIntText.getClass());

        proxyMapMapping.put(Text.class.getCanonicalName(), valueMap); 

    }

    // Key : Long 
    private void addProxyMapperLongKeyOutMapping() throws Exception {
        Map<String, Class<? extends Mapper>> valueMap = new HashMap<String, Class<? extends Mapper>>();

        // Long, Long
        GenericProxyMapper<LongWritable,LongWritable> gMapLongLong = 
            new  GenericProxyMapper<LongWritable,LongWritable>(LongWritable.class,LongWritable.class);
        valueMap.put( LongWritable.class.getCanonicalName(), gMapLongLong.getClass());

        // Long, Int
        GenericProxyMapper<LongWritable,IntWritable> gMapLongInt = 
            new  GenericProxyMapper<LongWritable,IntWritable>(LongWritable.class,IntWritable.class);
        valueMap.put( IntWritable.class.getCanonicalName(), gMapLongInt.getClass());

        // Long, Float
        GenericProxyMapper<LongWritable,FloatWritable> gMapLongFloat = 
            new  GenericProxyMapper<LongWritable,FloatWritable>(LongWritable.class,FloatWritable.class);
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapLongFloat.getClass());

        // Long, Text
        GenericProxyMapper<LongWritable,Text> gMapLongText = 
            new  GenericProxyMapper<LongWritable,Text>(LongWritable.class,Text.class);
        valueMap.put( Text.class.getCanonicalName(), gMapLongText.getClass());

        proxyMapMapping.put(LongWritable.class.getCanonicalName(), valueMap); 

    }

    // Key : Float 
    private void addProxyMapperFloatKeyOutMapping() throws Exception {
        Map<String, Class<? extends Mapper>> valueMap = new HashMap<String, Class<? extends Mapper>>();

        // Float, Long
        GenericProxyMapper<FloatWritable,LongWritable> gMapFloatLong = 
            new  GenericProxyMapper<FloatWritable,LongWritable>(FloatWritable.class,LongWritable.class);
        valueMap.put( LongWritable.class.getCanonicalName(), gMapFloatLong.getClass());

        // Float, Int
        GenericProxyMapper<FloatWritable,IntWritable> gMapFloatInt = 
            new  GenericProxyMapper<FloatWritable,IntWritable>(FloatWritable.class,IntWritable.class);
        valueMap.put( IntWritable.class.getCanonicalName(), gMapFloatInt.getClass());

        // Float, Float
        GenericProxyMapper<FloatWritable,FloatWritable> gMapFloatFloat = 
            new  GenericProxyMapper<FloatWritable,FloatWritable>(FloatWritable.class,FloatWritable.class);
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapFloatFloat.getClass());

        // Float, Text
        GenericProxyMapper<FloatWritable,Text> gMapFloatText = 
            new  GenericProxyMapper<FloatWritable,Text>(FloatWritable.class,Text.class);
        valueMap.put( Text.class.getCanonicalName(), gMapFloatText.getClass());

        proxyMapMapping.put(FloatWritable.class.getCanonicalName(), valueMap); 

    }
    // add proxy mapper end
    //==============================================================================================
    //----------------------------------------------------------------------- 
    // version 1
    // proxy reducer
    // Key : Text
    private void addProxyReducerTextKeyOutMapping() throws Exception {
        Map<String, Class <? extends Reducer>> valueMap = new HashMap<String, Class <? extends Reducer>>();

        // Text, Long
        GenericProxyReducer<Text,LongWritable> gMapTextLong = 
            new  GenericProxyReducer<Text,LongWritable>(Text.class, LongWritable.class);
        valueMap.put( LongWritable.class.getCanonicalName(), gMapTextLong.getClass());

        // Text, Int
        GenericProxyReducer<Text,IntWritable> gMapTextInt = 
            new  GenericProxyReducer<Text,IntWritable>(Text.class, IntWritable.class);
        valueMap.put( IntWritable.class.getCanonicalName(), gMapTextInt.getClass());

        // Text, Float
        GenericProxyReducer<Text,FloatWritable> gMapTextFloat = 
            new  GenericProxyReducer<Text,FloatWritable>(Text.class, FloatWritable.class);
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapTextFloat.getClass());

        // Text, Text
        GenericProxyReducer<Text,Text> gMapTextText = 
            new  GenericProxyReducer<Text,Text>(Text.class, Text.class);
        valueMap.put( Text.class.getCanonicalName(), gMapTextText.getClass());

        proxyReduceMapping.put(Text.class.getCanonicalName(), valueMap); 

    }
    // Key : Integer 
    private void addProxyReducerIntKeyOutMapping() throws Exception {
        Map<String, Class <? extends Reducer>> valueMap = new HashMap<String, Class <? extends Reducer>>();
        /*
        // Int, Long
        GenericProxyReducer<IntWritable,LongWritable> gMapIntLong = 
            new  GenericProxyReducer<IntWritable,LongWritable>();
        valueMap.put( LongWritable.class.getCanonicalName(), gMapIntLong.getClass());

        // Int, Int
        GenericProxyReducer<IntWritable,IntWritable> gMapIntInt = 
            new  GenericProxyReducer<IntWritable,IntWritable>();
        valueMap.put( IntWritable.class.getCanonicalName(), gMapIntInt.getClass());

        // Int, Float
        GenericProxyReducer<IntWritable,FloatWritable> gMapIntFloat = 
            new  GenericProxyReducer<IntWritable,FloatWritable>();
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapIntFloat.getClass());

        // Int, Text
        GenericProxyReducer<IntWritable,Text> gMapIntText = 
            new  GenericProxyReducer<IntWritable,Text>();
        valueMap.put( Text.class.getCanonicalName(), gMapIntText.getClass());

        proxyReduceMapping.put(IntWritable.class.getCanonicalName(), valueMap); 
        */

    }
    // Key : Long 
    private void addProxyReducerLongKeyOutMapping() throws Exception {
        /*
        Map<String, Class <? extends Reducer>> valueMap = new HashMap<String, Class<? extends Reducer>>();

        // Long, Long
        GenericProxyReducer<LongWritable,LongWritable> gMapLongLong = 
            new  GenericProxyReducer<LongWritable,LongWritable>();
        valueMap.put( LongWritable.class.getCanonicalName(), gMapLongLong.getClass());

        // Long, Int
        GenericProxyReducer<LongWritable,IntWritable> gMapLongInt = 
            new  GenericProxyReducer<LongWritable,IntWritable>();
        valueMap.put( IntWritable.class.getCanonicalName(), gMapLongInt.getClass());

        // Long, Float
        GenericProxyReducer<LongWritable,FloatWritable> gMapLongFloat = 
            new  GenericProxyReducer<LongWritable,FloatWritable>();
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapLongFloat.getClass());

        // Long, Text
        GenericProxyReducer<LongWritable,Text> gMapLongText = 
            new  GenericProxyReducer<LongWritable,Text>();
        valueMap.put( Text.class.getCanonicalName(), gMapLongText.getClass());

        proxyReduceMapping.put(LongWritable.class.getCanonicalName(), valueMap); 
        */

    }

    // Key : Float 
    private void addProxyReducerFloatKeyOutMapping() throws Exception {
        /*
        Map<String, Class<? extends Reducer>> valueMap = new HashMap<String, Class<? extends Reducer>>();

        // Float, Long
        GenericProxyReducer<FloatWritable,LongWritable> gMapFloatLong = 
            new  GenericProxyReducer<FloatWritable,LongWritable>();
        valueMap.put( LongWritable.class.getCanonicalName(), gMapFloatLong.getClass());

        // Float, Int
        GenericProxyReducer<FloatWritable,IntWritable> gMapFloatInt = 
            new  GenericProxyReducer<FloatWritable,IntWritable>();
        valueMap.put( IntWritable.class.getCanonicalName(), gMapFloatInt.getClass());

        // Float, Float
        GenericProxyReducer<FloatWritable,FloatWritable> gMapFloatFloat = 
            new  GenericProxyReducer<FloatWritable,FloatWritable>();
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapFloatFloat.getClass());

        // Float, Text
        GenericProxyReducer<FloatWritable,Text> gMapFloatText = 
            new  GenericProxyReducer<FloatWritable,Text>();
        valueMap.put( Text.class.getCanonicalName(), gMapFloatText.getClass());

        proxyReduceMapping.put(FloatWritable.class.getCanonicalName(), valueMap); 
        */

    }
    // add proxy reduce end
    //==============================================================================================

}


