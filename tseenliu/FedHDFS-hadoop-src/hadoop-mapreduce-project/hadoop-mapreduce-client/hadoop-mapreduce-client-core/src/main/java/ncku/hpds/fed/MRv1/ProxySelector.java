package ncku.hpds.fed.MRv1 ;

import ncku.hpds.fed.MRv1.proxy.*;
import java.net.*;
import java.io.*;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;

public class ProxySelector {
    // key, value --> generic class
    private Map<String, Map<String, Class< ? extends Mapper>>> proxyMapMapping = 
        new HashMap<String, Map<String, Class< ? extends Mapper>>>(); 
    private Map<String, Map<String, Class< ? extends Reducer>>> proxyReduceMapping = 
        new HashMap<String, Map<String, Class< ? extends Reducer>>>(); 
    private String SeqFormatCanonicalName = SequenceFileInputFormat.class.getCanonicalName();
    private String mJobInputFormatName = "";
    private JobConf mJob;
    private static iAddOtherMapping mCallback = null;

    public ProxySelector(JobConf job ) {
        mJob = job;
        proxyMapMapping.clear();
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
            mJobInputFormatName = mJob.getInputFormat().getClass().getCanonicalName();
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
		addProxyMapperMapping( FloatWritable.class, DoubleWritable.class, ProxyMapperFloatDouble.class );
		addProxyMapperMapping( FloatWritable.class, FloatWritable.class, ProxyMapperFloatFloat.class );
		addProxyMapperMapping( FloatWritable.class, IntWritable.class, ProxyMapperFloatInt.class );
		addProxyMapperMapping( FloatWritable.class, LongWritable.class, ProxyMapperFloatLong.class );
		addProxyMapperMapping( FloatWritable.class, Text.class, ProxyMapperFloatText.class );
		addProxyMapperMapping( FloatWritable.class, UTF8.class, ProxyMapperFloatUTF8.class );
		addProxyMapperMapping( FloatWritable.class, VIntWritable.class, ProxyMapperFloatVInt.class );
		addProxyMapperMapping( FloatWritable.class, VLongWritable.class, ProxyMapperFloatVLong.class );
		addProxyMapperMapping( IntWritable.class, DoubleWritable.class, ProxyMapperIntDouble.class );
		addProxyMapperMapping( IntWritable.class, FloatWritable.class, ProxyMapperIntFloat.class );
		addProxyMapperMapping( IntWritable.class, IntWritable.class, ProxyMapperIntInt.class );
		addProxyMapperMapping( IntWritable.class, LongWritable.class, ProxyMapperIntLong.class );
		addProxyMapperMapping( IntWritable.class, Text.class, ProxyMapperIntText.class );
		addProxyMapperMapping( IntWritable.class, UTF8.class, ProxyMapperIntUTF8.class );
		addProxyMapperMapping( IntWritable.class, VIntWritable.class, ProxyMapperIntVInt.class );
		addProxyMapperMapping( IntWritable.class, VLongWritable.class, ProxyMapperIntVLong.class );
		addProxyMapperMapping( LongWritable.class, DoubleWritable.class, ProxyMapperLongDouble.class );
		addProxyMapperMapping( LongWritable.class, FloatWritable.class, ProxyMapperLongFloat.class );
		addProxyMapperMapping( LongWritable.class, IntWritable.class, ProxyMapperLongInt.class );
		addProxyMapperMapping( LongWritable.class, LongWritable.class, ProxyMapperLongLong.class );
		addProxyMapperMapping( LongWritable.class, Text.class, ProxyMapperLongText.class );
		addProxyMapperMapping( LongWritable.class, UTF8.class, ProxyMapperLongUTF8.class );
		addProxyMapperMapping( LongWritable.class, VIntWritable.class, ProxyMapperLongVInt.class );
		addProxyMapperMapping( LongWritable.class, VLongWritable.class, ProxyMapperLongVLong.class );
		addProxyMapperMapping( Text.class, DoubleWritable.class, ProxyMapperTextDouble.class );
		addProxyMapperMapping( Text.class, FloatWritable.class, ProxyMapperTextFloat.class );
		addProxyMapperMapping( Text.class, IntWritable.class, ProxyMapperTextInt.class );
		addProxyMapperMapping( Text.class, LongWritable.class, ProxyMapperTextLong.class );
		addProxyMapperMapping( Text.class, Text.class, ProxyMapperTextText.class );
		addProxyMapperMapping( Text.class, UTF8.class, ProxyMapperTextUTF8.class );
		addProxyMapperMapping( Text.class, VIntWritable.class, ProxyMapperTextVInt.class );
		addProxyMapperMapping( Text.class, VLongWritable.class, ProxyMapperTextVLong.class );
		addProxyMapperMapping( UTF8.class, DoubleWritable.class, ProxyMapperUTF8Double.class );
		addProxyMapperMapping( UTF8.class, FloatWritable.class, ProxyMapperUTF8Float.class );
		addProxyMapperMapping( UTF8.class, IntWritable.class, ProxyMapperUTF8Int.class );
		addProxyMapperMapping( UTF8.class, LongWritable.class, ProxyMapperUTF8Long.class );
		addProxyMapperMapping( UTF8.class, Text.class, ProxyMapperUTF8Text.class );
		addProxyMapperMapping( UTF8.class, UTF8.class, ProxyMapperUTF8UTF8.class );
		addProxyMapperMapping( UTF8.class, VIntWritable.class, ProxyMapperUTF8VInt.class );
		addProxyMapperMapping( UTF8.class, VLongWritable.class, ProxyMapperUTF8VLong.class );
		addProxyMapperMapping( VIntWritable.class, DoubleWritable.class, ProxyMapperVIntDouble.class );
		addProxyMapperMapping( VIntWritable.class, FloatWritable.class, ProxyMapperVIntFloat.class );
		addProxyMapperMapping( VIntWritable.class, IntWritable.class, ProxyMapperVIntInt.class );
		addProxyMapperMapping( VIntWritable.class, LongWritable.class, ProxyMapperVIntLong.class );
		addProxyMapperMapping( VIntWritable.class, Text.class, ProxyMapperVIntText.class );
		addProxyMapperMapping( VIntWritable.class, UTF8.class, ProxyMapperVIntUTF8.class );
		addProxyMapperMapping( VIntWritable.class, VIntWritable.class, ProxyMapperVIntVInt.class );
		addProxyMapperMapping( VIntWritable.class, VLongWritable.class, ProxyMapperVIntVLong.class );
		addProxyMapperMapping( VLongWritable.class, DoubleWritable.class, ProxyMapperVLongDouble.class );
		addProxyMapperMapping( VLongWritable.class, FloatWritable.class, ProxyMapperVLongFloat.class );
		addProxyMapperMapping( VLongWritable.class, IntWritable.class, ProxyMapperVLongInt.class );
		addProxyMapperMapping( VLongWritable.class, LongWritable.class, ProxyMapperVLongLong.class );
		addProxyMapperMapping( VLongWritable.class, Text.class, ProxyMapperVLongText.class );
		addProxyMapperMapping( VLongWritable.class, UTF8.class, ProxyMapperVLongUTF8.class );
		addProxyMapperMapping( VLongWritable.class, VIntWritable.class, ProxyMapperVLongVInt.class );
		addProxyMapperMapping( VLongWritable.class, VLongWritable.class, ProxyMapperVLongVLong.class );
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
		addProxyReducerMapping( FloatWritable.class, DoubleWritable.class, ProxyReducerFloatDouble.class );
		addProxyReducerMapping( FloatWritable.class, FloatWritable.class, ProxyReducerFloatFloat.class );
		addProxyReducerMapping( FloatWritable.class, IntWritable.class, ProxyReducerFloatInt.class );
		addProxyReducerMapping( FloatWritable.class, LongWritable.class, ProxyReducerFloatLong.class );
		addProxyReducerMapping( FloatWritable.class, Text.class, ProxyReducerFloatText.class );
		addProxyReducerMapping( FloatWritable.class, UTF8.class, ProxyReducerFloatUTF8.class );
		addProxyReducerMapping( FloatWritable.class, VIntWritable.class, ProxyReducerFloatVInt.class );
		addProxyReducerMapping( FloatWritable.class, VLongWritable.class, ProxyReducerFloatVLong.class );
		addProxyReducerMapping( IntWritable.class, DoubleWritable.class, ProxyReducerIntDouble.class );
		addProxyReducerMapping( IntWritable.class, FloatWritable.class, ProxyReducerIntFloat.class );
		addProxyReducerMapping( IntWritable.class, IntWritable.class, ProxyReducerIntInt.class );
		addProxyReducerMapping( IntWritable.class, LongWritable.class, ProxyReducerIntLong.class );
		addProxyReducerMapping( IntWritable.class, Text.class, ProxyReducerIntText.class );
		addProxyReducerMapping( IntWritable.class, UTF8.class, ProxyReducerIntUTF8.class );
		addProxyReducerMapping( IntWritable.class, VIntWritable.class, ProxyReducerIntVInt.class );
		addProxyReducerMapping( IntWritable.class, VLongWritable.class, ProxyReducerIntVLong.class );
		addProxyReducerMapping( LongWritable.class, DoubleWritable.class, ProxyReducerLongDouble.class );
		addProxyReducerMapping( LongWritable.class, FloatWritable.class, ProxyReducerLongFloat.class );
		addProxyReducerMapping( LongWritable.class, IntWritable.class, ProxyReducerLongInt.class );
		addProxyReducerMapping( LongWritable.class, LongWritable.class, ProxyReducerLongLong.class );
		addProxyReducerMapping( LongWritable.class, Text.class, ProxyReducerLongText.class );
		addProxyReducerMapping( LongWritable.class, UTF8.class, ProxyReducerLongUTF8.class );
		addProxyReducerMapping( LongWritable.class, VIntWritable.class, ProxyReducerLongVInt.class );
		addProxyReducerMapping( LongWritable.class, VLongWritable.class, ProxyReducerLongVLong.class );
		addProxyReducerMapping( Text.class, DoubleWritable.class, ProxyReducerTextDouble.class );
		addProxyReducerMapping( Text.class, FloatWritable.class, ProxyReducerTextFloat.class );
		addProxyReducerMapping( Text.class, IntWritable.class, ProxyReducerTextInt.class );
		addProxyReducerMapping( Text.class, LongWritable.class, ProxyReducerTextLong.class );
		addProxyReducerMapping( Text.class, Text.class, ProxyReducerTextText.class );
		addProxyReducerMapping( Text.class, UTF8.class, ProxyReducerTextUTF8.class );
		addProxyReducerMapping( Text.class, VIntWritable.class, ProxyReducerTextVInt.class );
		addProxyReducerMapping( Text.class, VLongWritable.class, ProxyReducerTextVLong.class );
		addProxyReducerMapping( UTF8.class, DoubleWritable.class, ProxyReducerUTF8Double.class );
		addProxyReducerMapping( UTF8.class, FloatWritable.class, ProxyReducerUTF8Float.class );
		addProxyReducerMapping( UTF8.class, IntWritable.class, ProxyReducerUTF8Int.class );
		addProxyReducerMapping( UTF8.class, LongWritable.class, ProxyReducerUTF8Long.class );
		addProxyReducerMapping( UTF8.class, Text.class, ProxyReducerUTF8Text.class );
		addProxyReducerMapping( UTF8.class, UTF8.class, ProxyReducerUTF8UTF8.class );
		addProxyReducerMapping( UTF8.class, VIntWritable.class, ProxyReducerUTF8VInt.class );
		addProxyReducerMapping( UTF8.class, VLongWritable.class, ProxyReducerUTF8VLong.class );
		addProxyReducerMapping( VIntWritable.class, DoubleWritable.class, ProxyReducerVIntDouble.class );
		addProxyReducerMapping( VIntWritable.class, FloatWritable.class, ProxyReducerVIntFloat.class );
		addProxyReducerMapping( VIntWritable.class, IntWritable.class, ProxyReducerVIntInt.class );
		addProxyReducerMapping( VIntWritable.class, LongWritable.class, ProxyReducerVIntLong.class );
		addProxyReducerMapping( VIntWritable.class, Text.class, ProxyReducerVIntText.class );
		addProxyReducerMapping( VIntWritable.class, UTF8.class, ProxyReducerVIntUTF8.class );
		addProxyReducerMapping( VIntWritable.class, VIntWritable.class, ProxyReducerVIntVInt.class );
		addProxyReducerMapping( VIntWritable.class, VLongWritable.class, ProxyReducerVIntVLong.class );
		addProxyReducerMapping( VLongWritable.class, DoubleWritable.class, ProxyReducerVLongDouble.class );
		addProxyReducerMapping( VLongWritable.class, FloatWritable.class, ProxyReducerVLongFloat.class );
		addProxyReducerMapping( VLongWritable.class, IntWritable.class, ProxyReducerVLongInt.class );
		addProxyReducerMapping( VLongWritable.class, LongWritable.class, ProxyReducerVLongLong.class );
		addProxyReducerMapping( VLongWritable.class, Text.class, ProxyReducerVLongText.class );
		addProxyReducerMapping( VLongWritable.class, UTF8.class, ProxyReducerVLongUTF8.class );
		addProxyReducerMapping( VLongWritable.class, VIntWritable.class, ProxyReducerVLongVInt.class );
		addProxyReducerMapping( VLongWritable.class, VLongWritable.class, ProxyReducerVLongVLong.class );
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
    //----------------------------------------------------------------------- 
	// Proxy Reducer Dummay Classes 

	// Proxy Reducer DoubleWritable
	public static class ProxyReducerDoubleDouble extends GenericProxyReducer <DoubleWritable, DoubleWritable>{}
	public static class ProxyReducerDoubleFloat extends GenericProxyReducer <DoubleWritable, FloatWritable>{}
	public static class ProxyReducerDoubleInt extends GenericProxyReducer <DoubleWritable, IntWritable>{}
	public static class ProxyReducerDoubleLong extends GenericProxyReducer <DoubleWritable, LongWritable>{}
	public static class ProxyReducerDoubleText extends GenericProxyReducer <DoubleWritable, Text>{}
	public static class ProxyReducerDoubleUTF8 extends GenericProxyReducer <DoubleWritable, UTF8>{}
	public static class ProxyReducerDoubleVInt extends GenericProxyReducer <DoubleWritable, VIntWritable>{}
	public static class ProxyReducerDoubleVLong extends GenericProxyReducer <DoubleWritable, VLongWritable>{}

	// Proxy Reducer FloatWritable
	public static class ProxyReducerFloatDouble extends GenericProxyReducer <FloatWritable, DoubleWritable>{}
	public static class ProxyReducerFloatFloat extends GenericProxyReducer <FloatWritable, FloatWritable>{}
	public static class ProxyReducerFloatInt extends GenericProxyReducer <FloatWritable, IntWritable>{}
	public static class ProxyReducerFloatLong extends GenericProxyReducer <FloatWritable, LongWritable>{}
	public static class ProxyReducerFloatText extends GenericProxyReducer <FloatWritable, Text>{}
	public static class ProxyReducerFloatUTF8 extends GenericProxyReducer <FloatWritable, UTF8>{}
	public static class ProxyReducerFloatVInt extends GenericProxyReducer <FloatWritable, VIntWritable>{}
	public static class ProxyReducerFloatVLong extends GenericProxyReducer <FloatWritable, VLongWritable>{}

	// Proxy Reducer IntWritable
	public static class ProxyReducerIntDouble extends GenericProxyReducer <IntWritable, DoubleWritable>{}
	public static class ProxyReducerIntFloat extends GenericProxyReducer <IntWritable, FloatWritable>{}
	public static class ProxyReducerIntInt extends GenericProxyReducer <IntWritable, IntWritable>{}
	public static class ProxyReducerIntLong extends GenericProxyReducer <IntWritable, LongWritable>{}
	public static class ProxyReducerIntText extends GenericProxyReducer <IntWritable, Text>{}
	public static class ProxyReducerIntUTF8 extends GenericProxyReducer <IntWritable, UTF8>{}
	public static class ProxyReducerIntVInt extends GenericProxyReducer <IntWritable, VIntWritable>{}
	public static class ProxyReducerIntVLong extends GenericProxyReducer <IntWritable, VLongWritable>{}

	// Proxy Reducer LongWritable
	public static class ProxyReducerLongDouble extends GenericProxyReducer <LongWritable, DoubleWritable>{}
	public static class ProxyReducerLongFloat extends GenericProxyReducer <LongWritable, FloatWritable>{}
	public static class ProxyReducerLongInt extends GenericProxyReducer <LongWritable, IntWritable>{}
	public static class ProxyReducerLongLong extends GenericProxyReducer <LongWritable, LongWritable>{}
	public static class ProxyReducerLongText extends GenericProxyReducer <LongWritable, Text>{}
	public static class ProxyReducerLongUTF8 extends GenericProxyReducer <LongWritable, UTF8>{}
	public static class ProxyReducerLongVInt extends GenericProxyReducer <LongWritable, VIntWritable>{}
	public static class ProxyReducerLongVLong extends GenericProxyReducer <LongWritable, VLongWritable>{}

	// Proxy Reducer Text
	public static class ProxyReducerTextDouble extends GenericProxyReducer <Text, DoubleWritable>{}
	public static class ProxyReducerTextFloat extends GenericProxyReducer <Text, FloatWritable>{}
	public static class ProxyReducerTextInt extends GenericProxyReducer <Text, IntWritable>{}
	public static class ProxyReducerTextLong extends GenericProxyReducer <Text, LongWritable>{}
	public static class ProxyReducerTextText extends GenericProxyReducer <Text, Text>{}
	public static class ProxyReducerTextUTF8 extends GenericProxyReducer <Text, UTF8>{}
	public static class ProxyReducerTextVInt extends GenericProxyReducer <Text, VIntWritable>{}
	public static class ProxyReducerTextVLong extends GenericProxyReducer <Text, VLongWritable>{}

	// Proxy Reducer UTF8
	public static class ProxyReducerUTF8Double extends GenericProxyReducer <UTF8, DoubleWritable>{}
	public static class ProxyReducerUTF8Float extends GenericProxyReducer <UTF8, FloatWritable>{}
	public static class ProxyReducerUTF8Int extends GenericProxyReducer <UTF8, IntWritable>{}
	public static class ProxyReducerUTF8Long extends GenericProxyReducer <UTF8, LongWritable>{}
	public static class ProxyReducerUTF8Text extends GenericProxyReducer <UTF8, Text>{}
	public static class ProxyReducerUTF8UTF8 extends GenericProxyReducer <UTF8, UTF8>{}
	public static class ProxyReducerUTF8VInt extends GenericProxyReducer <UTF8, VIntWritable>{}
	public static class ProxyReducerUTF8VLong extends GenericProxyReducer <UTF8, VLongWritable>{}

	// Proxy Reducer VIntWritable
	public static class ProxyReducerVIntDouble extends GenericProxyReducer <VIntWritable, DoubleWritable>{}
	public static class ProxyReducerVIntFloat extends GenericProxyReducer <VIntWritable, FloatWritable>{}
	public static class ProxyReducerVIntInt extends GenericProxyReducer <VIntWritable, IntWritable>{}
	public static class ProxyReducerVIntLong extends GenericProxyReducer <VIntWritable, LongWritable>{}
	public static class ProxyReducerVIntText extends GenericProxyReducer <VIntWritable, Text>{}
	public static class ProxyReducerVIntUTF8 extends GenericProxyReducer <VIntWritable, UTF8>{}
	public static class ProxyReducerVIntVInt extends GenericProxyReducer <VIntWritable, VIntWritable>{}
	public static class ProxyReducerVIntVLong extends GenericProxyReducer <VIntWritable, VLongWritable>{}

	// Proxy Reducer VLongWritable
	public static class ProxyReducerVLongDouble extends GenericProxyReducer <VLongWritable, DoubleWritable>{}
	public static class ProxyReducerVLongFloat extends GenericProxyReducer <VLongWritable, FloatWritable>{}
	public static class ProxyReducerVLongInt extends GenericProxyReducer <VLongWritable, IntWritable>{}
	public static class ProxyReducerVLongLong extends GenericProxyReducer <VLongWritable, LongWritable>{}
	public static class ProxyReducerVLongText extends GenericProxyReducer <VLongWritable, Text>{}
	public static class ProxyReducerVLongUTF8 extends GenericProxyReducer <VLongWritable, UTF8>{}
	public static class ProxyReducerVLongVInt extends GenericProxyReducer <VLongWritable, VIntWritable>{}
	public static class ProxyReducerVLongVLong extends GenericProxyReducer <VLongWritable, VLongWritable>{}

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
            new  GenericProxyReducer<Text,LongWritable>();
        valueMap.put( LongWritable.class.getCanonicalName(), gMapTextLong.getClass());

        // Text, Int
        GenericProxyReducer<Text,IntWritable> gMapTextInt = 
            new  GenericProxyReducer<Text,IntWritable>();
        valueMap.put( IntWritable.class.getCanonicalName(), gMapTextInt.getClass());

        // Text, Float
        GenericProxyReducer<Text,FloatWritable> gMapTextFloat = 
            new  GenericProxyReducer<Text,FloatWritable>();
        valueMap.put( FloatWritable.class.getCanonicalName(), gMapTextFloat.getClass());

        // Text, Text
        GenericProxyReducer<Text,Text> gMapTextText = 
            new  GenericProxyReducer<Text,Text>();
        valueMap.put( Text.class.getCanonicalName(), gMapTextText.getClass());

        proxyReduceMapping.put(Text.class.getCanonicalName(), valueMap); 

    }
    // Key : Integer 
    private void addProxyReducerIntKeyOutMapping() throws Exception {
        Map<String, Class <? extends Reducer>> valueMap = new HashMap<String, Class <? extends Reducer>>();

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

    }
    // Key : Long 
    private void addProxyReducerLongKeyOutMapping() throws Exception {
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

    }

    // Key : Float 
    private void addProxyReducerFloatKeyOutMapping() throws Exception {
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

    }
    // add proxy reduce end
    //==============================================================================================

}


