package ncku.hpds.fed.MRv1.proxy;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;

public class GenericProxyMapper<T3,T4> extends MapReduceBase implements Mapper<LongWritable, Text, T3, T4>{

    private T3 mKey ;
	private T4 mValue ;
    private Class<T3> mKeyClz ;
    private Class<T4> mValueClz;
    private String mSeperator = "||";
    private String mKeyClzName ;
    private String mValueClzName ; 
    public GenericProxyMapper(Class<T3> keyClz, Class<T4> valueClz) throws Exception {
        mKeyClz = keyClz;
        mValueClz = valueClz;
        mKey = mKeyClz.newInstance();
        mValue = mValueClz.newInstance();
        mKeyClzName = mKeyClz.getCanonicalName();
        mValueClzName = mValueClz.getCanonicalName();
    }

    @Override
	public void configure(JobConf jobConf){
		Configuration conf = new Configuration(jobConf);
		if(conf.get("proxy_seperator")!=null) {
            try {
			    mSeperator = conf.get("proxy_seperator");
            } catch ( Exception e ) {
                mSeperator = "||";
            }
		}
	}

	public void map(LongWritable key, Text value, OutputCollector<T3, T4> output, Reporter reporter) throws IOException{

        // get key
        String valueStr = value.toString();
        int firstTabPos = valueStr.indexOf("\t");
        String keyPart = valueStr.substring(0, firstTabPos);
        String valuePart = valueStr.substring(firstTabPos+1);

        //----------------------------------------------------------
        // Partial Generic Mapper Keypart 
        if ( mKeyClzName.contains("DoubleWritable") ) {
            DoubleWritable tKey = (DoubleWritable) mKey;
            tKey.set(Double.valueOf(keyPart));

        } else if ( mKeyClzName.contains("FloatWritable") ) {
            FloatWritable tKey = (FloatWritable) mKey;
            tKey.set(Float.valueOf(keyPart));

        } else if ( mKeyClzName.contains("IntWritable") ) {
            IntWritable tKey = (IntWritable) mKey;
            tKey.set(Integer.valueOf(keyPart));

        } else if ( mKeyClzName.contains("LongWritable") ) {
            LongWritable tKey = (LongWritable) mKey;
            tKey.set(Long.valueOf(keyPart));

        } else if ( mKeyClzName.contains("Text") ) {
            Text tKey = (Text) mKey;
            tKey.set(keyPart);

        } else if ( mKeyClzName.contains("UTF8") ) {
            UTF8 tKey = (UTF8) mKey;
            tKey.set(keyPart);

        } else if ( mKeyClzName.contains("VIntWritable") ) {
            VIntWritable tKey = (VIntWritable) mKey;
            tKey.set(Integer.valueOf(keyPart));

        } else if ( mKeyClzName.contains("VLongWritable") ) {
            VLongWritable tKey = (VLongWritable) mKey;
            tKey.set(Long.valueOf(keyPart));
        }
        //----------------------------------------------------------

		StringTokenizer itr = new StringTokenizer( valuePart, mSeperator );  
        String nextToken = "";

		while (itr.hasMoreTokens()) {
            nextToken = itr.nextToken() ;

            //----------------------------------------------------------
            // Partial Generic Mapper Value Part
            if ( mValueClzName.contains("DoubleWritable") ) {
                DoubleWritable tValue = (DoubleWritable) mValue;
                tValue.set(Double.valueOf(nextToken));

            } else if ( mValueClzName.contains("FloatWritable") ) {
                FloatWritable tValue = (FloatWritable) mValue;
                tValue.set(Float.valueOf(nextToken));

            } else if ( mValueClzName.contains("IntWritable") ) {
                IntWritable tValue = (IntWritable) mValue;
                tValue.set(Integer.valueOf(nextToken));

            } else if ( mValueClzName.contains("LongWritable") ) {
                LongWritable tValue = (LongWritable) mValue;
                tValue.set(Long.valueOf(nextToken));

            } else if ( mValueClzName.contains("Text") ) {
                Text tValue = (Text) mValue;
                tValue.set(nextToken);

            } else if ( mValueClzName.contains("UTF8") ) {
                UTF8 tValue = (UTF8) mValue;
                tValue.set(nextToken);

            } else if ( mValueClzName.contains("VIntWritable") ) {
                VIntWritable tValue = (VIntWritable) mValue;
                tValue.set(Integer.valueOf(nextToken));

            } else if ( mValueClzName.contains("VLongWritable") ) {
                VLongWritable tValue = (VLongWritable) mValue;
                tValue.set(Long.valueOf(nextToken));
            }
            //----------------------------------------------------------
			output.collect( mKey, mValue );
		}

	}
}
