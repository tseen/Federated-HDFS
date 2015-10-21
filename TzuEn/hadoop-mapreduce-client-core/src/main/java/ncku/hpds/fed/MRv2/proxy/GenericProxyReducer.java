package ncku.hpds.fed.MRv2.proxy;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;

public class GenericProxyReducer<T1,T2> extends Reducer<T1,T2,Text,Text> {

	private StringBuffer sb = new StringBuffer();
    private Text mKey = new Text();
	private Text mValue = new Text();   

	private int count =0;
	private int MAX_COUNT = 999;
    private String mSeperator = "||";
    
    private void __reset() {
	    sb.setLength(0);  // clean up the String buffer
        count = 0;
    }

    private Class<T1> mKeyClz ;
    private Class<T2> mValueClz;
    private String mKeyClzName ;
    private String mValueClzName ; 

    public GenericProxyReducer(Class<T1> keyClz, Class<T2> valueClz) throws Exception {
        mKeyClz = keyClz;
        mValueClz = valueClz;
        mKeyClzName = mKeyClz.getCanonicalName();
        mValueClzName = mValueClz.getCanonicalName();
    }

    @Override
	public void setup(Context context){
        System.out.println("Start Proxy Reducer");
		Configuration conf = context.getConfiguration();
		if(conf.get("max_token")!=null) {
            MAX_COUNT = 999;
            try {
			    MAX_COUNT =Integer.parseInt(conf.get("max_token"));
                System.out.println("set max count : " + MAX_COUNT );
            } catch ( Exception e ) {
            }
		}
		if(conf.get("proxy_seperator")!=null) {
            try {
			    mSeperator = conf.get("proxy_seperator");
            } catch ( Exception e ) {
                mSeperator = "||";
            }
		}
        __reset();
	}
    @Override	
	public void reduce(T1 key, Iterable<T2> values, Context context) 
        throws IOException,InterruptedException{

        Iterator<T2> it = values.iterator();
        __reset();
        //----------------------------------------------------------
        // Partial Generic Mapper Keypart 
        if ( mKeyClzName.contains("DoubleWritable") ) {
            DoubleWritable tKey = (DoubleWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("FloatWritable") ) {
            FloatWritable tKey = (FloatWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("IntWritable") ) {
            IntWritable tKey = (IntWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("LongWritable") ) {
            LongWritable tKey = (LongWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("Text") ) {
            Text tKey = (Text) key;
            mKey.set(tKey.toString());

        } else if ( mKeyClzName.contains("UTF8") ) {
            UTF8 tKey = (UTF8) key;
            mKey.set(tKey.toString());

        } else if ( mKeyClzName.contains("VIntWritable") ) {
            VIntWritable tKey = (VIntWritable) key;
            mKey.set(String.valueOf(tKey.get()));

        } else if ( mKeyClzName.contains("VLongWritable") ) {
            VLongWritable tKey = (VLongWritable) key;
            mKey.set(String.valueOf(tKey.get()));
        }
        System.out.println("mKey.toString : " + mKey.toString());
        while(it.hasNext()) {	
            T2 value = it.next();
            //sb.append(value).append(mSeperator);   
            //----------------------------------------------------------
            // Partial Generic Mapper Value Part
            if ( mValueClzName.contains("DoubleWritable") ) {
                DoubleWritable tValue = (DoubleWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("DoubleWritable : " + tValue.get());

            } else if ( mValueClzName.contains("FloatWritable") ) {
                FloatWritable tValue = (FloatWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("FloatWritable : " + tValue.get());

            } else if ( mValueClzName.contains("IntWritable") ) {
                IntWritable tValue = (IntWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("IntWritable : " + tValue.get());

            } else if ( mValueClzName.contains("LongWritable") ) {
                LongWritable tValue = (LongWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("LongWritable : " + tValue.get());

            } else if ( mValueClzName.contains("Text") ) {
                Text tValue = (Text) value;
                sb.append(tValue.toString()).append(mSeperator);
                System.out.println("Text : " + tValue.toString());

            } else if ( mValueClzName.contains("UTF8") ) {
                UTF8 tValue = (UTF8) value;
                sb.append(tValue.toString()).append(mSeperator);
                System.out.println("UTF8 : " + tValue.toString());

            } else if ( mValueClzName.contains("VIntWritable") ) {
                VIntWritable tValue = (VIntWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("VIntWritable : " + tValue.get());

            } else if ( mValueClzName.contains("VLongWritable") ) {
                VLongWritable tValue = (VLongWritable) value;
                sb.append(String.valueOf(tValue.get())).append(mSeperator);
                System.out.println("VLongWritable : " + tValue.get());
            }
            //----------------------------------------------------------
            count++;
            if ( count == MAX_COUNT ) {
                context.write(mKey, mValue);
                __reset();
            }
        }
        if ( sb.length() > 0 ) {
            mValue.set(sb.toString());
            context.write(mKey, mValue);
            __reset();
        }
	}
}
