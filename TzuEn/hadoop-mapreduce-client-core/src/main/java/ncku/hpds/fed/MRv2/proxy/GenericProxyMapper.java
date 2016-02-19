package ncku.hpds.fed.MRv2.proxy;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class GenericProxyMapper<T3,T4> extends Mapper<Object, Text, T3, T4>{

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
       // mKey = mKeyClz.newInstance();
        if(mValueClz.equals(NullWritable.class)){
        //	mValue = mValueClz.newInstance();
        	
        	Constructor<NullWritable> constructor 
        		= NullWritable.class.getDeclaredConstructor(new Class[0]);
        	constructor.setAccessible(true);
        	mValue = (T4) constructor.newInstance(new Object[0]);
        	mValueClzName = "NullWritable";
        }
        else{
        	mValue = mValueClz.newInstance();
            mValueClzName = mValueClz.getCanonicalName();
        }
        if(mKeyClz.equals(NullWritable.class)){
        //	mValue = mValueClz.newInstance();
        	
        	Constructor<NullWritable> constructor 
        		= NullWritable.class.getDeclaredConstructor(new Class[0]);
        	constructor.setAccessible(true);
        	mKey = (T3) constructor.newInstance(new Object[0]);
        	mKeyClzName = "NullWritable";
        }
        else{
        	mKey = mKeyClz.newInstance();
        	mKeyClzName = mKeyClz.getCanonicalName();
        }
        //mKeyClzName = mKeyClz.getCanonicalName();
    }
  
    public void stringToKey(String in, T3 key){
 
    }
    /*
     * 
     * */
    @Override
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		if(conf.get("proxy_seperator")!=null) {
            try {
			    mSeperator = conf.get("proxy_seperator");
            } catch ( Exception e ) {
                mSeperator = "||";
            }
		}
	}
    @Override
	public void map(Object key, Text value, Context context ) throws IOException,InterruptedException{

		try {
			// get key
			String valueStr = value.toString();
			System.out.println("valueStr:"+valueStr);
			int firstSeperatePos = valueStr.indexOf("=");
			String keyPart = valueStr.substring(0, firstSeperatePos);
			String valuePart = valueStr.substring(firstSeperatePos+1);

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
			} else if ( mKeyClzName.contains("NullWritable") ) {
			}
			else{
				stringToKey(keyPart, mKey);				
			}

			StringTokenizer itr = new StringTokenizer( valuePart, mSeperator );  
			String nextToken = "";

			while (itr.hasMoreTokens()) {
				nextToken = itr.nextToken() ;

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
				//else if ( mIsNullWritable ) {
					//mValue = (T4) NullWritable.get() ;
				//	context.write( mKey, mValue );
			//	}
				//----------------------------------------------------------
				System.out.println("KEY:"+mKey.toString()+" VALUE:"+mValue.toString());
				context.write( mKey, mValue );
			}

		} catch ( Exception e ) {
			e.printStackTrace();
			System.out.println("meet error skip it"); 
		}
	}
}
