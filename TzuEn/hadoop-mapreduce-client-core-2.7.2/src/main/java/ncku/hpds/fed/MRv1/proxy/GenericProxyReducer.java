package ncku.hpds.fed.MRv1.proxy;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.JobConf;

public class GenericProxyReducer<T1,T2> extends MapReduceBase implements Reducer<T1,T2,Text,Text> {

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

    public GenericProxyReducer() {
    }

    @Override
	public void configure(JobConf jobConf){
        System.out.println("Start Proxy Reducer");
		Configuration conf = new Configuration(jobConf);
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
	
	public void reduce(T1 key, Iterator<T2> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException{

        __reset();
        mKey.set(key.toString());
        while(values.hasNext()) {	
            T2 value = values.next();
            sb.append(value).append(mSeperator);   
            count++;
            if ( count == MAX_COUNT ) {
                mValue.set(sb.toString());
                output.collect(mKey, mValue);
                __reset();
            }
        }
        if ( sb.length() > 0 ) {
            mValue.set(sb.toString());
            output.collect(mKey, mValue);
            __reset();
        }
	}
}
