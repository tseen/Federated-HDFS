/*******************************************************
 * Copyright (C) 2016 High Performance Parallel and Distributed System Lab, National Cheng Kung University
 *******************************************************/
package ncku.hpds.fed.MRv2.proxy;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class GenericProxyMapperSeq<T1, T2, T3,T4> extends Mapper<T1, T2, T3, T4>{

	
    public GenericProxyMapperSeq(Class<T3> keyClz, Class<T4> valueClz) throws Exception {
      
    }
  
    public void stringToKey(String in, T3 key){
 
    }
    /*
     * 
     * */
    @Override
	public void setup(Context context){
	
	}
    @Override
	public void map(T1 key, T2 value, Context context ) throws IOException,InterruptedException{
    //	System.out.println("K:"+key.toString()+" __V:"+value.toString());	
			context.write( (T3)key, (T4)value );
		
	}
}
