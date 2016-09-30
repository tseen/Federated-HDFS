package ncku.hpds.fed.MRv2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class AbstractProxySelector {

	
	 
		abstract Class< ? extends Mapper> getProxyMapperClass(Class keyClz, Class valueClz) ;
	    abstract Class< ? extends Mapper> getProxyMapperClassSeq(Class keyClz, Class valueClz);
	    abstract Class< ? extends Reducer> getProxyReducerClass(Class keyClz, Class valueClz) ;
	    //----------------------------------------------------------------------- 

	    protected abstract void init();
	    //----------------------------------------------------------------------- 
	    //proxy map/reduce version2
	    protected abstract void addProxyMappers();
	    protected abstract void addProxyReducers();
	    //----------------------------------------------------------------------- 
	   // abstract void addProxyMapperMapping( Class keyClz, Class valueClz, Class<? extends Mapper> pmClz ) ;
	
	    //abstract void addProxyReducerMapping( Class keyClz, Class valueClz, Class<? extends Reducer> pmClz ) ;
	    //----------------------------------------------------------------------- 
		
	   
	}
