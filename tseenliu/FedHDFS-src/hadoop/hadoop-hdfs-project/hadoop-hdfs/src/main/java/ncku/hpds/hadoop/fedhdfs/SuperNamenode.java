package ncku.hpds.hadoop.fedhdfs;

import java.io.*;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.*;

import com.google.common.annotations.VisibleForTesting;

public class SuperNamenode {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		FSDataInputStream in = null;
		try{
    		in = fs.open(new Path(uri));
    		IOUtils.copyBytes(in, System.out, 4096, false);
    		
        }catch(Exception e){
        	IOUtils.closeStream(in);
         }
	}
}






/*
 import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class SuperNamenode {
        public static void main (String [] args) throws Exception{
        	
        	String uri = args[0];
        	Configuration conf = new Configuration();
        	FileSystem fs = FileSystem.get(URI.create(uri),conf);
        	FSDataInputStream in = null;
        	
        	try{
        		in = fs.open(new Path(uri));
        		IOUtils.copyBytes(in, System.out, 4096, false);
                	
                        
            }catch(Exception e){
            	IOUtils.closeStream(in);
             }
        }
} 
 */
