package ncku.hpds.hadoop.fedhdfs;

import java.util.ArrayList;

import org.apache.hadoop.util.RunJar;

public class SubmitJobsScheduler {
	
	public static void main(String[] args) throws Throwable {
		

        ArrayList arg = new ArrayList();

        String output="WordCountoutput";

        arg.add("share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar wordcount");

        arg.add("README.txt");
        arg.add(output);

        RunJar.main((String[])arg.toArray(new String[0]));

		
	}
}
