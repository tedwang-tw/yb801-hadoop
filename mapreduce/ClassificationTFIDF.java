import java.io.*;
import java.net.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClassificationTFIDF extends Configured implements Tool {
	@SuppressWarnings({ "unused", "deprecation" })
	@Override
	public int run(String[] args) throws URISyntaxException,IOException,InterruptedException,ClassNotFoundException {
	
		if(args.length != 2) {
			System.err.println("Usage: hadoop jar jarname.jar classname <input path> <out path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();	
		Job job = new Job(conf,"Classification TFIDF");	// create a job for mapreduce by using the configuration
		job.setJarByClass(ClassificationTFIDF.class);		// set class using on hdfs
		
		if(job==null) { return -1;}

		DistributedCache.addCacheFile(new URI("input/keywords_merge_sort_index.txt"),job.getConfiguration());	// use distributed cache files on hdfs

		FileInputFormat.addInputPath(job,new Path(args[0]));		// input file
		FileOutputFormat.setOutputPath(job,new Path(args[1]));		// output path

	//  job.setNumMapTasks(5);		// set number of mapper	(?)
	//	job.setNumReduceTasks(2);	// set number of reducer
	
		job.setMapperClass(ClassificationTFIDFMapper.class);		// set mapper
		job.setReducerClass(ClassificationTFIDFReducer.class);		// set reducer
	/*	set out put data style	*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)?0:1;
	}// end of method run()
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ClassificationTFIDF(),args);
		System.exit(exitCode);
	}// end of method main()
}// end of class TFIDF