import java.io.*;
import java.util.*;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Set;
import java.util.Map.Entry;
import java.net.*;
//import java.net.URI;
//import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.*;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF extends Configured implements Tool {
	
	static class TFIDFMapper extends Mapper<LongWritable,Text,Text,Text> {

	//	@Override
	//	protected void cleanup(Context context) throws IOException,InterruptedException { DistributedCache.purgeCache(conf);};
	//	DistributedCache.purgeCache(conf);
	/*
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			temperatureYear = readDistrubutedCacheFile(context);
		}// end of method setup()
	*/
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());		// set path for distributed cache data
			Map<String,String> keyWords = new HashMap<String,String>();		// key words
			String str = null;

			String[] jobWords = MRTools.getStringFromText(value);
			String jobNo = jobWords[0];

			try {
				BufferedReader buf = new BufferedReader(new FileReader(paths[0].toString()));

				while((str=buf.readLine()) != null) { 
					String values = str.concat(",").concat(jobNo).concat(",0,0,false");
					keyWords.put(str,values);
				}// end of while-loop for putting value to keyWords
				buf.close();
			} catch(IOException e) { 
				System.out.println("IOException in reading distributed cache data" + e.toString());
				System.exit(-1);
			}// end of try-catch for IOException

			Map<String,String> jobInf = MRTools.getJobInf(jobWords,keyWords);
			
			Set set = (Set)jobInf.entrySet();
			Iterator iterator = set.iterator();

			while(iterator.hasNext()) {
				Entry me = (Entry)iterator.next();
				String kw = (String)me.getKey();
				String valueInf = (String)me.getValue();
				context.write(new Text(kw),new Text(valueInf));
			}// end of while-loop for output
		}// end of method map()
	}// end of inner class TFIDFMapper

	static class TFIDFReducer extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {

			List<String> wordsInf = new ArrayList<String>();
			
			for(Text value : values) {
				String vs = value.toString();
				wordsInf.add(vs);
			}// end of for-loop for getting value from input Text
			
			List<String> wordInf = MRTools.getWordInf(wordsInf);
		// line 94~97 for printing tf*idf only
			String finalvalue = MRTools.getTogetherTFIDF(wordInf);

			context.write(new Text(key),new Text(finalvalue));
		/*
		// print metadata in output data
			Iterator<String> iterator = wordInf.iterator();
			
			while(iterator.hasNext()) {
				String finalvalue = iterator.next();
				context.write(new Text(key),new Text(finalvalue));
			}// end of while-loop for output
		*/
		}// end of method reduce()
	}// end of inner class TFIDFReducer

	@Override
	public int run(String[] args) throws URISyntaxException,IOException,InterruptedException,ClassNotFoundException {
	
		if(args.length != 2) {
			System.err.println("Usage: hadoop jar jarname.jar classname -files <distributed cache path> <input path> <out path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();	
		Job job = new Job(conf,"TFIDF");	// create a job for mapreduce by using the configuration
		job.setJarByClass(TFIDF.class);		// set class name 
		
		if(job==null) { return -1;}

		DistributedCache.addCacheFile(new URI("input/side-data/keywords_merge.txt"),job.getConfiguration());	// use files on hdfs

		FileInputFormat.addInputPath(job,new Path(args[0]));		// input path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));		// output path

	//  job.setNumMapTasks(5);		// set number of mapper	(?)
	//	job.setNumReduceTasks(2);	// set number of reducer
	
		job.setMapperClass(TFIDFMapper.class);				// set mapper
	//	job.setPartitionerClass(TFIDFPartitioner.class);	// set partitioner
	//	job.setCombinerClass(TFIDFReducer.class);			// set combiner
		job.setReducerClass(TFIDFReducer.class);			// set reducer

	/*	set out put data style	*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	/*	set out put data style for mapper	*/
	//	job.setMapOutputKeyClass(Text.class);
	//	job.setMapOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)?0:1;
	}// end of method run()
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TFIDF(),args);
		System.exit(exitCode);
	}// end of method main()
}// end of class TFIDF
