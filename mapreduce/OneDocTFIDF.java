import java.io.*;
import java.math.BigDecimal;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneDocTFIDF extends Configured implements Tool {
	static class TFIDFMapper extends Mapper<LongWritable,Text,Text,Text> {
		
		@SuppressWarnings({ "rawtypes", "deprecation" })
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());		// set path for distributed cache data
			BufferedReader in;
			Map<String,Double> counts = new HashMap<String,Double>();
			String[] inf = value.toString().split(",");
			String s;
			String[] old;
			String[] oldtf;
			
			try{
				in = new BufferedReader(new FileReader(paths[0].toString()));

				while((s=in.readLine()) != null) {
					counts.put(s.split(",")[1],0.0);
				}// end of while-loop for putting value to keyWords

				in.close();
			}catch(IOException e) {
				System.out.println("DistributedCache : " + e.toString());
			}// end of try-catch
			
			List<String> hasWords = new ArrayList<String>();
			
			for(int i=1;i<inf.length;i++) {
				if(counts.containsKey(inf[i])) {
					counts.put(inf[i],counts.get(inf[i])+1.0);
					hasWords.add(inf[i]);
			}}// end of for-loop for counting words
					
			Iterator it = ((Set)counts.entrySet()).iterator();
			
			while(it.hasNext()) {
				Entry e = (Entry)it.next();
			/*
				output key : String , jobNo + keyWordNo
				output value : double , tf
			*/
				if(hasWords.size()!=0)
					context.write(new Text((String)e.getKey()),new Text(inf[0]+","+((Double)e.getValue()/hasWords.size())));
				else
					context.write(new Text((String)e.getKey()),new Text(inf[0]+","+0));
			}// end of while-loop for finding tf
			
			try{
				in = new BufferedReader(new FileReader(paths[1].toString()));

				while((s=in.readLine()) != null) {
					old = s.split(":");
					for(int i=1;i<old.length;i++) {
						oldtf = old[i].split(",");
						context.write(new Text(old[0]),new Text(oldtf[0]+","+oldtf[1]));
				}}// end of while-loop for putting value to keyWords

				in.close();
			}catch(IOException e) {
				System.out.println("DistributedCache : " + e.toString());
			}// end of try-catch
			
			oldtf = null;
			old = null;
			it = null;
			s = null;
			hasWords = null;
			counts.clear();
			inf = null;
			
		}// end of method map()	
	}// end of inner class ClassificationTFMapper
	
	static class TFIDFPartitioner extends Partitioner<Text,Text> {
		@Override
		public int getPartition(Text key, Text value, int rNum) {
			return key.toString().hashCode()%rNum;
		}// end of method getPartition
	}// end of inner class CPartitioner
	
	static class TFIDFReducer extends Reducer<Text,Text,Text,NullWritable> {
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
			List<String> jobTFs = new ArrayList<String>();
			int hasWordJobs = 1;
			
			for(Text value : values) {
				jobTFs.add(value.toString());
			}// end of for-loop for getting value from values;
			
			for(String  jobTF : jobTFs) {
				BigDecimal tfNum = (new BigDecimal(jobTF.split(",")[1])).setScale(6,BigDecimal.ROUND_HALF_UP);
				if(tfNum.signum() != 0)
					hasWordJobs += 1;
			}// end of for-loop for count hasWordJobs
			
			double blog = (new BigDecimal(String.valueOf(jobTFs.size()))).divide(new BigDecimal(String.valueOf(hasWordJobs)),6,BigDecimal.ROUND_HALF_UP).doubleValue(); 
			BigDecimal idf = new BigDecimal(String.valueOf(Math.log(blog)));
			StringBuilder sb = new StringBuilder();
			sb.append(key.toString());
			
			for(String  jobTF : jobTFs) {
				sb.append(":").append(jobTF).append(",").append((new BigDecimal(jobTF.split(",")[1])).multiply(idf).setScale(6,BigDecimal.ROUND_HALF_UP));	
			}// end of for-loop for finding tfidf
			
			context.write(new Text(sb.toString()),null);
			
			sb.delete(0,sb.length());
			idf = null;
			jobTFs.clear();
		}// end of method reduce()
	}// end of inner class ClassificationIDFReducer
	@SuppressWarnings({ "unused", "deprecation" })
	@Override
	public int run(String[] args) throws URISyntaxException,IOException,InterruptedException,ClassNotFoundException {
	
		if(args.length != 2) {
			System.err.println("Usage: hadoop jar jarname.jar classname <input path> <out path>");
			System.exit(-1);}

		Configuration conf = new Configuration();	
		Job job = new Job(conf,"Cluster TFIDF");		// create a job for mapreduce by using the configuration
		job.setJarByClass(OneDocTFIDF.class);			// set class using on hdfs
		
		if(job==null) { return -1;}
	/*	use distributed cache files on hdfs	*/
		DistributedCache.addCacheFile(new URI("input/keywords_merge_sort_index.txt"),job.getConfiguration());
		DistributedCache.addCacheFile(new URI("output/cluster-tfidf/part-r-00000"),job.getConfiguration());

		FileInputFormat.addInputPath(job,new Path(args[0]));		// input file
		FileOutputFormat.setOutputPath(job,new Path(args[1]));		// output path

	//  job.setNumMapTasks(5);		// set number of mapper	(?)
	//	job.setNumReduceTasks(0);	// set number of reducer
	
		job.setMapperClass(TFIDFMapper.class);					// set mapper
		job.setPartitionerClass(TFIDFPartitioner.class);		// set partitioner
		job.setReducerClass(TFIDFReducer.class);				// set reducer
	/*	set out put data style	*/
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true)?0:1;
	}// end of method run()
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new OneDocTFIDF(),args);
		System.exit(exitCode);
	}// end of method main()
}// end of class ClusterTFIDF