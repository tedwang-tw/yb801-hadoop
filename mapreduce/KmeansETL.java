import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeansETL extends Configured implements Tool {

	public static int keyNum;
	
	static class KmeansETLMapper extends Mapper<LongWritable,Text,IntWritable,Text> {
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			
			String[] inf = value.toString().split(":");
			String keyWord = inf[0].trim();
			keyNum = inf.length - 1;
			
			for(int i=1;i<inf.length;i++) {
				String[] inInf = inf[i].split(",");
				String outputInf = keyWord.concat(",").concat(inInf[1]).concat(",").concat(inInf[2]);
				
				context.write(new IntWritable(Integer.parseInt(inInf[0])),new Text(outputInf));
			}// end of for-loop for setting output key-value for method map()
		}// end of method map()	
	}// end of inner class KmeansETLMapper	
	
	static class KmeansETLPartitioner extends Partitioner<IntWritable,Text>{
		
		public void setKeyNum(int num) { keyNum = num;}
		
		@Override
		public int getPartition(IntWritable key, Text value, int reducerNum) {
			
			int result = -1;
			int index = Integer.parseInt(key.toString()) - 1;
			int section = keyNum/reducerNum;

			for (int i = 0; i < reducerNum; i++) {
				if (index / section == i)
					result = i;
			}// end of finding result

			return (result != -1) ? result : reducerNum - 1;		
		}// end of method getPartition()
	}// end of class KmeansETLPartitioner
	
	static class KmeansETLReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
		@Override
		public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
		
			Set<String> metadata = new HashSet<String>();
			
			for(Text value : values) {
				metadata.add(value.toString());
			}// end of for-loop for getting value
			
			String[] metadataArray = new String[metadata.size()];
			
			Iterator<String> bfsort = metadata.iterator();
			int i = 0;
			
			while(bfsort.hasNext()) {
				metadataArray[i] = bfsort.next();
				i++;
			}// end of while-loop for putting value into metadataArray
			
			Arrays.sort(metadataArray);
			
			StringBuilder sb = new StringBuilder(300);

			for(i=0;i<metadataArray.length;i++) {
				String[] outputValue = metadataArray[i].split(","); 
			//	if(i==0)
			//		sb.append(outputValue[0]).append(":").append(outputValue[2]);
			//	else
					sb.append(",").append(outputValue[0]).append(":").append(outputValue[2]);
			}// end of for-loop for output data
			
			context.write(new IntWritable(Integer.parseInt(key.toString())),new Text(sb.toString()));	
		}// end of method reduce()
	}// end of inner class SimilarityETLReducer
	@SuppressWarnings({"unused","deprecation"})
	@Override
	public int run(String[] args) throws URISyntaxException,IOException,InterruptedException,ClassNotFoundException {
	
		if(args.length != 2) {
			System.err.println("Usage: hadoop jar jarname.jar classname -files <distributed cache path> <input path> <out path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();	
		Job job = new Job(conf,"ETL before  Kmeans cluster");		// create a job for mapreduce by using the configuration
		job.setJarByClass(KmeansETL.class);						// set class name 
		
		if(job==null) { return -1;}

		FileInputFormat.addInputPath(job,new Path(args[0]));		// input path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));		// output path

	//  job.setNumMapTasks(5);		// set number of mapper	(?)
	//	job.setNumReduceTasks(2);	// set number of reducer
	
		job.setMapperClass(KmeansETLMapper.class);					// set mapper
		job.setPartitionerClass(KmeansETLPartitioner.class);		// set partitioner
		job.setReducerClass(KmeansETLReducer.class);				// set reducer

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);		
			
		return job.waitForCompletion(true)?0:1;
	}// end of method run()
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new KmeansETL(),args);
		System.exit(exitCode);
	}// end of method main()
}// end of class KmeansETLMR