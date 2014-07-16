import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.*;

public class DXD_test extends Configured implements Tool {

	// static Variables
	static double[][] localDocVector; // Setup DistributeCache File and get
										// Array[][]
	static int reducerNum = 10; // Set Reducer Number

	// Mapper
	static class DXD_Mapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		// Method => Counting CosineSimilarity
		public double cosineSimilarity(double[] docVector1, double[] docVector2) {
			double dotProduct = 0.0;
			double magnitude1 = 0.0;
			double magnitude2 = 0.0;
			double cosineSimilarity = 0.0;

			for (int i = 0; i < docVector1.length; i++) {
				dotProduct += docVector1[i] * docVector2[i]; // a*b
				magnitude1 += Math.pow(docVector1[i], 2); // a^2
				magnitude2 += Math.pow(docVector2[i], 2); // b^2
			}
			magnitude1 = Math.sqrt(magnitude1); // sqrt(a^2)
			magnitude2 = Math.sqrt(magnitude2); // sqrt(b^2)

			if (magnitude1 != 0.0 && magnitude2 != 0.0)
				cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
			else
				return 0.0;
			return cosineSimilarity;
		}// end of method cosineSimilarity()

		// Method => Loading Local's TF*IDF Document
		public double[][] localDoc(String path) {
			File inputFile = new File(path);
			String str = null;
			List<String> docList = new ArrayList<String>();
			try {
				BufferedReader in = new BufferedReader(
						new FileReader(inputFile));
				while ((str = in.readLine()) != null)
					docList.add(str);
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			double[][] docVector = new double[docList.size()][docList.get(0)
					.split(",").length];
			for (int i = 0; i < docVector.length; i++) {
				String[] rowData = docList.get(i).split(",");
				for (int j = 0; j < rowData.length; j++) {
					docVector[i][j] = Double.parseDouble(rowData[j]);
				}
			}
			return docVector;
		}// end of method localDoc()

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Path[] path = DistributedCache.getLocalCacheFiles(context
					.getConfiguration()); // set path for distributed cache data
			localDocVector = localDoc(path[0].toString());
		}// end of Setup()

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Path[] path = DistributedCache.getLocalCacheFiles(context
			// .getConfiguration()); // set path for distributed cache data
			String[] line_split = value.toString().split(",");
			double[] docVector = new double[line_split.length - 1]; // line_split[0]
																	// is KEY
			double[] sim_matrix = new double[localDocVector.length];
			StringBuilder sb = new StringBuilder();

			for (int i = 1; i < line_split.length; i++)
				docVector[i - 1] = Double.parseDouble(line_split[i]);

			// counting cosineSimilarity
			for (int i = 0; i < localDocVector.length; i++)
				sim_matrix[i] = cosineSimilarity(docVector, localDocVector[i]);

			for (double sm : sim_matrix)
				sb.append(String.format("%.6f", sm)).append(",");
			sb.deleteCharAt(sb.length() - 1);

			context.write(
					new IntWritable(Integer.parseInt(line_split[0].toString())),
					new Text(sb.toString()));
		}
	}// end of Mapper

	// Partioner
	static class DXD_Partitioner extends Partitioner<IntWritable, Text> {

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			int result = -1;
			int index = Integer.parseInt(key.toString()) - 1;
			int section = localDocVector.length / numPartitions;
			System.out.println("numPartitions--" + numPartitions);
			for (int i = 0; i < numPartitions; i++) {
				if (index / section == i)
					result = i;
			}
			System.out.println("result--" + result);

			return (result != -1) ? result : numPartitions - 1;
		}

	}// end of Partitioner

	// Reducer
//	static class DXD_Reducer extends
//			Reducer<IntWritable, Text, IntWritable, Text> {
//
//		@Override
//		public void reduce(IntWritable key, Iterable<Text> values,
//				Context context) throws IOException, InterruptedException {
//
//			for (Text value : values) {
//				context.write(key, new Text(value));
//			}
//		}// end of reduce
//	}// end of DXD_Reducer

	// Main
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DXD_test(), args);
		System.exit(exitCode);
	}// end of main

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "DxD_test");
		job.setJarByClass(DXD_test.class);

		DistributedCache.addCacheFile(new URI("input/tfidf.txt"),
				job.getConfiguration()); 				// use files on hdfs
		// DistributedCache.addLocalFiles(conf,
		// "/home/cloudera/pq/myTest/docs/tfidf.txt");	// use files from local

		job.setPartitionerClass(DXD_Partitioner.class);
		job.setMapperClass(DXD_Mapper.class);
		// job.setReducerClass(DXD_Reducer.class);

		// job.setNumMapTasks(2);
		job.setNumReduceTasks(reducerNum);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}// end of run

}
