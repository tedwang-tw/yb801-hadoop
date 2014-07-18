import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

public class CreateSequenceFile {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		try {
			
			File file = new File("/home/cloudera/big-data-analysis/output/jobs");
			BufferedReader bf = new BufferedReader(new FileReader(file));
			String str;
			List<NamedVector> jobs = new ArrayList<NamedVector>();
			NamedVector job;	
			String[] metadataArray;
			double[] outputValues;
			
			while((str=bf.readLine()) != null) {
				metadataArray = str.split(",");
				outputValues = new double[metadataArray.length-1];
			
				for(int i=1;i<metadataArray.length;i++) {
					outputValues[i-1] = Double.parseDouble((metadataArray[i].split(":"))[1]);
				}// end of for-loop for output data
					
				job = new NamedVector(new DenseVector(outputValues),metadataArray[0].trim());
				jobs.add(job);	
				System.out.println(str);
			}// end of while-loop for reading data	
			
			Path outputpath = new Path("/home/cloudera/big-data-analysis/output/jobsSequenceFile");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Writer writer = new SequenceFile.Writer(fs,conf,outputpath,Text.class,VectorWritable.class);
			VectorWritable vec = new VectorWritable();
			
			for(NamedVector vector : jobs) {
				vec.set(vector);
				writer.append(new Text(vector.getName()),vec);
			}// end of for-loop for putting vectors into vectorWritable from jobs
			
			writer.close();
			bf.close();
			
		} catch(IOException e) {
			System.out.println("IOException in CreateSequenceFile" + e.toString());
			System.exit(-1);
		}// end of try-catch				
	}// end of method main()
}// end of class CreateSequenceFile

/*
public class CreateSequenceFile {
		
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		try {
			String uri = args[0];	// "hdfs://localhost.localdomain:8020/user/cloudera"
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri),conf);
			BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
			String str;
			
			while((str=bf.readLine()) != null) {
				String[] metadataArray = str.split(",");
				double[] outputValues = new double[metadataArray.length-1];
			
				for(int i=1;i<metadataArray.length;i++) {
					outputValues[i] = Double.parseDouble(metadataArray[i].split(",")[2]);
				}// end of for-loop for output data
	
				List<NamedVector> jobs = new ArrayList<NamedVector>();
				NamedVector job;
				
				job = new NamedVector(new DenseVector(outputValues),metadataArray[0]);
				jobs.add(job);	
	
				Path outputpath = new Path("output/jobskmeans");
				SequenceFile.Writer writer = new SequenceFile.Writer(fs,conf,outputpath,Text.class,VectorWritable.class);
				VectorWritable vec = new VectorWritable();
				
				for(NamedVector vector : jobs) {
					vec.set(vector);
					writer.append(new Text(vector.getName()),vec);
				}// end of for-loop for putting vectors into vectorWritable from jobs
				writer.close();
			}// end of while-loop for reading data			
		} catch(IOException e) {
			System.out.println("IOException in CreateSequenceFile" + e.toString());
			System.exit(-1);
		}// end of try-catch				
	}// end of method main()
}// end of class CreateSequenceFile
*/