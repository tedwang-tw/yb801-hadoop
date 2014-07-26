
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

public class CreateSequenceFileS {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		
		if(args.length != 2) {
			System.err.println("CreateSequenceFile Usage: jar jarname.jar classname <input path> <out path>");
			System.exit(-1);}
		
		try {			
			File file = new File(args[0]);
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
					outputValues[i-1] = Double.parseDouble(metadataArray[i].trim());
				}// end of for-loop for output data
					
				job = new NamedVector(new DenseVector(outputValues),metadataArray[0].trim());
				jobs.add(job);	
			}// end of while-loop for reading data	
			
			Path outputpath = new Path(args[1]);
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