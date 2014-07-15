import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TranspositionReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
	@Override
	public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
		
		StringBuilder sb = new StringBuilder(300);
		
		for(Text value : values) {
			sb.append(":").append(value.toString());
		}// end of for-loop for output
		
		context.write(key,new Text(sb.toString()));
	}// end of method reduce()
}// end of class ClassificationIDFReducer