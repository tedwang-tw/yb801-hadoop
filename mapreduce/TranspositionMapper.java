import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TranspositionMapper extends Mapper<LongWritable,Text,IntWritable,Text> {
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] inf = value.toString().split(":");
		String keyWord = inf[0].trim();
		for(int i=1;i<inf.length;i++) {
			String[] inInf = inf[i].split(",");
			String outputInf = keyWord.concat(",").concat(inInf[2]);
			
			context.write(new IntWritable(Integer.parseInt(inInf[0])),new Text(outputInf));
		}// end of for-loop for setting output key-value for method map()
	}// end of method map()	
}// end of class TranspositionMapper