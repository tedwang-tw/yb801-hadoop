import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClassificationTFIDFReducer extends Reducer<Text,Text,Text,Text> {
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {

		List<String> wordsInf = new ArrayList<String>();
		
		for(Text value : values) {
			String vs = value.toString();
			wordsInf.add(vs);
		}// end of for-loop for getting value from input Text
		
		List<String> wordInf = MRTools.getWordInf(wordsInf);
		StringBuilder sb = new StringBuilder(300);
		for(String inf : wordInf) {
			sb.append(inf);
		}// end of for-loop for output
		
		context.write(new Text(key),new Text(sb.toString()));
	}// end of method reduce()
}// end of class ClassificationIDFReducer