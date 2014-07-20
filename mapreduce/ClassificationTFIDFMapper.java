import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassificationTFIDFMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	protected static int mapCount = 0;
	protected Map<String,String> keyWords = new HashMap<String,String>();		// key words
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		@SuppressWarnings("deprecation")
		Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());		// set path for distributed cache data
		String str;
		BufferedReader buf;

		try {
			buf = new BufferedReader(new FileReader(paths[0].toString()));

			while((str=buf.readLine()) != null) {
				String[] strinf = str.split(",");
				keyWords.put(strinf[1],strinf[0]);
			}// end of while-loop for putting value to keyWords

			buf.close();
		} catch(IOException e) { 
			System.out.println("IOException in reading distributed cache data" + e.toString());
			System.exit(-1);
		}// end of try-catch for IOException
		
		str = null;
		paths = null;
		
	}// end of method setup()
	@SuppressWarnings("rawtypes")
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

		String[] valueString = value.toString().split(",");

	//	Set setKW = (Set)keyWords.entrySet();
		Iterator iteratorKW =((Set)keyWords.entrySet()).iterator();
		StringBuilder sb = new StringBuilder();
		
		while(iteratorKW.hasNext()) {
			Entry entry = (Entry)iteratorKW.next();
			sb.append((String)entry.getValue()).append(",").append(valueString[0]).append(",0");
			keyWords.put((String)entry.getKey(),sb.toString());
			sb.delete(0,sb.length());
		}// end of while-loop for set keyWords

		Map<String,String> jobInf = MRTools.getJobInf(valueString,keyWords);
	//	Set set = (Set)jobInf.entrySet();
		Iterator iterator = ((Set)jobInf.entrySet()).iterator();

		while(iterator.hasNext()) {
			Entry entry = (Entry)iterator.next();
			context.write(new Text((String)entry.getKey()),new Text((String)entry.getValue()));
		}// end of while-loop for output
		
		valueString = null;
		iteratorKW = null;
		jobInf = null;
		iterator = null;
		sb = null;
		
		mapCount ++;
		
		if(mapCount%1000==0){
			System.gc();
		}
		
	}// end of method map()	
}// end of class ClassificationTFMapper