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
	
	protected Map<String,String> keyWords = new HashMap<String,String>();		// key words

//	@Override
//	protected void cleanup(Context context) throws IOException,InterruptedException { DistributedCache.purgeCache(conf);};
//	DistributedCache.purgeCache(conf);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		@SuppressWarnings("deprecation")
		Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());		// set path for distributed cache data
		String str;

		try {
			BufferedReader buf = new BufferedReader(new FileReader(paths[0].toString()));

			while((str=buf.readLine()) != null) {
				keyWords.put(str,str);
			}// end of while-loop for putting value to keyWords

			buf.close();
		} catch(IOException e) { 
			System.out.println("IOException in reading distributed cache data" + e.toString());
			System.exit(-1);
		}// end of try-catch for IOException
	}// end of method setup()

	@SuppressWarnings("rawtypes")
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

		String[] valueString = value.toString().split(",");

		Set setKW = (Set)keyWords.entrySet();
		Iterator iteratorKW = setKW.iterator();
		
		while(iteratorKW.hasNext()) {
			Entry entry = (Entry)iteratorKW.next();
			String k = (String)entry.getKey();
		/*	
			String v = (String)entry.getValue();
			String rv = v.concat(",").concat(valueString[0]).concat(",0,0");
		*/	
			String rv = valueString[0].concat(",0,0");
			keyWords.put(k,rv);
		}// end of while-loop for set keyWords

		Map<String,String> jobInf = MRTools.getJobInf(valueString,keyWords);
		Set set = (Set)jobInf.entrySet();
		Iterator iterator = set.iterator();

		while(iterator.hasNext()) {
			Entry entry = (Entry)iterator.next();
			String keyWord = (String)entry.getKey();
			String valueInf = (String)entry.getValue();
			context.write(new Text(keyWord),new Text(valueInf));
		}// end of while-loop for output
	}// end of method map()	
}// end of class ClassificationTFMapper