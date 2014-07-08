import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

public class MRTools {
	
	public static String[] getStringFromText(Text text) {
		return text.toString().split(",");
	}// end of method getStringFromText()

	public static Map<String,String> getJobInf(String[] jobWords,Map<String,String> keyWords) {
		int total = 0;						// total word count in this job

		for(int i=1;i<jobWords.length;i++) {
			if(keyWords.containsKey(jobWords[i])) {
				String[] kwv = keyWords.get(jobWords[i]).split(",");
				int count = Integer.parseInt(kwv[2]) + 1;
				String countString = Integer.toString(count);
				String valueInf = kwv[0].concat(",").
						  		  concat(kwv[1]).concat(",").
						  		  concat(countString).concat(",0,true");
				keyWords.put(jobWords[i],valueInf);
		}}// end of for-loop for calculate word count in this job

		Set kwvSet = (Set)keyWords.entrySet();
		Iterator iterator = kwvSet.iterator();
		Iterator iteratora = kwvSet.iterator();
		
		while(iterator.hasNext()) {
			Entry mapEntry = (Entry)iterator.next();
			String kcount = ((String)mapEntry.getValue()).split(",")[2];
			total = total + Integer.parseInt(kcount);
		}// end of while-loop for calculate total word count in this job

		while(iteratora.hasNext()) {
			Entry mapEntry = (Entry)iteratora.next();
			String[] countInf = ((String)mapEntry.getValue()).split(",");
			BigDecimal countbd = new BigDecimal(countInf[2]);
			BigDecimal totalbd = new BigDecimal(Integer.toString(total));
			countInf[3] = "" + countbd.divide(totalbd,4,BigDecimal.ROUND_HALF_UP); 
			String valueInf = countInf[0].concat(",").
					  		  concat(countInf[1]).concat(",").
					  		  concat(countInf[2]).concat(",").
							  concat(countInf[3]).concat(",").
							  concat(countInf[4]);
			keyWords.put(countInf[0],valueInf);
		}// end of while-loop for calculate tf for words in this job

		return keyWords;
	}// end of method getJobInf()

	public static List<String> getWordInf(List<String> values) {
		
		List<String> wordInf = new ArrayList<String>();
		int totalJobs = values.size();
		int hasWordJobs = 0;

		for(String value : values) {
			boolean hasWord = Boolean.parseBoolean(value.split(",")[4]);
			if(hasWord)
				hasWordJobs += 1;
		}// end of for-loop for count hasWordJobs
/*		
		double blog = (double)totalJobs/(1+hasWordJobs); 
		double idf = Math.log(blog);

		for(String value : values) {
			String[] idfstring = value.split(",");
			double tf = Double.parseDouble(idfstring[3]);
			double tfidf = tf * idf;
			String tfidfbd = "" + tfidf;
			String valueInf = idfstring[0].concat(":").
							  concat(idfstring[1]).concat(",").
							  concat(idfstring[2]).concat(",").
							  concat(idfstring[3]).concat(",").
							  concat(tfidfbd);
			wordInf.add(valueInf);
		}// end of for-loop for finding tfidf
*/		
		BigDecimal totalbd = new BigDecimal(String.valueOf(totalJobs));
		BigDecimal hwjbd = new BigDecimal(String.valueOf(hasWordJobs));
		double blog = totalbd.divide(hwjbd,4,BigDecimal.ROUND_HALF_UP).doubleValue(); 
		double idf = Math.log(blog);
		BigDecimal idfbd = new BigDecimal(String.valueOf(idf));

		for(String value : values) {
			String[] idfstring = value.split(",");
			BigDecimal tfbd = new BigDecimal(idfstring[3]);
			BigDecimal tfidf = tfbd.multiply(idfbd).setScale(4,BigDecimal.ROUND_HALF_UP);
			String tfidfbd = "" + tfidf;
			String valueInf = idfstring[0].concat(":").
							  concat(idfstring[1]).concat(",").
							  concat(idfstring[2]).concat(",").
							  concat(idfstring[3]).concat(",").
							  concat(tfidfbd);
			wordInf.add(valueInf);
		}// end of for-loop for finding tfidf

		return wordInf;
	}// end of method getWordInf()
}// end of class MRTools
