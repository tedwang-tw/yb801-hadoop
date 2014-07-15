import java.math.BigDecimal;
import java.util.*;

@SuppressWarnings("rawtypes")
public class MRTools {

	public static Map<String,String> getJobInf(String[] jobWords,Map<String,String> keyWords) {
		
		Set<String> setStrings = new HashSet<String>();
		int total = jobWords.length-1;		// total word count in this job
		
		for(int i=1;i<jobWords.length;i++) {
			setStrings.add(jobWords[i]);
			
			if(keyWords.containsKey(jobWords[i])) {
				String[] kwv = keyWords.get(jobWords[i]).split(",");
				double count = Double.parseDouble(kwv[1]) + 1.0;
				String countString = Double.toString(count);
				String wordinf = kwv[0].concat(",").concat(countString).concat(",t");
				keyWords.put(jobWords[i],wordinf);
		}}// end of for-loop for calculate word count in this job
		
		Iterator iterator = setStrings.iterator();

		while(iterator.hasNext()) {
			String k = (String)iterator.next();
			
			if(keyWords.containsKey(k)) {
				String[] countInf = keyWords.get(k).split(",");
				BigDecimal countbd = new BigDecimal(countInf[1]);
				BigDecimal totalbd = new BigDecimal(Integer.toString(total));
				countInf[1] = "" + countbd.divide(totalbd,4,BigDecimal.ROUND_HALF_UP);
				String wordinf = countInf[0].concat(",").concat(countInf[1]).concat(",t");
				keyWords.put(k,wordinf);
		}}// end of while-loop for calculate tf for words in this job	

		return keyWords;
	}// end of method getJobInf()

	public static List<String> getWordInf(List<String> values) {
		
		List<String> wordInf = new ArrayList<String>();
		int hasWordJobs = 1;

		for(String value : values) {
			String hasWords = value.split(",")[2];
			if(hasWords.equals("t"))
				hasWordJobs += 1;
		}// end of for-loop for count hasWordJobs
	
		BigDecimal totalbd = new BigDecimal(String.valueOf(values.size()));					// total jobs
		BigDecimal hwjbd = new BigDecimal(String.valueOf(hasWordJobs));						// has word jobs
		double blog = totalbd.divide(hwjbd,6,BigDecimal.ROUND_HALF_UP).doubleValue(); 
		BigDecimal idf = new BigDecimal(String.valueOf(Math.log(blog)));

		for(String value : values) {
			String[] idfstring = value.split(",");
			String firstString = ":";
			BigDecimal tfbd = new BigDecimal(idfstring[1]);
			BigDecimal tfidf = tfbd.multiply(idf).setScale(6,BigDecimal.ROUND_HALF_UP);
			idfstring[2] = "" + tfidf;
			String valueInf = firstString.concat(idfstring[0]).concat(",").concat(idfstring[1]).concat(",").concat(idfstring[2]);
			wordInf.add(valueInf);
		}// end of for-loop for finding tfidf

		return wordInf;
	}// end of method getWordInf()
}// end of class MRTools