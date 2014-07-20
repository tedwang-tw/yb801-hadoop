import java.math.BigDecimal;
import java.util.*;

public class MRTools {

	public static Map<String,String> getJobInf(String[] jobWords,Map<String,String> keyWords) {
		
		int total = jobWords.length-1;		// total word count in this job
		String[] countInf;
		StringBuilder sb = new StringBuilder();
		
		for(int i=1;i<jobWords.length;i++) {
			if(keyWords.containsKey(jobWords[i])) {
				countInf = keyWords.get(jobWords[i]).split(",");
				int count = Integer.parseInt(countInf[2]) + 1;
				sb.append( countInf[0]).append(",").append(countInf[1]).append(",").append(count);
				keyWords.put(jobWords[i],sb.toString());
				sb.delete(0,sb.length());
		}}// end of for-loop for calculate word count in this job
		
		for(int i=1;i<jobWords.length;i++) {			
			if(keyWords.containsKey(jobWords[i])) {
				countInf = keyWords.get(jobWords[i]).split(",");
				countInf[2] = "" + (new BigDecimal(countInf[2])).divide(new BigDecimal(Integer.toString(total)),4,BigDecimal.ROUND_HALF_UP);
				sb.append( countInf[0]).append(",").append(countInf[1]).append(",").append(countInf[2]);
				keyWords.put(countInf[0],sb.toString());
				sb.delete(0,sb.length());
		}}// end of for-loop for calculate word count in this job
		
		countInf = null;
		sb = null;
		
		return keyWords;
	}// end of method getJobInf()

	public static List<String> getWordInf(List<String> values) {

		StringBuilder sb = new StringBuilder();
		List<String> wordInf = new ArrayList<String>();
		int hasWordJobs = 1;

		for(String value : values) {
			BigDecimal tfNum = new BigDecimal(value.split(",")[2]);
			if(tfNum.signum() != 0)
				hasWordJobs += 1;
		}// end of for-loop for count hasWordJobs
	
		double blog = (new BigDecimal(String.valueOf(values.size()))).divide(new BigDecimal(String.valueOf(hasWordJobs)),6,BigDecimal.ROUND_HALF_UP).doubleValue(); 
		BigDecimal idf = new BigDecimal(String.valueOf(Math.log(blog)));

		for(String value : values) {
			sb.append(":").append(",").append(value).append(",").append((new BigDecimal(value.split(",")[2])).multiply(idf).setScale(4,BigDecimal.ROUND_HALF_UP));
			wordInf.add(sb.toString());
			sb.delete(0,sb.length());		
		}// end of for-loop for finding tfidf
		
		sb = null;
		idf = null;
		
		return wordInf;
	}// end of method getWordInf()
}// end of class MRTools