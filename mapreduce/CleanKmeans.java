import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class CleanKmeans {

	public static void main(String[] args) {
		if(args.length != 2) {
			System.err.println("CleanOutputDataFromKmeans Usage: jar jarname.jar classname <input path> <out path>");
			System.exit(-1);}
		
		File filein = new File(args[0]);
		File fileout = new File(args[1]);
		BufferedReader buf;
		String str;
		List<String> values = new ArrayList<String>();
		List<String> jobNo = new ArrayList<String>();
		List<String> groupNo = new ArrayList<String>();
		PrintWriter out = null;
		StringBuilder sb = new StringBuilder(300);
		String[] inf;
		String value;
		int count = 0;
		
		try{
			buf = new BufferedReader(new FileReader(filein));
			out = new PrintWriter(new FileWriter(fileout));
				
			while((str=buf.readLine())!=null) {
				if(str.indexOf("Key:")!=(-1)) {
					inf = (str.split("=")[0]).split(":");
					value = (inf[4].trim()).concat(":").concat(inf[1].trim());
					values.add(value);
					jobNo.add(inf[4]);
					groupNo.add(inf[1]);
			}}// end of while-loop for reading data
			
			sb.append("MA:KM\n");
			for(String s : values) {
				
				if(count != values.size()-1)
					sb.append(s.trim()).append(",");
				else
					sb.append(s.trim());
				count++;
			}
			
			out.println(sb);	
			out.close();
			buf.close();
		}catch(IOException e) {
			System.out.println(e.toString());
		}// end of try-catch	
	}// end of method main()
}// end of class CleanOutputDataFromKmeansCluster