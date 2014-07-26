import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class JobTable {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		File kmeansFile = new File(args[0]);
		File joburlFile = new File(args[1]);
		File outputFile = new File(args[2]);
		BufferedReader buf;
		String str;
		String[] stra = null;
		int i=0;
		PrintWriter out;
		StringBuilder sb = new StringBuilder(300);
		
		try{
			buf = new BufferedReader(new FileReader(kmeansFile));
			
			while((str=buf.readLine())!=null) {
				if(!str.equals("MA:KM")) {
					stra = str.split(",");
			}}
			
			buf = new BufferedReader(new FileReader(joburlFile));
			
			while((str=buf.readLine())!=null) {
				sb.append(stra[i].replace(":",",")).append(",").append(str).append("\n");
				i++;
			}
			
			out = new PrintWriter(new FileWriter(outputFile));
			out.println(sb);
			
			buf.close();
			out.close();
		}catch(IOException e) {
			System.out.println("kmeansFile :"+e.toString());
			System.exit(-1);
		}
		
	}// end of method main()
}// end of class JobTable