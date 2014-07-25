import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



public class FileSystemCat {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
		InputStream in = null;
		
		try{
			in = fs.open(new Path(args[0]));
			IOUtils.copyBytes(in,System.out,4096,false);
		}finally {
			IOUtils.closeStream(in);
		}
	}// end of method main()
}// end of class FileSystemCat