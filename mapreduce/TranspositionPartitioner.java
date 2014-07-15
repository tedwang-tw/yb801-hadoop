import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class TranspositionPartitioner extends Partitioner<IntWritable,Text>{
	@Override
	public int getPartition(IntWritable key, Text value, int reducerNum) {
		return key.hashCode()/reducerNum;
	}// end of method getPartition()
}// end of class TranspositionPartitioner