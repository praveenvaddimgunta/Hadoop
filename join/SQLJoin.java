import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SQLJoin {

	public static class JoinMapper extends Mapper <LongWritable, Text, Text, JoinWritable>{
		String inputFileName;
		protected void setup(Context context) throws IOException, InterruptedException	{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			inputFileName = fileSplit.getPath().getName();
		}		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException	{
			String [] tokens = value.toString().split(" ");
			if (tokens.length == 2)	{
				context.write(new Text(tokens[0]), new JoinWritable(tokens[1], inputFileName));
			}
		}
	}

	public static class JoinReducer extends Reducer <Text, JoinWritable, NullWritable, Text>{	
		public void reduce(Text key, Iterable<JoinWritable> values, Context context) throws IOException, InterruptedException{
			String dept = null;
			String name = null;
			StringBuffer rec = null;
			String id = key.toString();
			ArrayList<String> lis = new ArrayList<>();
			for (JoinWritable mow : values)	{
				if (mow.getMrFileName().toString().equals("cust.txt")){
					name = mow.getMrValue().toString();
				}else if (mow.getMrFileName().toString().equals("ord.txt"))	{
					dept = mow.getMrValue().toString();
					lis.add(dept);
				}
			}			
			if (lis.size() > 0) {
				for(String st: lis) {
					rec = new StringBuffer(id).append(",");
					rec.append(name).append(",").append(st);
					context.write(NullWritable.get(), new Text (rec.toString()));
				}			
			}
		}
	}

	

	public static void main(String[] args) throws Exception	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3)	{
			System.err.println("Usage: Join <in-1> <in-2> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Sql Join");
		job.setJarByClass(SQLJoin.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class JoinWritable implements Writable{
		private Text mrValue;
		private Text mrFileName;

		public JoinWritable()	{
			set(new Text(), new Text());
		}

		public JoinWritable(String mrValue, String mrFileName)	{
			set(new Text(mrValue), new Text(mrFileName));
		}

		public JoinWritable(Text mrValue, Text mrFileName)	{
			set(mrValue, mrFileName);
		}

		public void set(Text mrValue, Text mrFileName)	{
			this.mrValue = mrValue;
			this.mrFileName = mrFileName;
		}

		public Text getMrValue()	{
			return mrValue;
		}

		public Text getMrFileName()	{
			return mrFileName;
		}

		public void write(DataOutput out) throws IOException{
			mrValue.write(out);
			mrFileName.write(out);
		}

		public void readFields(DataInput in) throws IOException	{
			mrValue.readFields(in);
			mrFileName.readFields(in);
		}

		public int hashCode(){
			return mrValue.hashCode() * 163 + mrFileName.hashCode();
		}

		public String toString(){
			return mrValue + "\t" + mrFileName;
		}
	}
