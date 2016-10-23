import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixVectorCompute {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text lineNumber = new Text(); // Matrix row
		private static int i = 0;
		private final static int[] vector = {2, 3, 4}; // Vector value

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int j = 0; // Vector index
			lineNumber.set(i + "");
			while (itr.hasMoreTokens()) {
				int result = vector[j] * Integer.parseInt(itr.nextToken());
				IntWritable one = new IntWritable(result);
				context.write(lineNumber, one);
				j ++;
			}
			i ++;
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "Matrix Vector");

		job.setJarByClass(MatrixVectorCompute.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);		
		
				
		if(!fs.exists(input)) {
			System.err.println("Input file doesn't exists");
			return;
		}
		if(fs.exists(output)) {
			fs.delete(output, true);
			System.err.println("Output file deleted");
		}
		fs.close();

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
				
		job.waitForCompletion(true);				
		System.out.println("Executed Successfully!!!!");
	}
}