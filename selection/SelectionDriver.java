import java.io.IOException;
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


@SuppressWarnings("unused")
public class SelectionDriver
{
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Selection Operation");
		job.setJarByClass(SelectionDriver.class);
		job.setMapperClass(SelectionMapper.class);
		job.setReducerClass(SelectionReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("job done");
	}
}

class SelectionMapper extends Mapper <LongWritable, Text, Text,Text>{
	public static final String name = "a";
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{		
		String [] tokens = value.toString().split(" ");
		if (tokens.length > 1)	{
			if (tokens[1].equals(name)) {
				StringBuffer sb = new StringBuffer();
				for (int i = 1; i < tokens.length; i++) {
					if (i == tokens.length)
						sb.append(tokens[i]);
					else
						sb.append("   ").append(tokens[i]).append("   ");
				}
				context.write(new Text(tokens[0]),new Text(sb.toString()));

			}
			
		} 
	}
}
class SelectionReducer extends Reducer <Text,Text, NullWritable, Text>{	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for (Text mow : values)	{
			context.write(NullWritable.get(), new Text (key.toString()+mow.toString()));
		}
		
	}
}
