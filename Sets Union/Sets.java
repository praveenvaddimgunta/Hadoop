import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Sets {

public static class FileMapper extends Mapper<LongWritable, Text, Text, Text> {
  static Text fileName;
 
  @Override
  protected void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
       context.write(value, fileName);
  }
 
  @Override
  protected void setup(Context context) throws IOException,
          InterruptedException {
       String name = ((FileSplit) context.getInputSplit()).getPath().getName();
       fileName = new Text(name);
       context.write(new Text("a"), fileName);
  }
}



public static class FileReducer extends Reducer<Text, Text, Text, Text> {
 
    private final HashSet<String> fileNameSet = new HashSet<String>();
     
    enum Counter {
        LINES_IN_COMMON
    }
 
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // add for our first key every file to our set
        // make sure that this action is the first of the entire reduce step

        if(key.toString().equals("a")){
            for (Text t : values) {
                fileNameSet.add(t.toString());
            }
        } else {
            // now add evey incoming value to a temp set
            HashSet<String> set = new HashSet<String>();
            for (Text t : values) {
                set.add(t.toString());
            }
             
            // perform checks
            if(set.size() == fileNameSet.size()){
                // we know that this must be an intersection of all files
                context.getCounter(Counter.LINES_IN_COMMON).increment(1);
            } else {
                // do anything what you want with the difference
            }
  
        }
    }
}
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "Sets");
    job.setJarByClass(Sets.class);
    job.setMapperClass(FileMapper.class);
    job.setCombinerClass(FileReducer.class);
    job.setReducerClass(FileReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

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
