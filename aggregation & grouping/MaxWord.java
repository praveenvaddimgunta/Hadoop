import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxWord {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  public static class Maxofoneword
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    static int maxValue = 0;
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      
      if(sum > maxValue) {
        maxValue = sum;
        result.set(sum);
        context.write(key, result);
      }
    }
  }

  // public static class MaxFinder extends Reducer<Text,IntWritable,Text,IntWritable> {
  //   private IntWritable result = new IntWritable();
  //   public void reduce(Text key, Iterable<IntWritable> values,
  //                      Context context
  //                      ) throws IOException, InterruptedException {
  //     int maxValue = 0;
  //     for (IntWritable val : values) {
  //       System.out.println(key);
  //       if(val.get() > maxValue) {
  //         System.out.println(val.get());
  //         maxValue = val.get();
  //       }
  //     }
  //     result.set(maxValue);
  //     context.write(key, result);
  //   }
  // }

   

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Max Word");
    job.setJarByClass(MaxWord.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(Maxofoneword.class);
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
