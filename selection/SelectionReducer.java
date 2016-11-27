package com.dadv.selection;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SelectionReducer extends Reducer <Text,Text, NullWritable, Text>
{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		for (Text mow : values)
		{
			context.write(NullWritable.get(), new Text (key.toString()+mow.toString()));
		}
		
	}
}

