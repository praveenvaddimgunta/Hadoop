package com.dadv.selection;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


@SuppressWarnings("unused")
public class SelectionMapper extends Mapper <LongWritable, Text, Text,Text>
{
	public static final String name = "a";
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String [] tokens = value.toString().split(" ");
		if (tokens.length > 1)
		{
			if (tokens[1].equals(name)) {
				StringBuffer sb = new StringBuffer();
				for (int i = 1; i < tokens.length; i++) {
					if (i == tokens.length)
						sb.append(tokens[i]);
					else
						sb.append("   ").append(tokens[i]).append("   ");
				}//End of For Loop
				context.write(new Text(tokens[0]),new Text(sb.toString()));
			}//End of inner IF Block
			
		} //End Outer IF Block
	}  //End of map Method
} //End of SelectionMapper Class
