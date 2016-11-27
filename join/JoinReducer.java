package com.dadv.sql.join;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer <Text, JoinWritable, NullWritable, Text>
{
	
	public void reduce(Text key, Iterable<JoinWritable> values, Context context) throws IOException, InterruptedException
	{
		String dept = null;
		String name = null;
		StringBuffer rec = null;
		String id = key.toString();
		ArrayList<String> lis = new ArrayList<>();
		for (JoinWritable mow : values)
		{
			if (mow.getMrFileName().toString().equals("cust.txt"))
			{
				name = mow.getMrValue().toString();
			}
			else if (mow.getMrFileName().toString().equals("ord.txt"))
			{
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
