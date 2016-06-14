package com.data.util.mr;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");
		String year = tokens[2];
		int seaLevel;
		if (!tokens[34].equals("M")) {
			if(NumberUtils.isDigits(tokens[34]))
				seaLevel = Integer.parseInt(tokens[34]);
			else
				seaLevel = 0;
		} else {
			seaLevel = 0;
		}
		context.write(new Text(year), new IntWritable(seaLevel));
	}
}