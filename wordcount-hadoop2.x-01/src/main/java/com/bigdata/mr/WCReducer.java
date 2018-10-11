package com.bigdata.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 自定义Reducer 并重写reduce()方法
 * 
 * @author Administrator
 *
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	private static String TimeMill;
	private MultipleOutputs<Text, Text> mos;
	private static Text outValue = new Text();
	@Override
	protected void reduce(Text text, Iterable<LongWritable> v1, Context context)
			throws IOException, InterruptedException {
		TimeMill = context.getConfiguration().get("TimeMill");
		long count = 0;
		for (LongWritable i : v1) {
			count += i.get();
		}
		// 将结果输出z
		context.write(text, new LongWritable(count));
	}
}
