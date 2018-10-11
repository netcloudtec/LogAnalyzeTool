package com.bigdata.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * hadoop-2.x 将此程序运行在Linux weekend08 虚拟机上 Alt+shift+s 重写父类的方法
 * 这是old version的hadoop版本
 * @author Administrator
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	  private static Text outKey = new Text();
	  private static String TimeMill;
	  private static Text outValue = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 接受数据
		String line = value.toString();
		// 切分数据
		String[] words = line.split("\t");
		// 循环数据
		for (String s : words) {
			// 循环出一次记为1
			context.write(new Text(s), new LongWritable(1));
		}

	}

}
