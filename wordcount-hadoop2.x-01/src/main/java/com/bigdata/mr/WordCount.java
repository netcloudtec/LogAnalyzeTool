package com.bigdata.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 提交作业 打成jar包在linux下执行 打成jar包的方式 1）JAR file ：在linux下适用hadoop -jar 的方式执行
 * 2）Runnable JAR file:在linux下使用java -jar的方式执行 在这里我们采用JAR file的方式执行 注意：在打成jar的时候
 * 要指定 main方法 如果在打jar包的时候不去指定，只能在运行的时候去指定运行jar包的main方法 这里我们直接在打jar包的时候去指定main方法
 * 将打成的jar包放在linux的/usr/local/project目录下面
 * com.bigdata.mr.WordCount
 * @author Administrator
 *
 */
public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String TimeMill = Long.toString(System.currentTimeMillis());
		// 创建一个Job对象
		Job job = new Job();
		// 设置main方法的类
		job.setJarByClass(WordCount.class);
		// 设置Mapper的相关的属性
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// 设置Reduce的相关的属性
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);// reduce没有setReducerOutputKeyClass
		job.setOutputValueClass(LongWritable.class);
		job.getConfiguration().set("randNumber", TimeMill);
		FileInputFormat.setInputPaths(job,  new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);// 提交job任务 设置为true 显示打印详情
	}
}
