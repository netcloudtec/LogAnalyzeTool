package com.bigdata.mr;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class Test  
{
    public static String path1 = "file:///C:\\word.txt"; 
    public static String path2 = "file:///C:\\dirout\\";
    public static void main(String[] args) throws Exception
     {

         Configuration conf = new Configuration();

         FileSystem fileSystem = FileSystem.get(conf);//获取本地文件系统中的一个fileSystem实例

         if(fileSystem.exists(new Path(path2)))
         {
             fileSystem.delete(new Path(path2), true);
         }

         Job job = new Job();

         //job.setJarByClass(WordCount.class);

         FileInputFormat.setInputPaths(job, new Path(path1));
         job.setInputFormatClass(TextInputFormat.class);
         job.setMapperClass(MyMapper.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(LongWritable.class);

         job.setNumReduceTasks(1);
         job.setPartitionerClass(HashPartitioner.class);


         job.setReducerClass(MyReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(LongWritable.class);
         job.setOutputFormatClass(TextOutputFormat.class);
         FileOutputFormat.setOutputPath(job, new Path(path2));
         job.waitForCompletion(true);
         //查看运行结果：
         FSDataInputStream fr = fileSystem.open(new Path("file:///C:\\dirout\\part-r-00000"));
         IOUtils.copyBytes(fr, System.out, 1024, true);
     }    
     public  static  class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
     {

            protected void map(LongWritable k1, Text v1,Context context)throws IOException, InterruptedException
            {
                 //在这里我们利用context获取日志中的相关数据:键值对信息
                 LongWritable key = context.getCurrentKey();
                 System.out.println(v1.toString()+"对应的起始偏移量是："+key.get());
                 Text value = context.getCurrentValue();
                 System.out.println("当前文本行是："+value.toString());
                 //获取当前行文本所对应的文件的信息
                 FileSplit inputSplit = (FileSplit) context.getInputSplit();
                 Path path = inputSplit.getPath();
                 System.out.println(v1.toString()+"对应的文本路径是："+path);
                 String filename = path.getName();
                 System.out.println(v1.toString()+"对应的文件名是："+filename);
                 System.out.println("----------------------------------------------------------");



                 //利用context获取计数器,对敏感词汇进行计数
                 Counter counter = context.getCounter("Sensitive Word", "sensitiveword");
                 if(v1.toString().contains("fenlie"))
                 {
                     counter.increment(1L);  //如果日志当中包含dalai这个敏感词,自定义计数器加1       
                 }
                 String[] splited = v1.toString().split("\t");


                 for (String string : splited)
                 {
                       context.write(new Text(string),new LongWritable(1L));
                 }
            }   
            protected void cleanup(Context context)throws IOException, InterruptedException
            {
                  String jobName = context.getJobName();
                  System.out.println("当前运行的jobname是："+jobName);
                  JobID jobID = context.getJobID();
                  System.out.println("当前运行的jobId是："+jobID);
                  Configuration conf = context.getConfiguration();
                  System.out.println("运行中读取的配置文件是："+conf);
//                  String user = context.getUser();
//                  System.out.println("当前操作用户是："+user);

            }
     }
     public  static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
     {
        protected void reduce(Text k2, Iterable<LongWritable> v2s,Context context)throws IOException, InterruptedException
        {
                 long sum = 0L;
                 for (LongWritable v2 : v2s)
                {
                    sum += v2.get();
                }
                context.write(k2,new LongWritable(sum));
        }
     }
}