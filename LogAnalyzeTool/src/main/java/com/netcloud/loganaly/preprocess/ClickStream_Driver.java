package com.netcloud.loganaly.preprocess;

import com.netcloud.loganaly.domain.WebLogBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


    public class ClickStream_Driver extends Configured implements Tool {
    public static void main(String[] args) {
        int ret = 0;
        try {
            ret = ToolRunner.run(new ClickStream_Driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage:  [generic options] %s <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(new Path(args[1]), true);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ClickStream_Driver.class);
        //设置job的mapper和reducer
        job.setMapperClass(ClickStream_Mapper.class);
        job.setReducerClass(ClickStream_Reduce.class);
//        设置mapper过后的细节
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);
//        置Reducer细节
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
//        设置Reducer的数目
        job.setNumReduceTasks(4);
        //设置文件输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
