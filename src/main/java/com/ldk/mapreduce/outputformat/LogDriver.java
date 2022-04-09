package com.ldk.mapreduce.outputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置jar
        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //设置map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义的outputformat
        job.setOutputFormatClass(LogOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\款款\\Desktop\\All Documents\\input\\inputoutputformat"));
        //虽然我们自定义了outputformat，但是因为我们的outputformat继承了fileoutputformatt
        //而fileoutputformat要输出一个_SUCCESS文件。所以在这还得指定一个输出目录。
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\款款\\Desktop\\All Documents\\output\\output4444"));

        //提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
