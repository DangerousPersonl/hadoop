package com.ldk.mapreduce.WritableComparable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2设置jar
        job.setJarByClass(FlowDriver.class);

        //3关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4设置mapper输出key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6设置数据输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\款款\\Desktop\\All Documents\\output\\output4"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\款款\\Desktop\\All Documents\\output\\output7"));
        //7提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
