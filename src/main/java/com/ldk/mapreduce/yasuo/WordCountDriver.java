package com.ldk.mapreduce.yasuo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);

        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);


        //2 设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\款款\\Desktop\\All Documents\\input\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\款款\\Desktop\\All Documents\\output\\output6"));


        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }

}
