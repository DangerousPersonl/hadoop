package com.ldk.mapreduce.yasuo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, mapper阶段输入的key的类型：Text
 * VALUEIN,mapper阶段输入value类型：IntWritable
 * KEYOUT, mapper阶段输出的key类型：Text
 * VALUEOUT，mapper阶段输出的value类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //2切割
        String[] words = line.split(" ");
        //3循环写出
        for (String word : words) {
            //封装outk
            outK.set(word);
            //写出
            context.write(outK,outV);
        }
    }
}
