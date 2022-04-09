package com.ldk.mapreduce.partitioner2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-21 17:05
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
       //1获取一行
        String line = value.toString();

        //2切割
        String[] split = line.split("\t");

        //3抓取想要的数据
        // 13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        // 13846544121	192.196.100.2			264	0	200
        //手机号：13736230513
        //上行流流量和下行流量
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        //4封装（顺序很重要）
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        //5写出
        context.write(outK,outV);
    }
}
