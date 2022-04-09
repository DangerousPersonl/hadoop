package com.ldk.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        //1遍历集合累加值
        long totalup = 0;
        long totaldowm = 0;
        for (FlowBean value : values) {
            totalup += value.getUpFlow();
            totaldowm += value.getDownFlow();
        }
        //2封装outK,outV
        outV.setUpFlow(totalup);
        outV.setDownFlow(totaldowm);
        outV.setSumFlow();

        //3写出
        context.write(key,outV);
    }
}
