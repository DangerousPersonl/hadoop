package com.ldk.mapreduce.outputformat;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private  FSDataOutputStream otherOut;
    private  FSDataOutputStream ldkOut;

    public LogRecordWriter(TaskAttemptContext job) {
        //创建俩条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            ldkOut = fs.create(new Path("C:\\Users\\款款\\Desktop\\All Documents\\output\\hadoop\\ldk.log"));

            otherOut = fs.create(new Path("C:\\Users\\款款\\Desktop\\All Documents\\output\\hadoop\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        //具体写

        if(log.contains("ldk")){
            ldkOut.writeBytes(log+"\n");
        }else{
            otherOut.writeBytes(log+"\n");
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关流
        IOUtils.closeStream(ldkOut);
        IOUtils.closeStream(otherOut);
    }
}
