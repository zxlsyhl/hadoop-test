package org.zxl.hadoop.mapreducetest.mrutil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //获取FileSpilt对象，包含输入文件的信息
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            System.out.println(String.format("************fileSplit.getPath()=[%s],fileSplit.getStart()=[%s],fileSplit.getLength()=[%s]",fileSplit.getPath(),fileSplit.getStart(),fileSplit.getLength()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }

        String line = value.toString();
        String year = line.substring(0,4);
        int airTemperature = 0;
        try {
            airTemperature = Integer.parseInt(line.substring(4,8));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        if(airTemperature>100){
            System.err.println("Temperature over 100 degrees fro input:"+ value);
            context.setStatus("Detected possibly corrupt record: see logs.");
            context.getCounter("group1","counter1").increment(1);
        }
//        List<Temperature> temperatures = new ArrayList<Temperature>();
//        for(int i=0;i<1000000;i++){
//            Temperature temperature = new Temperature(i,"city"+i,i+10);
//            temperatures.add(temperature);
//        }

        context.write(new Text(year), new IntWritable(airTemperature));
    }
}
