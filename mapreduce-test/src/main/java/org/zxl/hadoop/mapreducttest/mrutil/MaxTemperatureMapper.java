package org.zxl.hadoop.mapreducttest.mrutil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
