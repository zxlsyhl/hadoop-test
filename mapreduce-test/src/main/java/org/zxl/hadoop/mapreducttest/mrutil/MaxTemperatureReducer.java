package org.zxl.hadoop.mapreducttest.mrutil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for(IntWritable value: values){
            maxValue = Math.max(maxValue, value.get());
            System.out.println(String.format("MaxTemperatureReducer:key[%s]-value[%s]",key,value));
        }
        context.write(key, new IntWritable(maxValue));
        System.out.println(String.format("MaxTemperatureReducer:key[%s]done!!!",key));
    }
}
