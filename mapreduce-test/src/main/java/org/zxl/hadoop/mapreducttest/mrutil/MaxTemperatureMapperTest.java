package org.zxl.hadoop.mapreducttest.mrutil;

import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * org.apache.hadoop.mrunit.MapDriver 不支持MapReduce的新版本类 org.apache.hadoop.mapreduce.Mapper 只支持老版本 org.apache.hadoop.mapred.Mapper
 */
public class MaxTemperatureMapperTest {
    public void processesValidRecord(){
        Text value = new Text("19500012");
//        new MapDriver<LongWritable, Text,Text, IntWritable>()
//                .withMapper(new MaxTemperatureMapper())
//                .withInput(new LongWritable(0), value)
//                .withOutput(new Text("1950"), new IntWritable(10))
//                .runTest();
    }
    @Test
    public void testTemperature(){
//        Temperature.OVER_100.increment(1);
//        System.out.println(Temperature.OVER_100.getCount());
//        Temperature.OVER_100.increment(1);
//        System.out.println(Temperature.OVER_100.getCount());

    }
}
