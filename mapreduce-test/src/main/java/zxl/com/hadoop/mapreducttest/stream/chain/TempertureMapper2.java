package zxl.com.hadoop.mapreducttest.stream.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TempertureMapper2 extends Mapper<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String date = key.toString().substring(4,8);
        int airTemperature = value.get();
        System.out.println(String.format("TempertureMapper2:date[%s]-airTemperature[%s]",date,airTemperature));
        context.write(new Text(date), new IntWritable(airTemperature));
    }
}
