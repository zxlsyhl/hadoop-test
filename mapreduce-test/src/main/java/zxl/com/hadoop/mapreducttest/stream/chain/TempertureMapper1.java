package zxl.com.hadoop.mapreducttest.stream.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TempertureMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year_date = line.substring(0,8);
        int airTemperature = Integer.parseInt(line.substring(8,12));
        System.out.println(String.format("TempertureMapper1:year_date[%s]-airTemperature[%s]",year_date,airTemperature));
        context.write(new Text(year_date), new IntWritable(airTemperature));
    }
}
