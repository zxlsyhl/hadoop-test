package zxl.com.hadoop.mapreducttest.stream.jobControl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JobControlReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for(IntWritable value: values){
            maxValue = Math.max(maxValue, value.get());
            System.out.println(String.format("JobControlReducer1:key[%s]-value[%s]",key,value));
        }
        context.write(key, new IntWritable(maxValue));
    }
}
