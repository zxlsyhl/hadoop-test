package zxl.com.hadoop.mapreducttest.stream.jobControl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JobControlMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String date = line.substring(0,6);
        int airTemperature = Integer.parseInt(line.substring(6,10));
        System.out.println(String.format("JobControlMapper1:date[%s]-airTemperature[%s]",date,airTemperature));
//        if(airTemperature>100){
//            System.err.println("Temperature over 100 degrees fro input:"+ value);
//            context.setStatus("Detected possibly corrupt record: see logs.");
//            context.getCounter("group1","counter1").increment(1);
//        }
//        List<Temperature> temperatures = new ArrayList<Temperature>();
//        for(int i=0;i<1000000;i++){
//            Temperature temperature = new Temperature(i,"city"+i,i+10);
//            temperatures.add(temperature);
//        }

        context.write(new Text(date), new IntWritable(airTemperature));
    }
}
