package org.zxl.hadoop.mapreducttest.stream.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureChainDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println(String.format("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName()));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration conf = getConf();

        Job job = new Job(conf, "Chain Max temperature");
        job.setJarByClass(getClass());


        //第一个map加入作业流
        JobConf map1Conf = new JobConf(false);
        ChainMapper.addMapper(job, TempertureMapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class,map1Conf);

        //第二个map加入作业流
        JobConf map2Conf = new JobConf(false);
        ChainMapper.addMapper(job, TempertureMapper2.class,Text.class, IntWritable.class, Text.class, IntWritable.class ,map2Conf);

        //加入reduce
        JobConf redConf = new JobConf(false);
        ChainReducer.setReducer(job, TemperatureReducer.class,Text.class, IntWritable.class, Text.class, IntWritable.class, redConf);

        job.setNumReduceTasks(1); //设置reduce输出文件数


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.setMapperClass(MaxTemperatureMapper.class);
//        job.setCombinerClass(MaxTemperatureReducer.class);
//        job.setReducerClass(MaxTemperatureReducer.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

        //对reduce输出进行压缩
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        return job.waitForCompletion(true)? 0:1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureChainDriver(), args);
        System.exit(exitCode);
    }
}
