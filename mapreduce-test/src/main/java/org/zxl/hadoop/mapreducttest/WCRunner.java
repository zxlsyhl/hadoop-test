package org.zxl.hadoop.mapreducttest;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Description： 用来描述一个特定的作业<br/>
 * Copyright (c) , 2018, xlj <br/>
 * This program is protected by copyright laws. <br/>
 * Program Name:WCRunner.java <br/>
 *
 * 该作业使用哪个类作为逻辑处理的map
 * 哪个作为reduce
 * 还可以指定该作业要处理的数据所在的路径
 * 还可以指定该作业输出的结果放到哪个路径
 *
 * @version : 1.0
 */

public class WCRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("ids","A2019111115441221|A2019111115412312");
        //首先要描述一个作业,这些信息是挺多的,哪个是map,哪个是reduce,输入输出路径在哪
        //一般来说这么多信息,就可以把它封装在一个对象里面,那么这个对象呢就是 ----Job对象
        Job job = Job.getInstance(configuration);

        //job用哪个类作为Mapper 指定输入输出数据类型是什么
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WCReducer.class);//对每个mapper结果进行reduce，能帮助减少mapper和reduce之间的数据传输量

        //job用哪个类作为Reducer 指定数据输入输出类型是什么
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定原始数据存放在哪里
        //参数1：里面是对哪个参数进行指定
        //参数2：文件在哪个路径下,这个路径下的所有文件都会去读的
        FileInputFormat.setInputPaths(job, new Path("input/data1/file.txt"));
//        FileInputFormat.setInputPaths(job, "input/data1");

        //指定处理结果的数据存放路径
        FileOutputFormat.setOutputPath(job, new Path("output6"));

        //提交
        int isok =  job.waitForCompletion(true)?0:-1;
        System.exit(isok);
    }
}