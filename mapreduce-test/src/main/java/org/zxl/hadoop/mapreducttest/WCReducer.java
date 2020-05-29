package org.zxl.hadoop.mapreducttest;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Description： Reducer<br/>
 * Copyright (c) , 2018, xlj <br/>
 * This program is protected by copyright laws. <br/>
 * Program Name:WCReducer.java <br/>
 *
 * @version : 1.0
 */
/*
 * 类型记得要对应
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, Text> {
    //map处理之后,value传过来的是一个value的集合
    //框架在map处理完成之后,将所有的KV对保存起来,进行分组,然后传递一个组,调用一次reduce
    //相同的key在一个组
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //读取configuration配置
        System.out.println("reduce:"+context.getConfiguration().get("ids"));
        //遍历valuelist，进行了累加
        int count = 0;
        for (IntWritable value : values) {
            //get()方法就能拿到里面的值
            count += value.get();
        }
        //输出一组(一个单词)的统计结果
        //默认输出到HDFS的一个文件上面去,放在HDFS的某个目录下
        context.write(key, new Text(count+""));
        //但是还差一个描述类：用来描述整个逻辑

        /*
         * Map，Reducce都是个分散的,那集群运行的时候不知道运行哪些MapReduce
         *
         * 处理业务逻辑的一个整体，叫做job
         * 我们就可以把那个job告诉那个集群,我们此次运行的是哪个job,
         * job里面用的哪个作为Mapper，哪个业务作为Reducer，我们得指定
         *
         * 所以还得写一个类用来描述处理业务逻辑
         * 把一个特定的业务处理逻辑叫做一个job(作业),我们就可以把这个job告诉那个集群,
         *
         */
    }
}