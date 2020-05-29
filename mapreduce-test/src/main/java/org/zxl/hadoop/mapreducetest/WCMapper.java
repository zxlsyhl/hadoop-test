package org.zxl.hadoop.mapreducetest;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Description： Mapper<br/>
 * Copyright (c) , 2018, xlj <br/>
 * This program is protected by copyright laws. <br/>
 * Program Name:WCMapper.java <br/>
 *
 * @version : 1.0
 * <p>
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * 它的这个Mapper让你去定义四个泛型,为什么mapper里面需要四个泛型
 * 其实读文本文件的操作不用你来实现，框架已经帮你实现了,框架可以读这个文件
 * 然后每读一行,就会发给你这个map,让你去运行一次,所以它读一行是不是把数据传给你，
 * <p>
 * 那他传给map的时候,这个数据就意味着类型的一个协议,我以什么类型的数据给你,我是不是得事先定好啊
 * map接收的数据类型得和框架给他的数据类型一致,不然的话就会出现类型转换异常
 * 所以map里面得定数据类型，前面两个是map拿数据的类型,拿数据是以什么类型拿的，那么框架就是以这个类型传给你
 * <p>
 * 另外两个泛型是map的输出数据类型,即reduce也得有4个泛型,前面两个是reduce拿数据的泛型得和map输出的泛型类型一致
 * 剩下两个是reduce再输出的结果时的两个数据类型
 */
/*
 * 4个泛型,前两个是指定mapper端输入数据的类型,为什么呢,mapper和reducer都一样
 * 拿数据，输出数据都是以<key,value>的形式进行的--那么key,value都分别有一个数据类型
 * KEYIN：输入的key的类型
 * VALUEIN：输入的value的类型
 * KEYOUT：输出的key的数据类型
 * VALUEOUT：输出的value的数据累心
 * map reduce的数据输入输出都是以key,value对封装的
 * 至于输入的key,value形式我们是不能控制的,是框架传给我们的,
 * 框架传给我们是什么类型,我们这里就写什么数据类型
 *
 * 默认情况下框架传给我们的mapper的输入数据中,key是要处理的文本中一行的起始偏移量,
 * 因为我们的框架是读一行就调用一次我们的偏移量
 * 那么就把一行的起始偏移量作为key,这一行的内容作为value
 *
 * 那么输出端的数据类型是什么，由于我们输出的数<hello,1>
 * 那么它们的数据类型就显而易见了
 * 初步定义为：
 * Mapper<Long, String, String, int>
 * 但是不管是Long还是String,在MapReduce里面运行的时候,这个数据读到网络里面进行传递
 * 即各个节点之间会进行传递,那么要在网络里面传输,那么就意味着这个数据得序列化
 * Long、String对象，内存对象走网络都得序列化,Long、String,int序列化
 * 如果自己实现Serializable接口，那么附加的信息太多了
 * hadoop实现了自己的一套序列化机制
 * 所以就不要用Java里面的数据类型了，而是用它自己的封装一套数据类型
 * 这样就有助于提高效率,实现了自己的序列化接口
 * 在序列化传输的 时候走的就是自己的序列化方法来传递,少了很多负载信息,传递数据精简,
 * Long---LongWritable
 * String也有自己的封装-Text
 * int--IntWritable
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //	MapReduce框架每读一次数据，就会调用一次该方法
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //读取configuration配置
        System.out.println("WCMapper:"+context.getConfiguration().get("ids"));
        //具体业务逻辑就写在这个方法体中,而且我们业务要处理的数据已经被框架传递进来,在方法参数中
        //key--这一行数据的其实偏移量   value--这一行数据的文本内容
        //1.先把单词拿出来,拿到一行
        String line = value.toString();
        //2.切分单词,这个是按照特定的分隔符 进行切分
        String[] words = line.split(" ");
        //3.把里面的单词发送出去
        /*
         * 怎么发出去呢？我都不知道reduce在哪里运行
         * 其实呢，这个不用我们关心
         * 你只要把你的东西给那个工具就可以了
         * 剩下的就给那个框架去做
         * 那个工具在哪-----context
         * 它把那个工具放到那个context里面去了，即输出的工具
         * 所以你只要输出到context里面就行了
         * 剩下的具体往哪里走，是context的事情
         */
        //遍历单词数组,输出为<K,V>形式 key是单词,value是1

        for (String word : words) {
            //记得把key和value继续封装起来,即下面
            context.write(new Text(word), new IntWritable(1));
        }
        /*
         * map方法的执行频率：每读一行就调一次
         * 最后到reduce 的时候，应该是把某个单词里面所有的1都到，才能处理
         * 而且中间有一个缓存的过程,因为每个map的处理速度都不会完全一致
         * 等那个单词所有的1都到齐了才传给reduce
         */
        //每一组key,value都全了，才会去调用一次reduce，reduce直接去处理valuelist
        //接着就是写Reduce逻辑了

    }
}