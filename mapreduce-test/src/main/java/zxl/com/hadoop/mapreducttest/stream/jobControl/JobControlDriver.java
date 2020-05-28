package zxl.com.hadoop.mapreducttest.stream.jobControl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import zxl.com.hadoop.mapreducttest.mrutil.MaxTemperatureMapper;
import zxl.com.hadoop.mapreducttest.mrutil.MaxTemperatureReducer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * window下main方法参数  input/{data4/*} output10 output11
 * linux下main方法参数  input/data4 output10 output11
 */
public class JobControlDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if(args.length != 3){
            System.err.println(String.format("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName()));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        //设置map任务输出gzip压缩格式
//        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
//        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);

        Job job1 = Job.getInstance(conf, getClass().getSimpleName()+"1");
        Job job2 = Job.getInstance(conf, getClass().getSimpleName()+"2");
        Job job3 = Job.getInstance(conf, getClass().getSimpleName()+"3");


        //job1作业参数配置
//        Path inPath = new Path(args[0]);
//        FileStatus[] status = fs.listStatus(inPath);
//        List<Path> list = new ArrayList<Path>();
//        for (FileStatus fileStatus : status) {
//            if (!fs.getFileStatus(fileStatus.getPath()).isDir()) {
//                list.add(fileStatus.getPath());
//            }
//        }
//        Path[] paths = new Path[list.size()];
//        list.toArray(paths);

//        TextInputFormat.setInputPaths(job, paths);
        job1.setJarByClass(getClass());
        FileInputFormat.addInputPath(job1, new Path(args[0])); //以目录作为输入
//        FileInputFormat.setInputPaths(job1,paths); //以文件列表作为输入
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setMapperClass(JobControlMapper1.class);
        job1.setCombinerClass(JobControlReducer1.class);
        job1.setReducerClass(JobControlReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);


        //job2作业参数配置
        job2.setJarByClass(getClass());
        FileInputFormat.addInputPath(job2, new Path(args[1]+"/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setMapperClass(JobControlMapper2.class);
        job2.setCombinerClass(JobControlReducer2.class);
        job2.setReducerClass(JobControlReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        //对reduce输出进行压缩
//        FileOutputFormat.setCompressOutput(job1, true);
//        FileOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

        //创建受控作业
        ControlledJob cjob1 = new ControlledJob(conf);
        ControlledJob cjob2 = new ControlledJob(conf);

        //将普通作业包装成受控作业
        cjob1.setJob(job1);
        cjob2.setJob(job2);

        //设置依赖关系
        cjob2.addDependingJob(cjob1);

        //新建作业控制器
        JobControl jc = new JobControl("My control job");

        //将受控作业添加到控制器中
        jc.addJob(cjob1);
        jc.addJob(cjob2);

        Thread jcThread = new Thread(jc);
        jcThread.start();
        while (true){
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                return 0;
            }
            if(jc.getFailedJobList().size()>0){
                System.out.println(jc.getFailedJobList());
                jc.stop();
                return 1;
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JobControlDriver(), args);
        System.exit(exitCode);
    }
}
