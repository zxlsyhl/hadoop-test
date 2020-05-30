package org.zxl.hadoop.mapreducetest.dbformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBInputFormatDriver extends Configured implements Tool {

    static class MyDBWritable implements Writable, DBWritable {
        int id;
        String name;
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setInt(1, id);
            preparedStatement.setString(2, name);
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.id = resultSet.getInt(1);
            this.name = resultSet.getString(2);
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.write(id);
            dataOutput.writeUTF(name);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.id = dataInput.readInt();
            this.name = dataInput.readUTF();
        }

        public String toString(){
            return "MyDBWritable[id"+id+",\t+"+"name="+name+"]";
        }
    }

    static class MyDbMapper extends Mapper<LongWritable, MyDBWritable, LongWritable, Text>{
        final Text v2 = new Text();
        @Override
        protected void map(LongWritable key, MyDBWritable value, Context context) throws IOException, InterruptedException {
            v2.set(value.toString());
            context.write(key , v2);
        }
    }



    public int run(String[] args) throws Exception{
        if(args.length != 1){
            System.err.println(String.format("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName()));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration conf = getConf();

        DBConfiguration.configureDB(conf ,"com.mysql.jdbc.Driver", "jdbc:mysql://192.168.1.102:3306/mp_test?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false",
                "root", "MyNewPass4!");


//        //设置map任务输出gzip压缩格式
//        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
//        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
//
//        //设置reduce输出分隔符
//        conf.set("mapred.textoutputformat.ignoreseparator","true");
//        conf.set("mapred.textoutputformat.separator",",");

        Job job = Job.getInstance(conf, DBInputFormatDriver.class.getSimpleName());
        job.setJarByClass(getClass());

//        FileInputFormat.addInputPath(job, new Path(args[0]));
        DBInputFormat.setInput(job, MyDBWritable.class, "select id,name from tb_one",
                "select count(1) from tb_one");
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setInputFormatClass(DBInputFormat.class);

        job.setMapperClass(MyDbMapper.class);
//        job.setCombinerClass(MaxTemperatureReducer.class);
//        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //设置reduce任务数，和分区策略类。任务数和分区数相对应
//        job.setNumReduceTasks(2);
//        job.setPartitionerClass(HashPartitioner.class);


        //对reduce输出进行压缩
//        FileOutputFormat.setCompressOutput(job, true);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        return job.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new DBInputFormatDriver(), args);
        System.exit(exitcode);
    }
}
