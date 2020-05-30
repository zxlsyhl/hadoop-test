package org.zxl.hadoop.mapreducetest.dbformat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 默认支持事务，全部成功或全部失败
 */
public class DBOutputFormatDriver extends Configured implements Tool {
    static class TblsWritable implements Writable, DBWritable{
        String tbl_name ; int tbl_age;

        public TblsWritable() {
        }

        public TblsWritable(String tbl_name, int tbl_age) {
            this.tbl_name = tbl_name;
            this.tbl_age = tbl_age;
        }

        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1, this.tbl_name);
            preparedStatement.setInt(2, this.tbl_age);
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.tbl_name = resultSet.getString(1);
            this.tbl_age = resultSet.getInt(2);
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.tbl_name);
            dataOutput.writeInt(this.tbl_age);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.tbl_name = dataInput.readUTF();
            this.tbl_age = dataInput.readInt();
        }

        public String toString(){
            return new String(this.tbl_name+" "+this.tbl_age);
        }
    }

    static class DBOutputFormatMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    static class DBOutputFormatReducer extends Reducer<LongWritable, Text, TblsWritable, TblsWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder value = new StringBuilder();
            for (Text text: values){
                value.append(text);
            }
            String[] outputArr = value.toString().split("\t");
            if(StringUtils.isNotBlank(outputArr[0])){
                String name = outputArr[0].trim();
                int age = 0;
                try {
                    age = Integer.parseInt(outputArr[1].trim());
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                context.write(new TblsWritable(name,age) ,null);
            }
        }
    }

    public int run(String[] args) throws Exception{
        Configuration conf = getConf();

        DBConfiguration.configureDB(conf ,"com.mysql.jdbc.Driver", "jdbc:mysql://192.168.1.102:3306/mp_test?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false",
                "root", "MyNewPass4!");

        Job job = Job.getInstance(conf, DBOutputFormatDriver.class.getSimpleName());
        job.setJarByClass(getClass());
        //输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(DBOutputFormatMapper.class);
        job.setReducerClass(DBOutputFormatReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        //输出到哪些表、字段
        DBOutputFormat.setOutput(job, "tb_out", "name","age");

        return job.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new DBOutputFormatDriver(), args);

        System.exit(exitcode);
    }
}
