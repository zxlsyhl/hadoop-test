package org.zxl.hadoop.mapreducetest.configuration;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class Test1 {
    /**
     * 读取配置文件
     */
    @Test
    public void test1(){
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        System.out.println(conf.get("color"));
    }

    /**
     * 添加多个配置文件时，后添加的会覆盖之前定义的属性。
     */
    @Test
    public void test2(){
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        conf.addResource("configuration-2.xml");
        System.out.println(conf.get("color"));
    }


}
