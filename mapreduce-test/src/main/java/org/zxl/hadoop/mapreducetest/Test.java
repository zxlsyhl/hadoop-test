package org.zxl.hadoop.mapreducetest;

import java.util.Properties;

public class Test {
    public static void main(String[] args) {
        Properties properties = System.getProperties();
        for(String key:properties.stringPropertyNames()){

            System.out.println(key+":"+System.getProperty(key));
        }

    }
}
