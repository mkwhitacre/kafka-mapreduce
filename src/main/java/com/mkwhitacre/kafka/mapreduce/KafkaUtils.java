package com.mkwhitacre.kafka.mapreduce;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Properties;

/**
 * Created by mkw on 1/24/16.
 */
public class KafkaUtils {

    public static Properties getKafkaConnectionProperties(Configuration config){
        Properties props = new Properties();
        for(Map.Entry<String, String> value: config){
            props.setProperty(value.getKey(), value.getValue());
        }

        return props;
    }


    public static Configuration addKafkaConnectionProperties(Properties properties, Configuration config){
        for(String name: properties.stringPropertyNames()){
            config.set(name, properties.getProperty(name));
        }
        return config;
    }
}
