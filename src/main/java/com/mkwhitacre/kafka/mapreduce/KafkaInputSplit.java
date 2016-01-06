package com.mkwhitacre.kafka.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;

/**
 * Created by mkw on 1/5/16.
 */
public class KafkaInputSplit extends InputSplit {

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }
}
