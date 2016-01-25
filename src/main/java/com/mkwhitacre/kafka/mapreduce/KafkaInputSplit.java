package com.mkwhitacre.kafka.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

//TODO right now for simplicitly assuming one split per TopicPartition but could group to cut down on mappers.
public class KafkaInputSplit extends InputSplit {

    private String topic;
    private int partition;
    private long startingOffset;
    private long endingOffset;
    private transient TopicPartition topicPartition;

    public KafkaInputSplit(String topic, int partition, long startingOffset, long endingOffset){
        this.topic = topic;
        this.partition = partition;
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
    }


    @Override
    public long getLength() throws IOException, InterruptedException {
        return startingOffset > 0 ? endingOffset - startingOffset : endingOffset;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        //Leave empty since data locality not really an issues.
        return new String[0];
    }

    public TopicPartition getTopicPartition(){
        if(topicPartition == null){
            topicPartition = new TopicPartition(topic, partition);
        }
        return topicPartition;
    }

    public long getStartingOffset(){
        return startingOffset;
    }

    public long getEndingOffset(){
        return endingOffset;
    }
}
