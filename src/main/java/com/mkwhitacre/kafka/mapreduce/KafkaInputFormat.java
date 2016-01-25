package com.mkwhitacre.kafka.mapreduce;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class KafkaInputFormat<K, V> extends InputFormat<K, V> {


    private final String[] topics;

    //TODO need to figure out how to take in Kafka Topic/Partition beginning offset info
    //Also possibly take in Kafka Topic/Partition ending offset (or make it visible)

    public KafkaInputFormat(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, String...topics) {
        this.topics = topics;
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {

        Properties connectionProps = KafkaUtils.getKafkaConnectionProperties(jobContext.getConfiguration());
        List<InputSplit> splits = new LinkedList<>();
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(connectionProps)) {
            for (String topic : topics) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                for(PartitionInfo info: partitionInfos){
                    //TODO figure out how to find the current lowest + max offsets
                    //trying to do this with just kafka-clients but might need kafka_2.x
                    splits.add(new KafkaInputSplit(topic, info.partition(), -1, -1));
                }
            }
        }

        return splits;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new KafkaRecordReader<>();
    }
}
