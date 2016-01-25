package com.mkwhitacre.kafka.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static com.mkwhitacre.kafka.mapreduce.KafkaUtils.getKafkaConnectionProperties;

public class KafkaRecordReader<K, V> extends RecordReader<K, V> {

    private Consumer<K,V> consumer;
    private ConsumerRecord<K,V> record;
    private long endingOffset;
    private Iterator<ConsumerRecord<K,V>> recordIterator;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        consumer = new KafkaConsumer<>(getKafkaConnectionProperties(taskAttemptContext.getConfiguration()));
        KafkaInputSplit split = (KafkaInputSplit) inputSplit;
        TopicPartition topicPartition = split.getTopicPartition();
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, split.getStartingOffset());
        endingOffset = split.getEndingOffset();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        recordIterator  = getRecords();
        record = recordIterator.hasNext() ? recordIterator.next() : null;
        return record != null && record.offset() > endingOffset;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return record.key();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return record.value();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        //not most accurate but gives reasonable estimate
        return record.offset()/endingOffset;
    }

    private Iterator<ConsumerRecord<K,V>> getRecords() {
        ConsumerRecords<K,V> records = null;
        if(recordIterator == null || recordIterator.hasNext()) {
            records = consumer.poll(1000);
        }
        return records != null ? records.iterator(): ConsumerRecords.<K,V>empty().iterator();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
