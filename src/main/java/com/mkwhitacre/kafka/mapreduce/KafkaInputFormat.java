package com.mkwhitacre.kafka.mapreduce;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class  KafkaInputFormat<K, V> extends InputFormat<K, V> {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return null;
    }
}
