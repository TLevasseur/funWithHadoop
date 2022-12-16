package edu.reduce.map.fun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ChessFileInputFormat extends FileInputFormat<LongWritable, GameWritable> {
    public RecordReader<LongWritable, GameWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new ChessRecordReader();
    }
}
