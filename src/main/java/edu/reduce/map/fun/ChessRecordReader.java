package edu.reduce.map.fun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ChessRecordReader extends RecordReader<LongWritable, GameWritable> {
    LineRecordReader lineRecordReader = new LineRecordReader();

    private static final Logger log = Logger.getLogger(ChessRecordReader.class);

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineRecordReader.initialize(split, context);
    }

    public boolean nextKeyValue() throws IOException {
        return lineRecordReader.nextKeyValue();
    }

    public LongWritable getCurrentKey() {
        return lineRecordReader.getCurrentKey();
    }

    public GameWritable getCurrentValue() {
        return getGameWritableOrDefault(lineRecordReader.getCurrentValue().toString());
    }

    protected static GameWritable getGameWritableOrDefault(String line) {
        try {
            return GameWritables.fromColumns(line.split(","));
        } catch (Exception e) {
            log.error("Could not parse line :" + line, e);
            return new GameWritable();
        }
    }

    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    public void close() throws IOException {
        lineRecordReader.close();
    }
}
