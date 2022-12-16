package edu.reduce.map.fun;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Count extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = countHelper(values);
        context.write(key, new IntWritable(count));
    }

    protected static int countHelper(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable i : values) {
            count += i.get();
        }
        return count;
    }
}
