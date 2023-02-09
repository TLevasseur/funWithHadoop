package edu.reduce.map.fun.transform;

import edu.reduce.map.fun.GameWritable;
import edu.reduce.map.fun.GameWritables;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapToRecord extends Mapper<LongWritable, GameWritable, Void, GenericRecord> {

    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        if (!GameWritables.EMPTY.equals(value)) {
            context.write(null, GameWritables.record(value));
        }
    }
}
