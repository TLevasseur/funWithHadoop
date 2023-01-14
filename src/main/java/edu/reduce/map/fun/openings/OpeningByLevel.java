package edu.reduce.map.fun.openings;

import edu.reduce.map.fun.GameWritable;
import edu.reduce.map.fun.GameWritables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OpeningByLevel extends Mapper<LongWritable, GameWritable, LevelWritable, Text> {
    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        if (!GameWritables.EMPTY.equals(value)) {
            context.write(new LevelWritable(Math.max(value.getBlackRating(), value.getWhiteRating())), value.getOpening());
        }
    }
}
